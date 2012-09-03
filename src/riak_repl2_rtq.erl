%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtq).

%% @doc Queue module for realtime replication.
%%
%% The queue strives to reliably pass on realtime replication, with the
%% aim of reducing the need to fullsync.  Every item in the queue is
%% given a sequence number when pushed.  Consumers register with the
%% queue, then pull passing in a function to receive items (executed
%% on the queue process - it can cast/! as it desires).
%%
%% Once the consumer has delievered the item, it must ack the queue
%% with the sequence number.  If multiple deliveries have taken
%% place an ack of the highest seq number acknowledge all previous.
%%
%% The queue is currently stored in a private ETS table.  Once
%% all consumers are done with an item it is removed from the table.

-behaviour(gen_server).
%% API
-export([start_link/0,
         register/1,
         unregister/1,
         set_max_bytes/1,
         push/2,
         pull/2,
         ack/2,
         status/0,
         dumpq/0]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {qtab = ets:new(?MODULE, [private, ordered_set]), % ETS table
                qseq = 0,  % Last sequence number handed out
                max_bytes = undefined, % maximum ETS table memory usage in bytes
                cs = []}).  % Consumers
-record(c, {name,      % consumer name
            aseq = 0,  % last sequence acked
            cseq = 0,  % last sequence sent
            drops = 0, % number of dropped queue entries (not items)
            errs = 0,  % delivery errors
            deliver}).  % deliver function if pending, otherwise undefined

%% API
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Name) ->
    gen_server:call(?SERVER, {register, Name}).

unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}).

%% Set the maximum number of bytes to use - could take a while to return
%% on a big queue
set_max_bytes(MaxBytes) ->
    gen_server:call(?SERVER, {set_max_bytes, MaxBytes}, infinity).

%% Push an item onto the queue
push(NumItems, Bin) ->
    gen_server:cast(?SERVER, {push, NumItems, Bin}).

%% DeliverFun - (Seq, Item)
pull(Name, DeliverFun) ->
    gen_server:cast(?SERVER, {pull, Name, DeliverFun}).

ack(Name, Seq) ->
    gen_server:cast(?SERVER, {ack, Name, Seq}).

status() ->
    gen_server:call(?SERVER, status).

dumpq() ->
    gen_server:call(?SERVER, dumpq).

%% Internals
init([]) ->
    MaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes),
    {ok, #state{max_bytes = MaxBytes}}. % lots of initialization done by defaults

handle_call(status, _From, State = #state{qtab = QTab, max_bytes = MaxBytes,
                                          qseq = QSeq, cs = Cs}) ->
    Status =
        [{bytes, qbytes(QTab)},
         {max_bytes, MaxBytes}] ++
        [{Name, [{pending, QSeq - CSeq},  % items to be send
                 {unacked, CSeq - ASeq},  % sent items requiring ack
                 {drops, Drops}]} 
         || #c{name = Name, aseq = ASeq, cseq = CSeq, drops = Drops} <- Cs],
    {reply, Status, State};
handle_call({register, Name}, _From, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    MinSeq = minseq(QTab, QSeq),
    case lists:keytake(Name, #c.name, Cs) of
        {value, C = #c{aseq = PrevASeq, drops = PrevDrops}, Cs2} ->
            %% Work out if anything should be considered dropped if
            %% unacknowledged.
            Drops = max(0, MinSeq - PrevASeq),

            %% Re-registering, send from the last acked sequence
            CSeq = C#c.aseq,
            UpdCs = [C#c{cseq = CSeq, drops = PrevDrops + Drops} | Cs2];
        false ->
            %% New registration, start from the beginning
            CSeq = MinSeq,
            UpdCs = [#c{name = Name, aseq = CSeq, cseq = CSeq} | Cs]
    end,
    {reply, {ok, CSeq}, State#state{cs = UpdCs}};
handle_call({unregister, Name}, _From, State = #state{qtab = QTab, cs = Cs}) ->
    case lists:keytake(Name, #c.name, Cs) of
        {value, C, Cs2} ->
            %% Remove C from Cs, let any pending process know 
            %% and clean up the queue
            case C#c.deliver of
                undefined ->
                    ok;
                Deliver ->
                    Deliver({error, unregistered})
            end,
            MinSeq = case Cs2 of 
                         [] ->
                             State#state.qseq; % no consumers, remove it all
                         _ ->
                             lists:min([Seq || #c{aseq = Seq} <- Cs2])
                     end,
            cleanup(QTab, MinSeq),
            {reply, ok, State#state{cs = Cs2}};
        false ->
            {reply, {error, not_registered}, State}
    end;
handle_call({set_max_bytes, MaxBytes}, _From, State) ->
    {reply, ok, trim_q(State#state{max_bytes = MaxBytes})};
handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State}.


handle_cast({push, NumItems, Bin}, State = #state{qtab = QTab, qseq = QSeq,
                                                  cs = Cs}) ->
    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin},
    %% Send to any pending consumers
    Cs2 = [maybe_deliver_item(C, QEntry) || C <- Cs],
    ets:insert(QTab, QEntry),
    {noreply, trim_q(State#state{qseq = QSeq2, cs = Cs2})};
handle_cast({pull, Name, DeliverFun}, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    UpdCs = case lists:keytake(Name, #c.name, Cs) of
                {value, C, Cs2} ->
                    [maybe_pull(QTab, QSeq, C, DeliverFun) | Cs2];
                false ->
                    DeliverFun({error, not_registered})
            end,
    {noreply, State#state{cs = UpdCs}};
handle_cast({ack, Name, Seq}, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    %% Scan through the clients, updating Name for Seq and also finding the minimum
    %% sequence
    {UpdCs, MinSeq} = lists:foldl(
                        fun(C, {Cs2, MinSeq2}) ->
                                case C#c.name of
                                    Name ->
                                        {[C#c{aseq = Seq} | Cs2], min(Seq, MinSeq2)};
                                    _ ->
                                        {[C | Cs2], min(C#c.aseq, MinSeq2)}
                                end
                        end, {[], QSeq}, Cs),
    %% Remove any entries from the ETS table before MinSeq
    cleanup(QTab, MinSeq),
    {noreply, State#state{cs = UpdCs}}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, #state{cs = Cs}) ->
    [case DeliverFun of
         undefined ->
             ok;
         _ ->
             DeliverFun({error, {terminate, Reason}})
     end || #c{deliver = DeliverFun} <- Cs],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



maybe_pull(QTab, QSeq, C = #c{cseq = CSeq}, DeliverFun) ->
    CSeq2 = CSeq + 1,
    case CSeq2 =< QSeq of
        true -> % something reday
            [QEntry] = ets:lookup(QTab, CSeq2),
            deliver_item(C, DeliverFun, QEntry);
        false ->
            %% consumer is up to date with head, keep deliver function
            %% until something pushed
            C#c{deliver = DeliverFun}
    end.

maybe_deliver_item(C = #c{deliver = DeliverFun}, QEntry) ->
    case DeliverFun of
        undefined ->
            C;
        _ ->
            deliver_item(C, DeliverFun, QEntry)
    end.

deliver_item(C, DeliverFun, {Seq,_NumItem,_Bin} = QEntry) ->
    try
        Seq = C#c.cseq + 1, % bit of paranoia, remove after EQC
        ok = DeliverFun(QEntry),
        C#c{cseq = Seq, deliver = undefined}
    catch
        _:_ ->
            %% do not advance head so it will be delivered again
            C#c{errs = C#c.errs + 1, deliver = undefined}
    end.

%% Find the first sequence number
minseq(QTab, QSeq) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            QSeq;
        MinSeq ->
            MinSeq
    end.


%% Cleanup until the start of the table
cleanup(_QTab, '$end_of_table') ->
    ok;
cleanup(QTab, Seq) ->
    ets:delete(QTab, Seq),
    cleanup(QTab, ets:prev(QTab, Seq)).

%% Trim the queue if necessary
trim_q(State = #state{max_bytes = undefined}) ->
    State;
trim_q(State = #state{qtab = QTab, max_bytes = MaxBytes}) ->
    case qbytes(QTab) > MaxBytes of
        true ->
            Cs2 = trim_q_entries(QTab, MaxBytes, State#state.cs, ets:first(QTab)),

            %% Adjust the last sequence handed out number
            MinSeq = minseq(QTab, State#state.qseq),
            Cs3 = [case CSeq < MinSeq of
                       true ->
                           C#c{cseq = MinSeq};
                       _ ->
                           C
                   end || C = #c{cseq = CSeq} <- Cs2],
            State#state{cs = Cs3};
        false -> % Q size is less than MaxBytes words
            State
    end.

trim_q_entries(_QTab, _MaxBytes, Cs, '$end_of_table') ->
    Cs;
trim_q_entries(QTab, MaxBytes, Cs, TrimSeq) ->
    ets:delete(QTab, TrimSeq),
    Cs2 = [case CSeq < TrimSeq of
               true ->
                   %% If the last sent qentry is before the trimseq
                   %% it will never be sent, so count it as a drop.
                   C#c{drops = C#c.drops + 1};
               _ ->
                   C
           end || C = #c{cseq = CSeq} <- Cs],
    %% Rinse and repeat until meet the target or the queue is empty
    case qbytes(QTab) > MaxBytes of
        true ->
            trim_q_entries(QTab, MaxBytes, Cs2, TrimSeq + 1);
        _ ->
            Cs2
    end.
    
qbytes(QTab) ->
    WordSize = erlang:system_info(wordsize),
    Words = ets:info(QTab, memory),
    Words * WordSize.
