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
         start_test/0,
         register/1,
         unregister/1,
         set_max_bytes/1,
         push/2,
         pull/2,
         pull_sync/2,
         ack/2,
         ack_sync/2,
         status/0,
         dumpq/0,
         is_empty/1,
         all_queues_empty/0,
         shutdown/0,
         stop/0,
         is_running/0]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {qtab = ets:new(?MODULE, [private, ordered_set]), % ETS table
                qseq = 0,  % Last sequence number handed out
                max_bytes = undefined, % maximum ETS table memory usage in bytes
                cs = [],
                shutting_down=false,
                qsize_bytes = 0,
                word_size=erlang:system_info(wordsize)
               }).

% Consumers
-record(c, {name,      % consumer name
            aseq = 0,  % last sequence acked
            cseq = 0,  % last sequence sent
            drops = 0, % number of dropped queue entries (not items)
            errs = 0,  % delivery errors
            deliver  % deliver function if pending, otherwise undefined
           }).
%% API
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_test() ->
    gen_server:start(?MODULE, [], []).

register(Name) ->
    gen_server:call(?SERVER, {register, Name}, infinity).

unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}, infinity).


is_empty(Name) ->
    gen_server:call(?SERVER, {is_empty, Name}, infinity).

all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).

%% Set the maximum number of bytes to use - could take a while to return
%% on a big queue
set_max_bytes(MaxBytes) ->
    gen_server:call(?SERVER, {set_max_bytes, MaxBytes}, infinity).

%% Push an item onto the queue
push(NumItems, Bin) ->
    gen_server:cast(?SERVER, {push, NumItems, Bin}).

pull(Name, DeliverFun) ->
    gen_server:cast(?SERVER, {pull, Name, DeliverFun}).

pull_sync(Name, DeliverFun) ->
    gen_server:call(?SERVER, {pull_with_ack, Name, DeliverFun}, infinity).

ack(Name, Seq) ->
    gen_server:cast(?SERVER, {ack, Name, Seq}).

ack_sync(Name, Seq) ->
    gen_server:call(?SERVER, {ack_sync, Name, Seq}, infinity).

status() ->
    gen_server:call(?SERVER, status, infinity).

dumpq() ->
    gen_server:call(?SERVER, dumpq, infinity).

shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

is_running() ->
    gen_server:call(?SERVER, is_running, infinity).


%% Internals
init([]) ->
    %% Default maximum realtime queue size to 100Mb
    MaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes, 100*1024*1024),
    {ok, #state{max_bytes = MaxBytes}}. % lots of initialization done by defaults

handle_call(status, _From, State = #state{qtab = QTab, max_bytes = MaxBytes,
                                          qseq = QSeq, cs = Cs}) ->
    Consumers =
        [{Name, [{pending, QSeq - CSeq},  % items to be send
                 {unacked, CSeq - ASeq},  % sent items requiring ack
                 {drops, Drops},          % number of dropped entries due to max bytes
                 {errs, Errs}]}           % number of non-ok returns from deliver fun
         || #c{name = Name, aseq = ASeq, cseq = CSeq, 
               drops = Drops, errs = Errs} <- Cs],
    Status =
        [{bytes, qbytes(QTab, State)},
         {max_bytes, MaxBytes},
         {consumers, Consumers}],
    {reply, Status, State};

handle_call(shutting_down, _From, State = #state{shutting_down=false}) ->
    %% this will allow the realtime repl hook to determine if it should send
    %% to another host
    riak_repl2_rtq_proxy:start(),
    {reply, ok, State#state{shutting_down = true}};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(is_running, _From,
            State = #state{shutting_down = ShuttingDown}) ->
    {reply, not ShuttingDown, State};

handle_call({is_empty, Name}, _From, State = #state{qseq = QSeq, cs = Cs}) ->
    Result = is_queue_empty(Name, QSeq, Cs),
    {reply, Result, State};

handle_call(all_queues_empty, _From, State = #state{qseq = QSeq, cs = Cs}) ->
    Result = lists:all(fun (#c{name = Name}) -> is_queue_empty(Name, QSeq, Cs) end, Cs),
    {reply, Result, State};


handle_call({register, Name}, _From, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    MinSeq = minseq(QTab, QSeq),
    case lists:keytake(Name, #c.name, Cs) of
        {value, C = #c{aseq = PrevASeq, drops = PrevDrops}, Cs2} ->
            %% Work out if anything should be considered dropped if
            %% unacknowledged.
            Drops = max(0, MinSeq - PrevASeq - 1),

            %% Re-registering, send from the last acked sequence
            CSeq = case C#c.aseq < MinSeq of
                true -> MinSeq;
                false -> C#c.aseq
            end,
            UpdCs = [C#c{cseq = CSeq, drops = PrevDrops + Drops, 
                         deliver = undefined} | Cs2];
        false ->
            %% New registration, start from the beginning
            CSeq = MinSeq,
            UpdCs = [#c{name = Name, aseq = CSeq, cseq = CSeq} | Cs]
    end,
    {reply, {ok, CSeq}, State#state{cs = UpdCs}};
handle_call({unregister, Name}, _From, State) ->
    case unregister_q(Name, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {{error, not_registered}, State} ->  
            {reply, {error, not_registered}, State}
  end;

handle_call({set_max_bytes, MaxBytes}, _From, State) ->
    {reply, ok, trim_q(State#state{max_bytes = MaxBytes})};
handle_call(dumpq, _From, State = #state{qtab = QTab}) ->
    {reply, ets:tab2list(QTab), State};

handle_call({pull_with_ack, Name, DeliverFun}, _From, State) ->
    {reply, ok, pull(Name, DeliverFun, State)};

handle_call({push, NumItems, Bin}, _From, State) ->
    {reply, ok, push(NumItems, Bin, State)};

handle_call({ack_sync, Name, Seq}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, State)}.

handle_cast({push, NumItems, Bin}, State) ->
    {noreply, push(NumItems, Bin, State)};

handle_cast({pull, Name, DeliverFun}, State) ->
     {noreply, pull(Name, DeliverFun, State)};

handle_cast({ack, Name, Seq}, State) ->
       {noreply, ack_seq(Name, Seq, State)}.

ack_seq(Name, Seq, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
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
    NewState = cleanup(QTab, MinSeq, State),
    NewState#state{cs = UpdCs}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(Reason, #state{cs = Cs}) ->
    erlang:unregister(?SERVER),
    flush_pending_pushes(),
    [case DeliverFun of
         undefined ->
             ok;
         _ ->
            catch(DeliverFun({error, {terminate, Reason}}))
     end || #c{deliver = DeliverFun} <- Cs],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

flush_pending_pushes() ->
    receive
        {'$gen_cast', {push, NumItems, Bin}} ->
            riak_repl2_rtq_proxy:push(NumItems, Bin),
            flush_pending_pushes()
    after
        1000 ->
            ok
    end.


unregister_q(Name, State = #state{qtab = QTab, cs = Cs}) ->
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
            NewState0 = cleanup(QTab, MinSeq, State),
            NewState = NewState0#state{cs = Cs2},
            {ok, NewState};
        false ->
            {{error, not_registered}, State}
    end.

push(NumItems, Bin, State = #state{qtab = QTab,
                                   qseq = QSeq,
                                   cs = Cs,
                                   shutting_down = false}) ->
    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin},
    %% Send to any pending consumers
    Cs2 = [maybe_deliver_item(C, QEntry) || C <- Cs],
    Size = ets_obj_size(Bin, State),
    NewState = update_q_size(State, Size),
    ets:insert(QTab, QEntry),
    trim_q(NewState#state{qseq = QSeq2, cs = Cs2});

push(NumItems, Bin, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin),
    State.

pull(Name, DeliverFun, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
     UpdCs = case lists:keytake(Name, #c.name, Cs) of
                {value, C, Cs2} ->
                    [maybe_pull(QTab, QSeq, C, DeliverFun) | Cs2];
                false ->
                    DeliverFun({error, not_registered})
            end,
    State#state{cs = UpdCs}.

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

deliver_item(C, DeliverFun, {Seq,_NumItem, _Bin} = QEntry) ->
    try
        Seq = C#c.cseq + 1, % bit of paranoia, remove after EQC
        ok = DeliverFun(QEntry),
        C#c{cseq = Seq, deliver = undefined}
    catch
        _:_ ->
            riak_repl_stats:rt_source_errors(),
            %% do not advance head so it will be delivered again
            C#c{errs = C#c.errs + 1, deliver = undefined}
    end.

%% Cleanup until the start of the table
cleanup(_QTab, '$end_of_table', State) ->
    State;
cleanup(QTab, Seq, State) ->
    case ets:lookup(QTab, Seq) of
        [] -> cleanup(QTab, ets:prev(QTab, Seq), State);
        [{_, _, Bin}] ->
           ObjSize = ets_obj_size(Bin, State),
           NewState = update_q_size(State, -ObjSize),
           ets:delete(QTab, Seq),
           cleanup(QTab, ets:prev(QTab, Seq), NewState);
        _ ->
            lager:warning("Unexpected object in RTQ")
    end.

ets_obj_size(Obj, #state{word_size = WordSize}) when is_binary(Obj) ->
  BSize = erlang:byte_size(Obj),
  case BSize > 64 of
        true -> BSize - (6 * WordSize);
        false -> BSize
  end;
ets_obj_size(Obj, _) ->
  erlang:size(Obj).

update_q_size(State = #state{qsize_bytes = CurrentQSize}, Diff) ->
  State#state{qsize_bytes = CurrentQSize + Diff}.

%% Trim the queue if necessary
trim_q(State = #state{max_bytes = undefined}) ->
    State;
trim_q(State = #state{qtab = QTab, qseq = QSeq, max_bytes = MaxBytes}) ->
    case qbytes(QTab, State) > MaxBytes of
        true ->
            {Cs2, NewState} = trim_q_entries(QTab, MaxBytes, State#state.cs,
                                             State),

            %% Adjust the last sequence handed out number
            %% so that the next pull will retrieve the new minseq
            %% number.  If that increases a consumers cseq,
            %% reset the aseq too.  The drops have already been
            %% accounted for.
            NewCSeq = case ets:first(QTab) of
                          '$end_of_table' ->
                              QSeq; % if empty, make sure pull waits
                          MinSeq ->
                              MinSeq - 1
                      end,
            Cs3 = [case CSeq < NewCSeq of
                       true ->
                           C#c{cseq = NewCSeq, aseq = NewCSeq};
                       _ ->
                           C
                   end || C = #c{cseq = CSeq} <- Cs2],
            NewState#state{cs = Cs3};
        false -> % Q size is less than MaxBytes words
            State
    end.

trim_q_entries(QTab, MaxBytes, Cs, State) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            {Cs, State};
        TrimSeq ->
            [{_, _, Bin}] = ets:lookup(QTab, TrimSeq),
            ObjSize = ets_obj_size(Bin, State),
            NewState = update_q_size(State, -ObjSize),
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
            case qbytes(QTab, NewState) > MaxBytes of
                true ->
                    trim_q_entries(QTab, MaxBytes, Cs2, NewState);
                _ ->
                    {Cs2, NewState}
            end
    end.

-ifdef(TEST).
qbytes(_QTab, #state{qsize_bytes = QSizeBytes}) ->
    %% when EQC testing, don't account for ETS overhead
    QSizeBytes.
-else.
qbytes(QTab, #state{qsize_bytes = QSizeBytes, word_size=WordSize}) ->
    Words = ets:info(QTab, memory),
    (Words * WordSize) + QSizeBytes.
-endif.

is_queue_empty(Name, QSeq, Cs) ->
    case lists:keytake(Name, #c.name, Cs) of
        {value,  #c{cseq = CSeq}, _Cs2} ->
            CSeq2 = CSeq + 1,
            case CSeq2 =< QSeq of
                true -> false;
                false -> true
            end;

        false -> lager:error("Unknown queue")
    end.


%% Find the first sequence number
minseq(QTab, QSeq) ->
    case ets:first(QTab) of
        '$end_of_table' ->
            QSeq;
        MinSeq ->
            MinSeq - 1
    end.

