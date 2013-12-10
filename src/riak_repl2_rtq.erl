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
         start_link/1,
         start_test/0,
         register/1,
         unregister/1,
         set_max_bytes/1,
         push/3,
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
% private api
-export([report_drops/1]).

-define(overload_ets, rtq_overload_ets).
-define(SERVER, ?MODULE).
-define(DEFAULT_OVERLOAD, 2000).
-define(DEFAULT_RECOVER, 1000).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {qtab = ets:new(?MODULE, [private, ordered_set]), % ETS table
                qseq = 0,  % Last sequence number handed out
                max_bytes = undefined, % maximum ETS table memory usage in bytes

                % if the message q exceeds this, the rtq is overloaded
                overload = ?DEFAULT_OVERLOAD :: pos_integer(),

                % if the rtq is in overload mode, it does not recover until =<
                recover = ?DEFAULT_RECOVER :: pos_integer(),

                overloaded = false :: boolean(),
                overload_drops = 0 :: non_neg_integer(),

                cs = [],
                undeliverables = [],
                shutting_down=false,
                qsize_bytes = 0,
                word_size=erlang:system_info(wordsize)
               }).

% Consumers
-record(c, {name,      % consumer name
            aseq = 0,  % last sequence acked
            cseq = 0,  % last sequence sent
            skips = 0,
            drops = 0, % number of dropped queue entries (not items)
            errs = 0,  % delivery errors
            deliver,  % deliver function if pending, otherwise undefined
            delivered = false  % used by the skip count.
            % skip_count is used to help the sink side determine if an item has
            % been dropped since the last delivery. The sink side can't
            % determine if there's been drops accurately if the source says there
            % were skips before it's sent even one item.
           }).

%% API
%% @doc Start linked, registeres to module name.
-spec start_link() -> {ok, pid()}.
start_link() ->
    Overload = app_helper:get_env(riak_repl, rtq_overload_threshold, ?DEFAULT_OVERLOAD),
    Recover = app_helper:get_env(riak_repl, rtq_overload_recover, ?DEFAULT_RECOVER),
    Opts = [{overload_threshold, Overload}, {overload_recover, Recover}],
    start_link(Opts).

%% @doc Start linked, registers to module name, with given options. This makes
%% testing some options a bit easier as it removes a dependance on app_helper.
-type overload_threshold_option() :: {'overload_threshold', pos_integer()}.
-type overload_recover_option() :: {'overload_recover', pos_integer()}.
-type start_option() :: overload_threshold_option() | overload_recover_option().
-type start_options() :: [start_option()].
-spec start_link(Options :: start_options()) -> {'ok', pid()}.
start_link(Options) ->
    case ets:info(?overload_ets) of
        undefined ->
            ets:new(?overload_ets, [named_table, public, {read_concurrency, true}]),
            ets:insert(?overload_ets, {overloaded, false});
        _ ->
            ok
    end,
    gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%% @doc Test helper, starts unregistered and unlinked.
-spec start_test() -> {ok, pid()}.
start_test() ->
    gen_server:start(?MODULE, [], []).

%% @doc Register a consumer with the given name. The Name of the consumer is
%% the name of the remote cluster by convention. Returns the oldest unack'ed
%% sequence number.
-spec register(Name :: string()) -> {'ok', number()}.
register(Name) ->
    gen_server:call(?SERVER, {register, Name}, infinity).

%% @doc Removes a consumer.
-spec unregister(Name :: string()) -> 'ok' | {'error', 'not_registered'}.
unregister(Name) ->
    gen_server:call(?SERVER, {unregister, Name}, infinity).

%% @doc True if the given consumer has no items to consume.
-spec is_empty(Name :: string()) -> boolean().
is_empty(Name) ->
    gen_server:call(?SERVER, {is_empty, Name}, infinity).

%% @doc True if no consumer has items to consume.
-spec all_queues_empty() -> boolean().
all_queues_empty() ->
    gen_server:call(?SERVER, all_queues_empty, infinity).

%% @doc Set the maximum number of bytes to use - could take a while to return
%% on a big queue. The maximum is for the backend data structure used itself,
%% not just the raw size of the objects. This was chosen to keep a situation
%% where overhead of stored objects would cause more memory to be used than
%% expected just looking at MaxBytes.
-spec set_max_bytes(MaxBytes :: pos_integer() | 'undefined') -> 'ok'.
set_max_bytes(MaxBytes) ->
    % TODO if it always returns 'ok' it should likely be a cast, eg:
    % why are we blocking the caller while it trims the queue?
    gen_server:call(?SERVER, {set_max_bytes, MaxBytes}, infinity).

%% @doc Push an item onto the queue. Bin should be the list of objects to push
%% run through term_to_binary, while NumItems is the length of that list
%% before being turned to a binary. Meta is an orddict() of data about the
%% queued item. The key `routed_clusters' is a list of the clusters the item
%% has received and ack for. The key `local_forwards' is added automatically.
%% It is a list of the remotes this cluster forwards to. It is intended to be
%% used by consumers to alter the `routed_clusters' key before being sent to
%% the sink.
-spec push(NumItems :: pos_integer(), Bin :: binary(), Meta :: orddict:orddict()) -> 'ok'.
push(NumItems, Bin, Meta) ->
    case should_drop() of
        true ->
            lager:debug("rtq overloaded"),
            riak_repl2_rtq_overload_counter:drop();
        false ->
            gen_server:cast(?SERVER, {push, NumItems, Bin, Meta})
    end.

should_drop() ->
    [{overloaded, Val}] = ets:lookup(?overload_ets, overloaded),
    Val.

%% @doc Like push/3, only Meta is orddict:new/0.
-spec push(NumItems :: pos_integer(), Bin :: binary()) -> 'ok'.
push(NumItems, Bin) ->
    push(NumItems, Bin, []).

%% @doc Using the given DeliverFun, send an item to the consumer Name
%% asynchonously.
-type queue_entry() :: {pos_integer(), pos_integer(), binary(), orddict:orddict()}.
-type not_reg_error() :: {'error', 'not_registered'}.
-type deliver_fun() :: fun((queue_entry() | not_reg_error()) -> 'ok').
-spec pull(Name :: string(), DeliverFun :: deliver_fun()) -> 'ok'.
pull(Name, DeliverFun) ->
    lager:info("in async pull, Name=~p, DeliverFun=~p", [Name, DeliverFun]),
    gen_server:cast(?SERVER, {pull, Name, DeliverFun}).

%% @doc Block the caller while the pull is done.
-spec pull_sync(Name :: string(), DeliverFun :: deliver_fun()) -> 'ok'.
pull_sync(Name, DeliverFun) ->
    lager:info("in sync pull, Name=~p, DeliverFun=~p", [Name, DeliverFun]),
    gen_server:call(?SERVER, {pull_with_ack, Name, DeliverFun}, infinity).

%% @doc Asynchronously acknowldge delivery of all objects with a sequence
%% equal or lower to Seq for the consumer.
-spec ack(Name :: string(), Seq :: pos_integer()) -> 'ok'.
ack(Name, Seq) ->
    gen_server:cast(?SERVER, {ack, Name, Seq}).

%% @doc Same as ack/2, but blocks the caller.
-spec ack_sync(Name :: string(), Seq :: pos_integer()) ->'ok'.
ack_sync(Name, Seq) ->
    gen_server:call(?SERVER, {ack_sync, Name, Seq}, infinity).

%% @doc The status of the queue.
%% <dl>
%% <dt>bytes</dt><dd>Size of the data store backend</dd>
%% <dt>max_bytes</dt><dd>Maximum size of the data store backend</dd>
%% <dt>consumers</dt><dd>Key - Value pair of the consumer stats, key is the
%% consumer name.</dd>
%% </dl>
%%
%% The consumers have the following data:
%% <dl>
%% <dt>pending</dt><dd>Number of queue items left to send.</dd>
%% <dt>unacked</dt><dd>Number of queue items that are sent, but not yet acked</dd>
%% <dt>drops</dt><dd>Dropped entries due to max_bytes</dd>
%% <dt>errs</dt><dd>Number of non-ok returns from deliver fun</dd>
%% </dl>
-spec status() -> [any()].
status() ->
    gen_server:call(?SERVER, status, infinity).

%% @doc return the data store as a list.
-spec dumpq() -> [any()].
dumpq() ->
    gen_server:call(?SERVER, dumpq, infinity).

%% @doc Signal that this node is doing down, and so a proxy process needs to
%% start to avoid dropping, or aborting unacked results.
-spec shutdown() -> 'ok'.
shutdown() ->
    gen_server:call(?SERVER, shutting_down, infinity).

stop() ->
    gen_server:call(?SERVER, stop, infinity).

%% @doc Will explode if the server is not started, but will tell you if it's
%% in shutdown.
-spec is_running() -> boolean().
is_running() ->
    gen_server:call(?SERVER, is_running, infinity).

%% @private
report_drops(N) ->
    gen_server:cast(?SERVER, {report_drops, N}).

%% Internals
%% @private
init(Options) ->
    %% Default maximum realtime queue size to 100Mb
    MaxBytes = app_helper:get_env(riak_repl, rtq_max_bytes, 100*1024*1024),
    Overloaded = proplists:get_value(overload_threshold, Options, ?DEFAULT_OVERLOAD),
    Recover = proplists:get_value(overload_recover, Options, ?DEFAULT_RECOVER),
    {ok, #state{max_bytes = MaxBytes, overload = Overloaded, recover = Recover}}. % lots of initialization done by defaults

%% @private
handle_call(status, _From, State = #state{qtab = QTab, max_bytes = MaxBytes,
                                          qseq = QSeq, cs = Cs}) ->
    Consumers =
        [{Name, [{pending, QSeq - CSeq},  % items to be send
                 {unacked, CSeq - ASeq - Skips},  % sent items requiring ack
                 {drops, Drops},          % number of dropped entries due to max bytes
                 {errs, Errs}]}           % number of non-ok returns from deliver fun
         || #c{name = Name, aseq = ASeq, cseq = CSeq,
               drops = Drops, errs = Errs, skips = Skips} <- Cs],
    Status =
        [{bytes, qbytes(QTab, State)},
         {max_bytes, MaxBytes},
         {consumers, Consumers},
         {overload_drops, State#state.overload_drops}],
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

% either old code or old node has sent us a old push, upvert it.
handle_call({push, NumItems, Bin}, From, State) ->
    handle_call({push, NumItems, Bin, []}, From, State);

% TODO what is this code for? there's no external interface for it...
handle_call({push, NumItems, Bin, Meta}, _From, State) ->
    State2 = maybe_flip_overload(State),
    {reply, ok, push(NumItems, Bin, Meta, State2)};

handle_call({ack_sync, Name, Seq}, _From, State) ->
    {reply, ok, ack_seq(Name, Seq, State)}.

% ye previous cast. rtq_proxy may send us an old pattern.
handle_cast({push, NumItems, Bin}, State) ->
    handle_cast({push, NumItems, Bin, []}, State);

handle_cast({push, NumItems, Bin, Meta}, State) ->
    State2 = maybe_flip_overload(State),
    {noreply, push(NumItems, Bin, Meta, State2)};

%% @private
handle_cast({report_drops, N}, State) ->
    QSeq = State#state.qseq + N,
    Drops = State#state.overload_drops + N,
    State2 = State#state{qseq = QSeq, overload_drops = Drops},
    State3 = maybe_flip_overload(State2),
    {noreply, State3};

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
    {ShrinkBy, Undeliverables} = clear_non_deliverables(QTab, UpdCs, State#state.word_size),
    Undeliverables2 = union_undeliverables(NewState#state.undeliverables, Undeliverables, MinSeq),
    NewState#state{cs = UpdCs, undeliverables = Undeliverables2, qsize_bytes = NewState#state.qsize_bytes - ShrinkBy}.

%% @private
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
terminate(Reason, #state{cs = Cs}) ->
    %% when started from tests, we may not be registered
    catch(erlang:unregister(?SERVER)),
    flush_pending_pushes(),
    [case DeliverFun of
         undefined ->
             ok;
         _ ->
            catch(DeliverFun({error, {terminate, Reason}}))
     end || #c{deliver = DeliverFun} <- Cs],
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_flip_overload(State) ->
    #state{overloaded = Overloaded, overload = Overload, recover = Recover} = State,
    {message_queue_len, MsgQLen} = erlang:process_info(self(), message_queue_len),
    if
        Overloaded andalso MsgQLen =< Recover ->
            lager:info("Recovered from overloaded condition"),
            ets:insert(?overload_ets, {overloaded, false}),
            State#state{overloaded = false};
        (not Overloaded) andalso MsgQLen > Overload ->
            lager:warning("Realtime queue mailbox size of ~p is greater than ~p indicating overload; objects will be dropped until size is less than or equal to ~p", [MsgQLen, Overload, Recover]),
            % flip the rt_dirty flag on
            riak_repl_stats:rt_source_errors(),
            ets:insert(?overload_ets, {overloaded, true}),
            State#state{overloaded = true};
        true ->
            State
    end.

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
            {ShrinkBy, Undeliverables} = clear_non_deliverables(QTab, Cs2, NewState#state.word_size),
            Undeliverables2 = union_undeliverables(State#state.undeliverables, Undeliverables, 0),
            {ok, NewState#state{undeliverables = Undeliverables2, qsize_bytes = NewState#state.qsize_bytes - ShrinkBy}};
        false ->
            {{error, not_registered}, State}
    end.

push(NumItems, Bin, Meta, State = #state{qtab = QTab, qseq = QSeq,
                                                  cs = Cs, shutting_down = false}) ->
    QSeq2 = QSeq + 1,
    QEntry = {QSeq2, NumItems, Bin, Meta},
    %% Send to any pending consumers
    CsNames = [Consumer#c.name || Consumer <- Cs],
    QEntry2 = set_local_forwards_meta(CsNames, QEntry),
    DeliverAndCs2 = [maybe_deliver_item(C, QEntry2) || C <- Cs],
    {DeliverResults, Cs2} = lists:unzip(DeliverAndCs2),
    AllSkipped = lists:all(fun
        (skipped) -> true;
        (_) -> false
    end, DeliverResults),
    State2 = if
        AllSkipped andalso length(Cs) > 0 ->
            State;
        true ->
            ets:insert(QTab, QEntry2),
            Size = ets_obj_size(Bin, State),
            update_q_size(State, Size)
    end,
    trim_q(State2#state{qseq = QSeq2, cs = Cs2});
push(NumItems, Bin, Meta, State = #state{shutting_down = true}) ->
    riak_repl2_rtq_proxy:push(NumItems, Bin, Meta),
    State.

pull(Name, DeliverFun, State = #state{qtab = QTab, qseq = QSeq, cs = Cs}) ->
    CsNames = [Consumer#c.name || Consumer <- Cs],
     UpdCs = case lists:keytake(Name, #c.name, Cs) of
                {value, C, Cs2} ->
                    [maybe_pull(QTab, QSeq, C, CsNames, DeliverFun, State#state.undeliverables) | Cs2];
                false ->
                    lager:info("not_registered"),
                    DeliverFun({error, not_registered})
            end,
    State#state{cs = UpdCs}.

maybe_pull(QTab, QSeq, C = #c{cseq = CSeq}, CsNames, DeliverFun, Undeliverables) ->
    CSeq2 = CSeq + 1,
    case CSeq2 =< QSeq of
        true -> % something reday
            case ets:lookup(QTab, CSeq2) of
                [] -> % entry removed, due to previously being unroutable
                    C2 = C#c{skips = C#c.skips + 1, cseq = CSeq2},
                    maybe_pull(QTab, QSeq, C2, CsNames, DeliverFun, Undeliverables);
                [QEntry] ->
                    QEntry2 = set_local_forwards_meta(CsNames, QEntry),
                    % if the item can't be delivered due to cascading rt,
                    % just keep trying.
                    case maybe_deliver_item(C#c{deliver = DeliverFun}, QEntry2) of
                        {skipped, C2} ->
                            maybe_pull(QTab, QSeq, C2, CsNames, DeliverFun, Undeliverables);
                        {_WorkedOrNoFun, C2} ->
                            C2
                    end
            end;
        false ->
            %% consumer is up to date with head, keep deliver function
            %% until something pushed
            C#c{deliver = DeliverFun}
    end.

maybe_deliver_item(C = #c{deliver = undefined}, QEntry) ->
    {_Seq, _NumItem, _Bin, Meta} = QEntry,
    Name = C#c.name,
    Routed = case orddict:find(routed_clusters, Meta) of
        error -> [];
        {ok, V} -> V
    end,
    Cause = case lists:member(Name, Routed) of
        true ->
            skipped;
        false ->
            no_fun
    end,
    {Cause, C};
maybe_deliver_item(C, QEntry) ->
    {Seq, _NumItem, _Bin, Meta} = QEntry,
    lager:info("in maybe_deliver_item, QEntry=~p", [QEntry]),
    #c{name = Name} = C,
    Routed = case orddict:find(routed_clusters, Meta) of
        error -> [];
        {ok, V} -> V
    end,
    case lists:member(Name, Routed) of
        true when C#c.delivered ->
            Skipped = C#c.skips + 1,
            {skipped, C#c{skips = Skipped, cseq = Seq}};
        true ->
            {skipped, C#c{cseq = Seq, aseq = Seq}};
        false ->
            {delivered, deliver_item(C, C#c.deliver, QEntry)}
    end.

deliver_item(C, DeliverFun, {Seq,_NumItem, _Bin, _Meta} = QEntry) ->
    try
        Seq = C#c.cseq + 1, % bit of paranoia, remove after EQC
        QEntry2 = set_skip_meta(QEntry, Seq, C),
        lager:info("in deliver item, QEntry2=~p", [QEntry2]),
        case DeliverFun(QEntry2) of
            ok ->
                C#c{cseq = Seq, deliver = undefined, delivered = true, skips = 0};
            skipped ->
                      C#c{cseq = Seq, deliver = undefined, delivered = true, skips = 0}
        end
    catch
        _:_ ->
            riak_repl_stats:rt_source_errors(),
            %% do not advance head so it will be delivered again
            C#c{errs = C#c.errs + 1, deliver = undefined}
    end.

% if nothing has been delivered, the sink assumes nothing was skipped
% fulfill that expectation.
set_skip_meta(QEntry, _Seq, _C = #c{delivered = false}) ->
    set_meta(QEntry, skip_count, 0);
set_skip_meta(QEntry, _Seq, _C = #c{skips = S}) ->
    set_meta(QEntry, skip_count, S).

set_local_forwards_meta(LocalForwards, QEntry) ->
    set_meta(QEntry, local_forwards, LocalForwards).

set_meta({_Seq, _NumItems, _Bin, Meta} = QEntry, Key, Value) ->
    Meta2 = orddict:store(Key, Value, Meta),
    setelement(4, QEntry, Meta2).

%% Cleanup until the start of the table
cleanup(_QTab, '$end_of_table', State) ->
    State;
cleanup(QTab, Seq, State) ->
    case ets:lookup(QTab, Seq) of
        [] -> cleanup(QTab, ets:prev(QTab, Seq), State);
        [{_, _, Bin, _Meta}] ->
           ShrinkSize = ets_obj_size(Bin, State),
           NewState = update_q_size(State, -ShrinkSize),
           ets:delete(QTab, Seq),
           cleanup(QTab, ets:prev(QTab, Seq), NewState);
        _ ->
            lager:warning("Unexpected object in RTQ")
    end.

ets_obj_size(Obj, #state{word_size = WordSize}) when is_binary(Obj) ->
  ets_obj_size(Obj, WordSize);
ets_obj_size(Obj, WordSize) when is_binary(Obj) ->
  BSize = erlang:byte_size(Obj),
  case BSize > 64 of
        true -> BSize - (6 * WordSize);
        false -> BSize
  end;
ets_obj_size(Obj, _) ->
  erlang:size(Obj).

update_q_size(State = #state{qsize_bytes = CurrentQSize}, Diff) ->
  State#state{qsize_bytes = CurrentQSize + Diff}.

clear_non_deliverables(QTab, ActiveConsumers, WordSize) ->
    Accumulator = fun(QEntry, Acc) ->
        {Seq, _, _, Meta} = QEntry,
        Routed = case orddict:find(routed_clusters, Meta) of
            error -> [];
            {ok, V} -> V
        end,
        RoutableActives = [AC || AC <- ActiveConsumers, AC#c.aseq < Seq, not lists:member(AC#c.name, Routed)],
        if
            RoutableActives == [] ->
                [Seq | Acc];
            true ->
                Acc
        end
    end,
    ToDelete = ets:foldl(Accumulator, [], QTab),
    DeleteFun = fun(Key, Acc) ->
      [{Key, _NumItems, Bin, _Meta}] = ets:lookup(QTab, Key),
      Size = ets_obj_size(Bin, WordSize),
      ets:delete(QTab, Key),
      Acc + Size
    end,
    Shrink = lists:foldl(DeleteFun, 0, ToDelete),
    {Shrink, ToDelete}.

union_undeliverables(SeqSet1, SeqSet2, MinSeq) ->
    Undeliverables = ordsets:union(SeqSet1, SeqSet2),
    lists:filter(fun(E) -> E >= MinSeq end, Undeliverables).

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
            [{_, _, Bin, _Meta}] = ets:lookup(QTab, TrimSeq),
            ShrinkSize = ets_obj_size(Bin, State),
            NewState = update_q_size(State, -ShrinkSize),
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

