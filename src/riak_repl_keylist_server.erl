%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_keylist_server).

%% @doc This is the server-side component of the new fullsync strategy
%% introduced in riak 1.1. It is an improvement over the previous strategy in
%% several ways:
%%
%% * Client and server build keylist in parallel
%% * No useless merkle tree is built
%% * Differences are calculated and transmitted in batches, not all in one
%%   message
%% * Backpressure is introduced in the exchange of differences
%% * Pausing/cancelling the diff is immediate
%%
%% In addition, the client does the requesting of partition data, which makes
%% this more of a pull model as compared to the legacy strategy, which was more
%% push oriented. The new protocol is outlined below.
%%
%% When the server receives a message to begin a fullsync, it checks that all
%% nodes in the cluster support the new bloom_fold capability, and relays the
%% command to the client. If bloom_fold is not supported by all nodes, it will
%% ignore the command and check again on the next fullsync request.
%%
%% For fullsync, the client builds the partition list and instructs the server
%% to build the keylist for the first partition, while also starting off its own
%% keylist build locally. When *both* builds are complete, the client sends
%% the keylist to the server. The server does the diff and then sends *any*
%% differing keys to the client, using the realtime repl protocol. This is a
%% departure from the legacy protocol in which vector clocks were available
%% for comparison. However, worst case is we try to write stale keys, which
%% will be ignored by the put_fsm. Once all the diffs are sent (and a final
%% ack is received), the client moves onto the next partition, if any.
%%
%% Note that the new key list algorithm uses a bloom fold filter to keep the
%% keys in disk-order to speed up the key-list creation process.

-behaviour(gen_fsm).

%% API
-export([start_link/5,
        start_fullsync/1,
        cancel_fullsync/1,
        pause_fullsync/1,
        resume_fullsync/1
    ]).

%% gen_fsm
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

%% states
-export([wait_for_partition/2,
        build_keylist/2,
        wait_keylist/2,
        diff_keylist/2,
        diff_bloom/2,
        diff_bloom/3,
        bloom_fold/3]).

-record(state, {
        sitename,
        socket,
        transport,
        work_dir,
        client,
        kl_pid,
        kl_ref,
        kl_fn,
        kl_fh,
        their_kl_fn,
        their_kl_fh,
        partition,
        diff_pid,
        diff_ref,
        stage_start,
        partition_start,
        pool,
        vnode_gets = true,
        diff_batch_size,
        bloom,
        bloom_pid,
        num_diffs,
        generator_paused = false,
        pending_acks = 0
    }).

%% -define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

-define(ACKS_IN_FLIGHT,2).

%% This is currently compared against the number of keys, not really
%% the number of differences, because we don't have a fast way to count
%% the differences before we start generating the diff stream. But, if
%% the number of keys is small, then we know the number of diffs is small
%% too. TODO: when we change the diff generator to use hash trees, revisit
%% this threshold to compare it to the actual number of differences or an
%% estimate of them.
-define(KEY_LIST_THRESHOLD,(1024)).

start_link(SiteName, Transport, Socket, WorkDir, Client) ->
    gen_fsm:start_link(?MODULE, [SiteName, Transport, Socket, WorkDir, Client], []).

start_fullsync(Pid) ->
    Pid ! start_fullsync.

cancel_fullsync(Pid) ->
    gen_fsm:send_event(Pid, cancel_fullsync).

pause_fullsync(Pid) ->
    gen_fsm:send_event(Pid, pause_fullsync).

resume_fullsync(Pid) ->
    gen_fsm:send_event(Pid, resume_fullsync).

stop(Pid) ->
    gen_server2:call(Pid, stop, infinity).

init([SiteName, Transport, Socket, WorkDir, Client]) ->
    MinPool = app_helper:get_env(riak_repl, min_get_workers, 5),
    MaxPool = app_helper:get_env(riak_repl, max_get_workers, 100),
    VnodeGets = app_helper:get_env(riak_repl, vnode_gets, true),
    DiffBatchSize = app_helper:get_env(riak_repl, diff_batch_size, 100),
    {ok, Pid} = poolboy:start_link([{worker_module, riak_repl_fullsync_worker},
            {worker_args, []},
            {size, MinPool}, {max_overflow, MaxPool}]),
    State = #state{sitename=SiteName, socket=Socket, transport=Transport,
        work_dir=WorkDir, client=Client, pool=Pid, vnode_gets=VnodeGets,
        diff_batch_size=DiffBatchSize},
    riak_repl_util:schedule_fullsync(),
    {ok, wait_for_partition, State}.

%% Request to start or resume Full Sync
wait_for_partition(Command, State)
        when Command == start_fullsync; Command == resume_fullsync ->
    %% annoyingly the server is the one that triggers the fullsync in the old
    %% protocol, so we'll just send it on to the client.
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    {next_state, wait_for_partition, State};
%% Full sync has completed
wait_for_partition(fullsync_complete, State) ->
    lager:info("Full-sync with site ~p completed", [State#state.sitename]),
    riak_repl_stats:server_fullsyncs(),
    riak_repl_util:schedule_fullsync(),
    {next_state, wait_for_partition, State};
%% Start full-sync of a partition
%% @plu server <- client: {partition,P}
wait_for_partition({partition, Partition}, State=#state{work_dir=WorkDir}) ->
    lager:info("Full-sync with site ~p; doing fullsync for ~p",
        [State#state.sitename, Partition]),

    lager:info("Full-sync with site ~p; building keylist for ~p",
        [State#state.sitename, Partition]),
    %% client wants keylist for this partition
    TheirKeyListFn = riak_repl_util:keylist_filename(WorkDir, Partition, theirs),
    KeyListFn = riak_repl_util:keylist_filename(WorkDir, Partition, ours),
    {ok, KeyListPid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, KeyListRef} = riak_repl_fullsync_helper:make_keylist(KeyListPid,
                                                                 Partition,
                                                                 KeyListFn),
    {next_state, build_keylist, State#state{kl_pid=KeyListPid,
            kl_ref=KeyListRef, kl_fn=KeyListFn,
            partition=Partition, partition_start=now(), stage_start=now(),
            pending_acks=0, generator_paused=false,
            their_kl_fn=TheirKeyListFn, their_kl_fh=undefined}};
%% Unknown event (ignored)
wait_for_partition(Event, State) ->
    lager:debug("Full-sync with site ~p; ignoring event ~p",
        [State#state.sitename, Event]),
    {next_state, wait_for_partition, State}.

build_keylist(Command, #state{kl_pid=Pid} = State)
        when Command == cancel_fullsync; Command == pause_fullsync ->
    %% kill the worker
    riak_repl_fullsync_helper:stop(Pid),
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    file:delete(State#state.kl_fn),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
%% Helper has sorted and written keylist to a file
%% @plu server <- s:key-lister: keylist_built
build_keylist({Ref, keylist_built, Size},
              State=#state{kl_ref=Ref, socket=Socket, transport=Transport, partition=Partition}) ->
    lager:info("Full-sync with site ~p; built keylist for ~p (built in ~p secs)",
        [State#state.sitename, Partition,
         riak_repl_util:elapsed_secs(State#state.stage_start)]),
    %% @plu server -> client: {kl_exchange, P}
    riak_repl_tcp_server:send(Transport, Socket, {kl_exchange, Partition}),
    %% note that num_diffs is being assigned the number of keys, regardless of diffs,
    %% because we don't the number of diffs yet. See TODO: above redarding KEY_LIST_THRESHOLD
    {next_state, wait_keylist, State#state{stage_start=now(), num_diffs=Size}};
%% Error
build_keylist({Ref, {error, Reason}}, #state{transport=Transport,
        socket=Socket, kl_ref=Ref} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p because of error ~p",
        [State#state.sitename, State#state.partition, Reason]),
    riak_repl_tcp_server:send(Transport, Socket, {skip_partition, State#state.partition}),
    {next_state, wait_for_partition, State};
build_keylist({_Ref, keylist_built, _Size}, State) ->
    lager:warning("Stale keylist_built message received, ignoring"),
    {next_state, build_keylist, State};
build_keylist({_Ref, {error, Reason}}, State) ->
    lager:warning("Stale {error, ~p} message received, ignoring", [Reason]),
    {next_state, build_keylist, State};
%% Request to skip specified partition
build_keylist({skip_partition, Partition}, #state{partition=Partition,
        kl_pid=Pid} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p as requested by client",
        [State#state.sitename, Partition]),
    catch(riak_repl_fullsync_helper:stop(Pid)),
    {next_state, wait_for_partition, State}.

wait_keylist(Command, #state{their_kl_fh=FH} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    case FH of
        undefined ->
            ok;
        _ ->
            %% close and delete the keylist file
            file:close(FH),
            file:delete(State#state.their_kl_fn),
            file:delete(State#state.kl_fn)
    end,
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
wait_keylist(kl_wait, State) ->
    %% ack the keylist chunks we've received so far
    %% @plu    server -> client: kl_ack
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, kl_ack),
    {next_state, wait_keylist, State};
%% I have recieved a chunk of the keylist
wait_keylist({kl_hunk, Hunk}, #state{their_kl_fh=FH0} = State) ->
    FH = case FH0 of
        undefined ->
            {ok, F} = file:open(State#state.their_kl_fn, [write, raw, binary,
                    delayed_write]),
            F;
        _ ->
            FH0
    end,
    file:write(FH, Hunk),
    {next_state, wait_keylist, State#state{their_kl_fh=FH}};
%% the client has finished sending the keylist
wait_keylist(kl_eof, #state{their_kl_fh=FH, num_diffs=NumKeys} = State) ->
    case FH of
        undefined ->
            %% client has a blank vnode, write a blank file
            file:write_file(State#state.their_kl_fn, <<>>),
            ok;
        _ ->
            file:sync(FH),
            file:close(FH),
            ok
    end,
    lager:info("Full-sync with site ~p; received keylist for ~p (received in ~p secs)",
        [State#state.sitename, State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    ?TRACE(lager:info("Full-sync with site ~p; calculating ~p differences for ~p",
                      [State#state.sitename, NumDKeys, State#state.partition])),
    {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),

    %% check capability of all nodes for bloom fold ability.
    %% Since we are the leader, the fact that we have this
    %% new code means we can only choose to use it if
    %% all nodes have been upgraded to use bloom.
    NextState = case riak_core_capability:get({riak_repl, bloom_fold}, false) of
        true ->
            %% all nodes support bloom, yay

            %% don't need the diff stream to pause itself
            DiffSize = 0,
            {ok, Bloom} = ebloom:new(NumKeys, 0.01, random:uniform(1000)),
            diff_bloom;
        false ->
            DiffSize = State#state.diff_batch_size div ?ACKS_IN_FLIGHT,
            Bloom = undefined,
            diff_keylist
    end,

    {ok, Ref} = riak_repl_fullsync_helper:diff_stream(Pid, State#state.partition,
                                                      State#state.kl_fn,
                                                      State#state.their_kl_fn,
                                                      DiffSize),

    lager:info("Full-sync with site ~p; using ~p for ~p",
               [State#state.sitename, NextState, State#state.partition]),
    {next_state, NextState, State#state{diff_ref=Ref, bloom=Bloom, diff_pid=Pid, stage_start=now()}};
wait_keylist({skip_partition, Partition}, #state{partition=Partition} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p as requested by client",
        [State#state.sitename, Partition]),
    {next_state, wait_for_partition, State}.

%% ----------------------------------- non bloom-fold -----------------------
%% diff_keylist states

diff_keylist(Command, #state{diff_pid=Pid} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    riak_repl_fullsync_helper:stop(Pid),
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
%% @plu server <-- diff_stream : merkle_diff
diff_keylist({Ref, {merkle_diff, {{B, K}, _VClock}}}, #state{
        transport=Transport, socket=Socket, diff_ref=Ref, pool=Pool} = State) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    case State#state.vnode_gets of
        true ->
            %% do a direct get against the vnode, not a regular riak client
            %% get().
            ok = riak_repl_fullsync_worker:do_get(Worker, B, K, Transport, Socket, Pool,
                State#state.partition);
        _ ->
            ok = riak_repl_fullsync_worker:do_get(Worker, B, K, Transport, Socket, Pool)
    end,
    {next_state, diff_keylist, State};
%% @plu server <-- key-lister: diff_paused
diff_keylist({Ref, diff_paused}, #state{socket=Socket, transport=Transport,
        partition=Partition, diff_ref=Ref, pending_acks=PendingAcks0} = State) ->
    %% request ack from client
    riak_repl_tcp_server:send(Transport, Socket, {diff_ack, Partition}),
    PendingAcks = PendingAcks0+1,
    %% If we have already received the ack for the previous batch, we immediately
    %% resume the generator, otherwise we wait for the ack from the client. We'll
    %% have at most ACKS_IN_FLIGHT windows of differences in flight.
    WorkerPaused = case PendingAcks < ?ACKS_IN_FLIGHT of
                       true ->
                           %% another batch can be sent immediately
                           State#state.diff_pid ! {Ref, diff_resume},
                           false;
                       false ->
                           %% already ACKS_IN_FLIGHT batches out. Don't resume yet.
                           true
                   end,
    {next_state, diff_keylist, State#state{pending_acks=PendingAcks,
                                           generator_paused=WorkerPaused}};
%% @plu server <-- client: diff_ack
diff_keylist({diff_ack, Partition}, #state{partition=Partition, diff_ref=Ref,
                                           generator_paused=WorkerPaused,
                                           pending_acks=PendingAcks0} = State) ->
    %% That's one less "pending" ack from the client. Tell client to keep going.
    PendingAcks = PendingAcks0-1,
    %% If the generator was paused, resume it. That would happen if there are already
    %% ACKS_IN_FLIGHT batches in flight. Better to check "paused" state than guess by
    %% pending acks count.
    case WorkerPaused of
        true ->
            State#state.diff_pid ! {Ref, diff_resume};
        false ->
            ok
    end,
    {next_state, diff_keylist, State#state{pending_acks=PendingAcks,generator_paused=false}};
diff_keylist({Ref, diff_done}, #state{diff_ref=Ref} = State) ->
    lager:info("Full-sync with site ~p; differences exchanged for partition ~p (done in ~p secs)",
        [State#state.sitename, State#state.partition,
         riak_repl_util:elapsed_secs(State#state.stage_start)]),
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, diff_done),
    {next_state, wait_for_partition, State}.

%% ----------------------------------- bloom-fold ---------------------------
%% diff_bloom states

%% Pause or Cancel
diff_bloom(Command, #state{diff_pid=Pid} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    ?MODULE:stop(Pid),
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};

%% Sent by streaming difference generator when hashed keys are different.
%% @plu server <- s:helper : merke_diff
diff_bloom({Ref, {merkle_diff, {{B, K}, _VClock}}}, #state{diff_ref=Ref, bloom=Bloom} = State) ->
    ebloom:insert(Bloom, <<B/binary, K/binary>>),
    {next_state, diff_bloom, State};

%% Sent by the fullsync_helper "streaming" difference generator when it's done.
%% @plu server <- s:helper : diff_done
diff_bloom({Ref, diff_done}, #state{diff_ref=Ref, partition=Partition, bloom=Bloom} = State) ->
    lager:info("Full-sync with site ~p; fullsync difference generator for ~p complete (completed in ~p secs)",
               [State#state.sitename, State#state.partition,
                riak_repl_util:elapsed_secs(State#state.partition_start)]),
    case ebloom:elements(Bloom) == 0 of
        true ->
            lager:info("No differences, skipping bloom fold"),
            riak_repl_tcp_server:send(State#state.transport, State#state.socket, diff_done),
            {next_state, wait_for_partition, State};
        false ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            OwnerNode = riak_core_ring:index_owner(Ring, Partition),

            Self = self(),
            DiffSize = State#state.diff_batch_size,
            Worker = fun() ->
                    riak_kv_vnode:fold({Partition,OwnerNode},
                        fun ?MODULE:bloom_fold/3, {Self,
                            {serialized, ebloom:serialize(Bloom)},
                            State#state.client, State#state.transport,
                            State#state.socket, DiffSize, DiffSize}),
                    gen_fsm:send_event(Self, {Ref, diff_exchanged}),
                    ok
            end,
            spawn_link(Worker), %% this isn't the Pid we need because it's just the vnode:fold
            {next_state, diff_bloom, State#state{bloom_pid=undefined}}
    end;

%% @plu server <-- s:helper : diff_paused
%% For bloom folding, we don't want the difference generator to pause at all.
diff_bloom({Ref,diff_paused}, #state{diff_ref=Ref} = State) ->
    ?TRACE(lager:info("diff_bloom <- diff_keys: {Ref, diff_paused}. resuming diff gen for ~p",
                      [State#state.partition])),
    ?TRACE(lager:info("diff_bloom -> diff_keys: {Ref, diff_resume}")),
    State#state.diff_pid ! {Ref, diff_resume},
    {next_state, diff_bloom, State};

%% Sent by bloom_folder after a window of diffs have been sent and it paused itself.
%% @plu server <-- bloom_fold: {BloomFoldPid, bloom_paused}
diff_bloom({BFPid,bloom_paused}, #state{socket=Socket, transport=Transport,
        partition=Partition, pending_acks=PendingAcks0} = State) ->
    ?TRACE(lager:info("diff_bloom <- bloom_paused")),
    %% request ack from client
    riak_repl_tcp_server:send(Transport, Socket, {diff_ack, Partition}),
    PendingAcks = PendingAcks0+1,
    %% If we have already received the ack for the previous batch, we immediately
    %% resume the generator, otherwise we wait for the ack from the client. We'll
    %% have at most ACKS_IN_FLIGHT windows of differences in flight.
    WorkerPaused = case PendingAcks < ?ACKS_IN_FLIGHT of
                       true ->
                           %% another batch can be sent immediately
                           ?TRACE(lager:info("diff_bloom resuming bloom worker immediately")),
                           ?TRACE(lager:info("diff_bloom -> ~p : bloom_resume", [BFPid])),
                           BFPid ! bloom_resume,
                           false;
                       false ->
                           %% already ACKS_IN_FLIGHT batches out. Don't resume yet.
                           true
                   end,
    ?TRACE(lager:info("diff_bloom WorkerPaused = ~p, PendingAcks = ~p", [WorkerPaused, PendingAcks])),
    {next_state, diff_bloom, State#state{pending_acks=PendingAcks,
                                         generator_paused=WorkerPaused,
                                         bloom_pid=BFPid}};

%% @plu server <-- client : diff_ack 'when ready for more
diff_bloom({diff_ack, Partition}, #state{partition=Partition,
                                         generator_paused=WorkerPaused,
                                         pending_acks=PendingAcks0} = State) ->
    %% That's one less "pending" ack from the client. Tell client to keep going.
    PendingAcks = PendingAcks0-1,
    ?TRACE(lager:info("diff_bloom <- diff_ack: PendingAcks = ~p", [PendingAcks])),
    %% If the generator was paused, resume it. That would happen if there are already
    %% ACKS_IN_FLIGHT batches in flight. Better to check "paused" state than guess by
    %% pending acks count.
    case WorkerPaused of
        true ->
            ?TRACE(lager:info("diff_bloom resuming bloom fold worker after ACK")),
            ?TRACE(lager:info("diff_bloom -> ~p : bloom_resume",
                              [State#state.bloom_pid])),
            State#state.bloom_pid ! bloom_resume;
        false ->
            ok
    end,
    {next_state, diff_bloom, State#state{pending_acks=PendingAcks,generator_paused=false}};

%% Sent by the Worker function after the bloom_fold exchanges a partition's worth of diffs
%% with the client.
%% @plu server <- bloom_fold : diff_exchanged 'all done
diff_bloom({Ref,diff_exchanged},  #state{diff_ref=Ref} = State) ->
    %% Tell client that we're done with differences for this partition.
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, diff_done),
    lager:info("Full-sync with site ~p; differences exchanged for partition ~p (done in ~p secs)",
               [State#state.sitename, State#state.partition,
                riak_repl_util:elapsed_secs(State#state.stage_start)]),
    %% Wait for another partition.
    {next_state, wait_for_partition, State}.

%% end of bloom states
%% --------------------------------------------------------------------------

%% server <- bloom_fold : diff_obj 'recv a diff object from bloom folder
diff_bloom({diff_obj, RObj}, _From, #state{client=Client, transport=Transport,
                                           socket=Socket} = State) ->
    case riak_repl_util:repl_helper_send(RObj, Client) of
        cancel ->
            skipped;
        Objects when is_list(Objects) ->
            %% server -> client : fs_diff_obj
            [riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, O}) || O <- Objects],
            riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, RObj})
    end,
    {reply, ok, diff_bloom, State}.

%% gen_fsm callbacks

handle_event(_Event, StateName, State) ->
    lager:debug("Full-sync with site ~p; ignoring ~p", [State#state.sitename, _Event]),
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Res = [{state, StateName}] ++
    case StateName of
        wait_for_partition ->
            [];
        _ ->
            [
                {fullsync, State#state.partition},
                {partition_start,
                    riak_repl_util:elapsed_secs(State#state.partition_start)},
                {stage_start,
                    riak_repl_util:elapsed_secs(State#state.stage_start)},
                {get_pool_size,
                    length(gen_fsm:sync_send_all_state_event(State#state.pool,
                            get_all_workers, infinity))}
            ]
    end,
    {reply, Res, StateName, State};
handle_sync_event(stop,_F,_StateName,State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event,_F,StateName,State) ->
    lager:debug("Fullsync with site ~p; ignoring ~p", [State#state.sitename,_Event]),
    {reply, ok, StateName, State}.

handle_info(start_fullsync, wait_for_partition, State) ->
    gen_fsm:send_event(self(), start_fullsync),
    {next_state, wait_for_partition, State};
handle_info(_I, StateName, State) ->
    lager:info("Full-sync with site ~p; ignoring ~p", [State#state.sitename, _I]),
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) -> 
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd),
    poolboy:stop(State#state.pool).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% internal funtions

log_stop(Command, State) ->
    lager:info("Full-sync with site ~p; ~s at partition ~p (after ~p secs)",
        [State#state.sitename, command_verb(Command), State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]).

command_verb(cancel_fullsync) ->
    "cancelled";
command_verb(pause_fullsync) ->
    "paused".

%% This folder will send batches of differences to the client. Each batch is "WinSz"
%% riak objects. After a batch is sent, it will pause itself and wait to be resumed
%% by receiving "bloom_resume".
bloom_fold(BK, V, {MPid, {serialized, SBloom}, Client, Transport, Socket, NSent, WinSz}) ->
    {ok, Bloom} = ebloom:deserialize(SBloom),
    bloom_fold(BK, V, {MPid, Bloom, Client, Transport, Socket, NSent, WinSz});
bloom_fold({B, K}, V, {MPid, Bloom, Client, Transport, Socket, 0, WinSz} = Acc) ->
    ?TRACE(lager:info("bloom_fold -> MPid(~p) : bloom_paused", [MPid])),
    gen_fsm:send_event(MPid, {self(), bloom_paused}),
    %% wait for a message telling us to stop, or to continue.
    %% TODO do this more correctly when there's more time.
    receive
        {'$gen_call', From, stop} ->
            gen_server2:reply(From, ok),
            Acc;
        bloom_resume ->
            ?TRACE(lager:info("bloom_fold <- MPid(~p) : bloom_resume", [MPid])),
            bloom_fold({B,K}, V, {MPid, Bloom, Client, Transport, Socket, WinSz, WinSz});
        Other ->
            ?TRACE(lager:info("bloom_fold <- ? : ~p", [Other]))
    end;
bloom_fold({B, K}, V, {MPid, Bloom, Client, Transport, Socket, NSent0, WinSz}) ->
    NSent = case ebloom:contains(Bloom, <<B/binary, K/binary>>) of
                true ->
                    RObj = binary_to_term(V),
                    gen_fsm:sync_send_event(MPid, {diff_obj, RObj}, infinity),
                    NSent0 - 1;
                false ->
                    ok,
                    NSent0
            end,
    {MPid, Bloom, Client, Transport, Socket, NSent, WinSz}.

