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
%% When the server receives a message to begin a fullsync, it relays it to the
%% client. The client builds the partition list and instructs the server to
%% build the keylist for the first partition, while also starting off its own
%% keylist build locally. When *both* builds are complete, the client sends
%% the keylist to the server. The server does the diff and then sends *any*
%% differing keys to the client, using the realtime repl protocol. This is a
%% departure from the legacy protocol in which vector clocks were available
%% for comparison. However, worst case is we try to write stale keys, which
%% will be ignored by the put_fsm. Once all the diffs are sent (and a final
% ack is received). The client moves onto the next partition, if any.

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
        diff_keylist/3,
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
        bloom
    }).


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

wait_for_partition(Command, State)
        when Command == start_fullsync; Command == resume_fullsync ->
    %% annoyingly the server is the one that triggers the fullsync in the old
    %% protocol, so we'll just send it on to the client.
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    {next_state, wait_for_partition, State};
wait_for_partition(fullsync_complete, State) ->
    lager:info("Full-sync with site ~p completed", [State#state.sitename]),
    riak_repl_stats:server_fullsyncs(),
    riak_repl_util:schedule_fullsync(),
    {next_state, wait_for_partition, State};
wait_for_partition({partition, Partition}, State) ->
    lager:info("Full-sync with site ~p; doing fullsync for ~p",
        [State#state.sitename, Partition]),
    gen_fsm:send_event(self(), continue),
    {next_state, build_keylist, State#state{partition=Partition,
            partition_start=now()}};
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
build_keylist(continue, #state{partition=Partition, work_dir=WorkDir} = State) ->
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
            kl_ref=KeyListRef, kl_fn=KeyListFn, stage_start=now(),
            their_kl_fn=TheirKeyListFn, their_kl_fh=undefined}};
build_keylist({Ref, keylist_built, Size}, State=#state{kl_ref=Ref, socket=Socket,
    transport=Transport, partition=Partition}) ->
    lager:info("Full-sync with site ~p; built keylist for ~p (built in ~p secs)",
        [State#state.sitename, Partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    riak_repl_tcp_server:send(Transport, Socket, {kl_exchange, Partition}),
    {ok, Bloom} = ebloom:new(Size, 0.01, random:uniform(1000)),
    {next_state, wait_keylist, State#state{stage_start=now(),
            bloom=Bloom}};
build_keylist({Ref, {error, Reason}}, #state{transport=Transport,
        socket=Socket, kl_ref=Ref} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p because of error ~p",
        [State#state.partition, Reason]),
    riak_repl_tcp_server:send(Transport, Socket, {skip_partition, State#state.partition}),
    {next_state, wait_for_partition, State};
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
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, kl_ack),
    {next_state, wait_keylist, State};
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
wait_keylist(kl_eof, #state{their_kl_fh=FH} = State) ->
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
    lager:info("Full-sync with site ~p; calculating differences for ~p",
        [State#state.sitename, State#state.partition]),
    {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),
    %% generate differences in batches of 1000, to add some backpressure
    {ok, Ref} = riak_repl_fullsync_helper:diff_stream(Pid, State#state.partition,
        State#state.kl_fn, State#state.their_kl_fn,
        State#state.diff_batch_size),
    {next_state, diff_keylist, State#state{diff_ref=Ref, diff_pid=Pid,
            stage_start=now()}};
wait_keylist({skip_partition, Partition}, #state{partition=Partition} = State) ->
    lager:warning("Full-sync with site ~p; skipping partition ~p as requested by client",
        [State#state.sitename, Partition]),
    {next_state, wait_for_partition, State}.


diff_keylist(Command, #state{diff_pid=Pid} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    riak_repl_fullsync_helper:stop(Pid),
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
%diff_keylist({Ref, {merkle_diff, {{B, K}, _VClock}}}, #state{
        %transport=Transport, socket=Socket, diff_ref=Ref, pool=Pool} = State) ->
    %Worker = poolboy:checkout(Pool, true, infinity),
    %case State#state.vnode_gets of
        %true ->
            % do a direct get against the vnode, not a regular riak client
            % get().
            %ok = riak_repl_fullsync_worker:do_get(Worker, B, K, Transport, Socket, Pool,
                %State#state.partition);
        %_ ->
            %ok = riak_repl_fullsync_worker:do_get(Worker, B, K, Transport, Socket, Pool)
    %end,
    %{next_state, diff_keylist, State};
diff_keylist({Ref, {merkle_diff, {{B, K}, _VClock}}}, #state{diff_ref=Ref, bloom=Bloom} = State) ->
    ebloom:insert(Bloom, <<B/binary, K/binary>>),
    {next_state, diff_keylist, State};
diff_keylist({Ref, diff_paused}, #state{socket=Socket, transport=Transport,
        partition=Partition, diff_ref=Ref} = State) ->
    %% we've sent all the diffs in this batch, wait for the client to be ready
    %% for more
    %riak_repl_tcp_server:send(Transport, Socket, {diff_ack, Partition}),
    State#state.diff_pid ! {Ref, diff_resume},
    {next_state, diff_keylist, State};
%diff_keylist({diff_ack, Partition}, #state{partition=Partition, diff_ref=Ref} = State) ->
    %% client has processed last batch of differences, generate some more
    %State#state.diff_pid ! {Ref, diff_resume},
    %{next_state, diff_keylist, State};
diff_keylist({Ref, diff_exchanged}, #state{diff_ref=Ref, partition=Partition} = State) ->
    riak_repl_tcp_server:send(State#state.transport, State#state.socket, diff_done),
    lager:info("done exchanging keys"),
    {next_state, wait_for_partition, State};
diff_keylist({Ref, diff_done}, #state{diff_ref=Ref, partition=Partition} = State) ->
    %lager:info("Full-sync with site ~p; differences exchanged for partition ~p (done in ~p secs)",
        %[State#state.sitename, State#state.partition,
            %riak_repl_util:elapsed_secs(State#state.stage_start)]),
    %lager:info("Full-sync with site ~p; fullsync for ~p complete (completed in ~p secs)",
        %[State#state.sitename, State#state.partition,
            %riak_repl_util:elapsed_secs(State#state.partition_start)]),
    %riak_repl_tcp_server:send(State#state.transport, State#state.socket, diff_done),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    OwnerNode = riak_core_ring:index_owner(Ring, Partition),

    Self = self(),
    Worker = fun() ->
            riak_kv_vnode:fold({Partition,OwnerNode},
                fun ?MODULE:bloom_fold/3, {Self,
                    {serialized, ebloom:serialize(State#state.bloom)},
                    State#state.client, State#state.transport,
                    State#state.socket}),
            %gen_server2:cast(Self, merkle_finish)
            lager:info("Done folding keys"),
            gen_fsm:send_event(Self, {Ref, diff_exchanged}),
            ok
    end,
    FolderPid = spawn_link(Worker),

    {next_state, diff_keylist, State}.

diff_keylist({diff_obj, RObj}, _From, #state{client=Client, transport=Transport,
        socket=Socket} = State) ->
    case riak_repl_util:repl_helper_send(RObj, Client) of
        cancel ->
            skipped;
        Objects when is_list(Objects) ->
            [riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, O}) || O <- Objects],
            riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, RObj})
    end,
    {reply, ok, diff_keylist, State}.

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

bloom_fold(BK, V, {MPid, {serialized, SBloom}, Client, Transport, Socket}) ->
    lager:info("deserializing bloom filter"),
    {ok, Bloom} = ebloom:deserialize(SBloom),
    lager:info("deserialized bloom filter"),
    bloom_fold(BK, V, {MPid, Bloom, Client, Transport, Socket});
bloom_fold({B, K}, V, {MPid, Bloom, Client, Transport, Socket} = Acc) ->
    case ebloom:contains(Bloom, <<B/binary, K/binary>>) of
        true ->
            RObj = binary_to_term(V),
            gen_fsm:sync_send_event(MPid, {diff_obj, RObj}, infinity);

            %lager:info("replicate ~p/~p", [B, K]),
        false ->
            %lager:info("not replicating ~p/~p", [B, K])
            ok
    end,
    Acc.

