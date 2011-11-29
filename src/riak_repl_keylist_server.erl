
-module(riak_repl_keylist_server).

-behaviour(gen_fsm).

%% API
-export([start_link/4]).

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
        diff_keylist/2]).

-record(state, {
        sitename,
        socket,
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
        partition_start
    }).


start_link(SiteName, Socket, WorkDir, Client) ->
    gen_fsm:start_link(?MODULE, [SiteName, Socket, WorkDir, Client], []).

init([SiteName, Socket, WorkDir, Client]) ->
    State = #state{sitename=SiteName, socket=Socket,
        work_dir=WorkDir, client=Client},
    riak_repl_util:schedule_fullsync(),
    {ok, wait_for_partition, State}.

wait_for_partition(Command, State)
        when Command == start_fullsync; Command == resume_fullsync ->
    %% annoyingly the server is the one that triggers the fullsync in the old
    %% protocol, so we'll just send it on to the client.
    riak_repl_tcp_server:send(State#state.socket, Command),
    {next_state, wait_for_partition, State};
wait_for_partition(fullsync_complete, State) ->
    lager:notice("Fullsync with site ~p completed", [State#state.sitename]),
    riak_repl_stats:server_fullsyncs(),
    {next_state, wait_for_partition, State};
wait_for_partition({partition, Partition}, State) ->
    lager:notice("Doing fullsync for ~p", [Partition]),
    {next_state, build_keylist, State#state{partition=Partition,
            partition_start=now()}, 0};
wait_for_partition(Event, State) ->
    lager:info("ignoring event ~p", [Event]),
    {next_state, wait_for_partition, State}.

build_keylist(Command, #state{kl_pid=Pid} = State)
        when Command == cancel_fullsync; Command == pause_fullsync ->
    %% kill the worker
    riak_repl_fullsync_helper:stop(Pid),
    riak_repl_tcp_server:send(State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
build_keylist(timeout, #state{partition=Partition, work_dir=WorkDir} = State) ->
    lager:notice("Building keylist for ~p", [Partition]),
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
build_keylist({Ref, keylist_built}, State=#state{kl_ref = Ref, socket=Socket,
    partition=Partition}) ->
    lager:notice("Built keylist for ~p (built in ~p secs)", [Partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    riak_repl_tcp_server:send(Socket, {kl_exchange, Partition}),
    {next_state, wait_keylist, State#state{stage_start=now()}}.

wait_keylist(Command, #state{their_kl_fh=FH} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    case FH of
        undefined ->
            ok;
        _ ->
            %% close and delete the keylist file
            file:close(FH),
            file:delete(State#state.their_kl_fn)
    end,
    riak_repl_tcp_server:send(State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
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
            %% client has a blank vnode
            file:write_file(State#state.their_kl_fn, <<>>),
            ok;
        _ ->
            file:sync(FH),
            file:close(FH),
            ok
    end,
    lager:notice("Received keylist for ~p (received in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    lager:notice("Calculating differences for ~p", [State#state.partition]),
    {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, Ref} = riak_repl_fullsync_helper:diff_stream(Pid, State#state.partition,
        State#state.kl_fn, State#state.their_kl_fn, 1000),
    {next_state, diff_keylist, State#state{diff_ref=Ref, diff_pid=Pid,
            stage_start=now()}}.

diff_keylist(Command, #state{diff_pid=Pid} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    riak_repl_fullsync_helper:stop(Pid),
    riak_repl_tcp_server:send(State#state.socket, Command),
    log_stop(Command, State),
    {next_state, wait_for_partition, State};
diff_keylist({Ref, {merkle_diff, {{B, K}, _VClock}}}, #state{client=Client,
        socket=Socket, diff_ref=Ref} = State) ->
    case Client:get(B, K) of
        {ok, RObj} ->
            %% TODO don't have the vclock, so we just blast the key
            case riak_repl_util:repl_helper_send(RObj, Client) of
                cancel ->
                    skipped;
                Objects when is_list(Objects) ->
                    [riak_repl_tcp_server:send(Socket, {diff_obj, O}) || O <- Objects],
                    riak_repl_tcp_server:send(Socket, {diff_obj, RObj})
            end;
        {error, notfound} ->
            ok;
        _ ->
            ok
    end,
    {next_state, diff_keylist, State};
diff_keylist({Ref, diff_paused}, #state{socket=Socket, partition=Partition,
        diff_ref=Ref} = State) ->
    %lager:notice("reqursting diff_ack from client"),
    riak_repl_tcp_server:send(Socket, {diff_ack, Partition}),
    {next_state, diff_keylist, State};
diff_keylist({diff_ack, Partition}, #state{partition=Partition, diff_ref=Ref} = State) ->
    %lager:notice("client has processed all diffs up to this point"),
    State#state.diff_pid ! {Ref, diff_resume},
    {next_state, diff_keylist, State};
diff_keylist({Ref, diff_done}, #state{diff_ref=Ref} = State) ->
    lager:notice("Differences exchanged for partition ~p (done in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    lager:notice("Fullsync for ~p complete (completed in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]),
    riak_repl_tcp_server:send(State#state.socket, diff_done),
    {next_state, wait_for_partition, State}.

%% gen_fsm callbacks

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event,_F,StateName,State) ->
    {reply, ok, StateName, State}.

handle_info(_I, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) -> 
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% internal funtions

log_stop(Command, State) ->
    lager:notice("Fullsync ~s at partition ~p (after ~p secs)",
        [command_verb(Command), State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]).

command_verb(cancel_fullsync) ->
    "cancelled";
command_verb(pause_fullsync) ->
    "paused".
