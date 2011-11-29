
-module(riak_repl_keylist_client).

-behaviour(gen_fsm).

-include("riak_repl.hrl").

%% API
-export([start_link/3]).

%% gen_fsm
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

%% states
-export([wait_for_fullsync/2,
        request_partition/2,
        send_keylist/2,
        wait_ack/2]).

-record(state, {
        sitename,
        socket,
        work_dir,
        partitions = [],
        partition,
        kl_fn,
        kl_fh,
        kl_pid,
        kl_ref,
        our_kl_ready,
        their_kl_ready,
        stage_start,
        partition_start
    }).

start_link(SiteName, Socket, WorkDir) ->
    gen_fsm:start_link(?MODULE, [SiteName, Socket, WorkDir], []).

init([SiteName, Socket, WorkDir]) ->
    {ok, wait_for_fullsync,
        #state{sitename=SiteName,socket=Socket,work_dir=WorkDir}}.

wait_for_fullsync(Command, State)
        when Command == start_fullsync; Command == resume_fullsync ->
    case State#state.partitions of
        [] ->
            case app_helper:get_env(riak_repl, {progress,
                        State#state.sitename}, []) of
                [] ->
                    %% last sync completed or was cancelled
                    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                    Partitions = riak_repl_util:get_partitions(Ring);
                Progress ->
                    lager:notice("Resuming failed fullsync at ~p",
                        [hd(Progress)]),
                    Partitions = Progress
            end;
        _ ->
            Partitions = [State#state.partition | State#state.partitions] % resuming from pause
    end,
    Remaining = length(Partitions),
    lager:notice("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    {next_state, request_partition, State#state{partitions=Partitions}, 0}.

request_partition(Command, #state{kl_pid=Pid, sitename=SiteName} = State)
        when Command == pause_fullsync; Command == cancel_fullsync ->
    case Pid of
        undefined ->
            ok;
        _ ->
            riak_repl_fullsync_helper:stop(Pid)
    end,
    file:delete(State#state.kl_fn),
    NewState = case Command of
        cancel_fullsync ->
            application:unset_env(riak_repl, {progress, SiteName}),
            State#state{partitions=[], partition=undefined};
        _ ->
            State
    end,
    log_stop(Command, State),
    {next_state, wait_for_fullsync, NewState};
request_partition(timeout, #state{partitions=[], sitename=SiteName} = State) ->
    application:unset_env(riak_repl, {progress, SiteName}),
    lager:notice("Fullsync with site ~p completed", [State#state.sitename]),
    riak_repl_tcp_client:send(State#state.socket, fullsync_complete),
    {next_state, wait_for_fullsync, State#state{partition=undefined}};
request_partition(timeout, #state{partitions=[P|T], work_dir=WorkDir, socket=Socket} = State) ->
    lager:notice("Starting fullsync for ~p", [P]),
    application:set_env(riak_repl, {progress, State#state.sitename}, [P|T]),
    riak_repl_tcp_client:send(Socket, {partition, P}),
    KeyListFn = riak_repl_util:keylist_filename(WorkDir, P, ours),
    lager:notice("Building keylist for ~p, ~p remain", [P, length(T)]),
    {ok, KeyListPid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, KeyListRef} = riak_repl_fullsync_helper:make_keylist(KeyListPid,
                                                                 P,
                                                                 KeyListFn),
    {next_state, request_partition, State#state{kl_fn=KeyListFn,
            our_kl_ready=false, their_kl_ready=false,
            partition_start=now(), stage_start=now(),
            kl_pid=KeyListPid, kl_ref=KeyListRef, partition=P, partitions=T}};
request_partition({Ref, keylist_built}, State=#state{kl_ref = Ref}) ->
    lager:notice("Built keylist for ~p, (built in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    case State#state.their_kl_ready of
        true ->
            {next_state, send_keylist, State#state{stage_start=now()}, 0};
        _ ->
            {next_state, request_partition, State#state{our_kl_ready=true,
                    kl_pid=undefined}}
    end;
request_partition({kl_exchange, P}, #state{partition=P} = State) ->
    case State#state.our_kl_ready of
        true ->
            {next_state, send_keylist, State#state{stage_start=now()}, 0};
        _ ->
            {next_state, request_partition, State#state{their_kl_ready=true}}
    end.

send_keylist(Command, #state{kl_fh=FH, sitename=SiteName} = State)
        when Command == cancel_fullsync; Command == pause_fullsync ->
    file:close(FH),
    file:delete(State#state.kl_fn),
    NewState = case Command of
        cancel_fullsync ->
            application:unset_env(riak_repl, {progress, SiteName}),
            State#state{partitions=[], partition=undefined};
        _ ->
            State
    end,
    log_stop(Command, State),
    {next_state, wait_for_fullsync, NewState};
send_keylist(timeout, #state{kl_fh=FH0,socket=Socket} = State) ->
    FH = case FH0 of
        undefined ->
            lager:notice("Sending keylist for ~p", [State#state.partition]),
            {ok, F} = file:open(State#state.kl_fn, [read, binary, raw, read_ahead]),
            F;
        _ ->
            FH0
    end,
    case file:read(FH, ?MERKLE_CHUNKSZ) of
        {ok, Data} ->
            riak_repl_tcp_client:send(Socket, {kl_hunk, Data}),
            {next_state, send_keylist, State#state{kl_fh=FH}, 0};
        eof ->
            file:close(FH),
            riak_repl_tcp_client:send(Socket, kl_eof),
            lager:notice("Sent keylist for ~p (sent in ~p secs)",
                [State#state.partition,
                    riak_repl_util:elapsed_secs(State#state.stage_start)]),
            lager:notice("Exhanging differences for ~p",
                [State#state.partition]),
            {next_state, wait_ack, State#state{kl_fh=undefined,
                    stage_start=now()}}
    end.

wait_ack(Command, #state{sitename=SiteName} = State)
        when Command == cancel_fullsync; Command == pause_fullsync ->
    NewState = case Command of
        cancel_fullsync ->
            application:unset_env(riak_repl, {progress, SiteName}),
            State#state{partitions=[], partition=undefined};
        _ ->
            State
    end,
    log_stop(Command, State),
    {next_state, wait_for_fullsync, NewState};
wait_ack({diff_ack, Partition}, #state{partition=Partition, socket=Socket} =
    State) ->
    %lager:notice("got diff ack ~p", [Partition]),
    riak_repl_tcp_client:send(Socket, {diff_ack, Partition}),
    {next_state, wait_ack, State};
wait_ack(diff_done, State) ->
    lager:notice("Differences exchanged for ~p (done in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.stage_start)]),
    lager:notice("Fullsync for partition ~p complete (done in ~p secs)",
        [State#state.partition,
            riak_repl_util:elapsed_secs(State#state.partition_start)]),
    {next_state, request_partition, State, 0}.


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
