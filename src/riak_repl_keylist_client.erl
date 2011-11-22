
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
        their_kl_ready
    }).

start_link(SiteName, Socket, WorkDir) ->
    gen_fsm:start_link(?MODULE, [SiteName, Socket, WorkDir], []).

init([SiteName, Socket, WorkDir]) ->
    {ok, wait_for_fullsync,
        #state{sitename=SiteName,socket=Socket,work_dir=WorkDir}}.

wait_for_fullsync(start_fullsync, State) ->
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
            Partitions = State#state.partitions % resuming from pause
    end,
    Remaining = length(Partitions),
    lager:info("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    {next_state, request_partition, State#state{partitions=Partitions}, 0}.

request_partition(timeout, #state{partitions=[], sitename=SiteName} = State) ->
    application:unset_env(riak_repl, {progress, SiteName}),
    lager:info("fullsync with site ~p completed", [State#state.sitename]),
    {next_state, wait_for_fullsync, State};
request_partition(timeout, #state{partitions=[P|T], work_dir=WorkDir, socket=Socket} = State) ->
    application:set_env(riak_repl, {progress, State#state.sitename}, [P|T]),
    riak_repl_tcp_client:send(Socket, {partition, P}),
    KeyListFn = riak_repl_util:keylist_filename(WorkDir, P, ours),
    lager:notice("building keylist for ~p, ~p remain", [P, length(T)]),
    {ok, KeyListPid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, KeyListRef} = riak_repl_fullsync_helper:make_keylist(KeyListPid,
                                                                 P,
                                                                 KeyListFn),
    {next_state, request_partition, State#state{kl_fn=KeyListFn,
            our_kl_ready=false, their_kl_ready=false,
            kl_pid=KeyListPid, kl_ref=KeyListRef, partition=P, partitions=T}};
request_partition({Ref, keylist_built}, State=#state{kl_ref = Ref}) ->
    lager:notice("built keylist for ~p", [State#state.partition]),
    case State#state.their_kl_ready of
        true ->
            lager:notice("time to exchange keylists"),
            {next_state, send_keylist, State, 0};
        _ ->
            {next_state, request_partition, State#state{our_kl_ready=true}}
    end;
request_partition({kl_exchange, P}, #state{partition=P} = State) ->
    case State#state.our_kl_ready of
        true ->
            lager:notice("time to exchange keylists"),
            {next_state, send_keylist, State, 0};
        _ ->
            {next_state, request_partition, State#state{their_kl_ready=true}}
    end.

send_keylist(timeout, #state{kl_fh=FH0,socket=Socket} = State) ->
    FH = case FH0 of
        undefined ->
            {ok, F} = file:open(State#state.kl_fn, [read, binary, raw, read_ahead]),
            F;
        _ ->
            FH0
    end,
    case file:read(FH, ?MERKLE_CHUNKSZ) of
        {ok, Data} ->
            riak_repl_tcp_server:send(Socket, {kl_hunk, Data}),
            {next_state, send_keylist, State#state{kl_fh=FH}, 0};
        eof ->
            file:close(FH),
            riak_repl_tcp_server:send(Socket, kl_eof),
            {next_state, wait_ack, State#state{kl_fh=undefined}}
    end.

wait_ack(diff_done, State) ->
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

diff_hunk([], Acc) ->
    lager:debug("differing keys: ~p", [Acc]),
    Acc;
diff_hunk([{D, Hash}|Tail], Acc) ->
    {B, K} = riak_repl_util:binunpack_bkey(D),
    {ok, C} = riak:local_client(),
    case C:get(B, K) of
        {ok, Obj} ->
            case hash_obj(Obj) of
                Hash ->
                    %% same on other side
                    diff_hunk(Tail, Acc);
                _ ->
                    diff_hunk(Tail, [{{B, K}, riak_object:vclock(Obj)}|Acc])
            end;
        {error, notfound} ->
            diff_hunk(Tail, [{{B, K}, vclock:fresh()}|Acc]);
        _ ->
            diff_hunk(Tail, [{{B, K}, vclock:fresh()}|Acc])
    end.

hash_obj(RObj) ->
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    erlang:phash2(term_to_binary(UpdObj)).

