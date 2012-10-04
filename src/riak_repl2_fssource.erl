-module(riak_repl2_fssource).

-behaviour(gen_server).
%% API
-export([start_link/1, connected/5, connect_failed/3, start_fullsync/1,
         stop_fullsync/1, legacy_status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        cluster,
        connection_ref,
        fullsync_worker,
        work_dir
    }).

start_link(Cluster) ->
    gen_server:start_link(?MODULE, [Cluster], []).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
        {connected, Socket, Transport, Endpoint, Proto}).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync).

stop_fullsync(Pid) ->
    gen_server:call(Pid, stop_fullsync).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% gen server

init([Cluster]) ->
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {active, false}],
    ClientSpec = {{fullsync,[{1,0}]}, {TcpOptions, ?MODULE, self()}},

    %% TODO: check for bad remote name
    lager:info("connecting to remote ~p", [Cluster]),
    case riak_core_connection_mgr:connect({rt_repl, Cluster}, ClientSpec) of
        {ok, Ref} ->
            lager:info("connection ref ~p", [Ref]),
            {ok, #state{cluster = Cluster, connection_ref = Ref}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, _Proto}, _From,
        State=#state{cluster=Cluster}) ->
    lager:info("fullsync connection to ~p",[Cluster]),
    Transport:setopts(Socket, [{active, once}]),
    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
    {ok, Client} = riak:local_client(),
    %% strategy is hardcoded
    {ok, FullsyncWorker} = riak_repl_keylist_server:start_link(Cluster,
        Transport, Socket, WorkDir, Client),
    riak_repl_keylist_server:start_fullsync(FullsyncWorker),
    {reply, ok, State#state{transport=Transport, socket=Socket,
            fullsync_worker=FullsyncWorker, work_dir=WorkDir}};
handle_call(start_fullsync, _From, State=#state{fullsync_worker=FSW}) ->
    riak_repl_keylist_server:start_fullsync(FSW),
    {reply, ok, State};
handle_call(stop_fullsync, _From, State=#state{fullsync_worker=FSW}) ->
    riak_repl_keylist_server:cancel_fullsync(FSW),
    {reply, ok, State};
handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW}) ->
    Res = case is_pid(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, fullsync},
            {fullsync_worker, FSW}
        ],
    {reply, {status, Desc ++ Res}, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({connect_failed, _Pid, Reason},
     State = #state{cluster = Cluster}) ->
     lager:info("fullsync replication connection to cluster ~p failed ~p",
        [Cluster, Reason]),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    gen_fsm:send_event(State#state.fullsync_worker, Msg),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    case is_pid(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    %% clean up work dir
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


