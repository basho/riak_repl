-module(riak_repl2_fssource).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/2, connected/6, connect_failed/3, start_fullsync/1,
         stop_fullsync/1, cluster_name/1, legacy_status/2, fullsync_complete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        ip,
        partition,
        cluster,
        connection_ref,
        fullsync_worker,
        work_dir,
        strategy
    }).

start_link(Partition, IP) ->
    gen_server:start_link(?MODULE, [Partition, IP], []).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid, Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
        {connected, Socket, Transport, Endpoint, Proto, Props}, ?LONG_TIMEOUT).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync, ?LONG_TIMEOUT).

stop_fullsync(Pid) ->
    gen_server:call(Pid, stop_fullsync, ?LONG_TIMEOUT).

fullsync_complete(Pid) ->
    %% cast to avoid deadlock in terminate
    gen_server:cast(Pid, fullsync_complete).

%% get the cluster name
cluster_name(Pid) ->
    gen_server:call(Pid, cluster_name, ?LONG_TIMEOUT).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% gen server

init([Partition, IP]) ->
    %% Possibly try to obtain the per-vnode lock before connecting.
    %% If we return error, we expect the coordinator to start us again later.
    case riak_repl_util:maybe_get_vnode_lock(Partition) of
        ok ->
            %% got the lock, or ignored it.
            case connect(IP, riak_repl_util:get_local_strategy(), Partition) of
                {error, Reason} ->
                    {stop, Reason};
                Result ->
                    Result
            end;
        {error, Reason} ->
            %% the vnode is probably busy. Try again later.
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, Proto, Props},
            _From, State=#state{ip=IP, partition=Partition}) ->
    Cluster = proplists:get_value(clustername, Props),
    lager:debug("fullsync connection to ~p for ~p",[IP, Partition]),

    SocketTag = riak_repl_util:generate_socket_tag("fs_source", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, source,
                                       SocketTag}, Transport),

    %% Strategy still depends on what the sink is capable of.
    {_,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,

    Strategy = riak_repl_util:decide_common_strategy(CommonMajor, Socket, Transport),
    lager:debug("Common strategy: ~p with cluster: ~p", [Strategy, Cluster]),

    {ok, Client} = riak:local_client(),

    %% TODO: This should be more generic. We should eliminate the strategy
    %% specific socket logic and make the start_fullsync functions the same.
    case Strategy of
        keylist ->
            %% Keylist server strategy
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            %% We maintain ownership of the socket. We will consume TCP messages in handle_info/2
            Transport:setopts(Socket, [{active, once}]),
            {ok, FullsyncWorker} = riak_repl_keylist_server:start_link(Cluster,
                                                                       Transport, Socket, WorkDir, Client),
            riak_repl_keylist_server:start_fullsync(FullsyncWorker, [Partition]),
            {reply, ok, State#state{transport=Transport, socket=Socket, cluster=Cluster,
                                    fullsync_worker=FullsyncWorker, work_dir=WorkDir,
                                    strategy=keylist}};
        aae ->
            %% AAE strategy
            {ok, FullsyncWorker} = riak_repl_aae_source:start_link(Cluster, Client,
                                                                   Transport, Socket,
                                                                   Partition,
                                                                   self()),
            %% Give control of socket to AAE worker. It will consume all TCP messages.
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_source:start_exchange(FullsyncWorker),
            {reply, ok,
             State#state{transport=Transport, socket=Socket, cluster=Cluster,
                         fullsync_worker=FullsyncWorker, work_dir=undefined,
                         strategy=aae}}
    end;
            
handle_call(start_fullsync, _From, State=#state{fullsync_worker=FSW,
                                                strategy=keylist}) ->
    riak_repl_keylist_server:start_fullsync(FSW),
    {reply, ok, State};
handle_call(stop_fullsync, _From, State=#state{fullsync_worker=FSW,
                                               strategy=Strategy}) ->
    Mod = riak_repl_util:strategy_module(Strategy, ?MODULE),
    Mod:cancel_fullsync(FSW),
    {reply, ok, State};
handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket,
                                               strategy=Strategy}) ->
    Res = case is_pid(FSW) andalso is_process_alive(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    SocketStats = riak_core_tcp_mon:format_socket_stats(
        riak_core_tcp_mon:socket_status(Socket), []),
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, Strategy},
            {fullsync_worker, riak_repl_util:safe_pid_to_list(FSW)},
            {socket, SocketStats}
        ],
    {reply, Desc ++ Res, State};
handle_call(cluster_name, _From, State) ->
    Name = case State#state.cluster of
        undefined ->
            {connecting, State#state.ip};
        ClusterName ->
            ClusterName
    end,
    {reply, Name, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(not_responsible, State=#state{partition=Partition}) ->
    lager:info("Fullsync of partition ~p stopped because AAE trees can't be compared.", [Partition]),
    lager:info("Probable cause is one or more differing bucket n_val properties between source and sink clusters."),
    lager:info("Restarting fullsync connection for partition ~p with keylist strategy.", [Partition]),
    case connect(State#state.ip, keylist, Partition) of
        {ok, State2} ->
            {noreply, State2};
        {error, Reason} ->
            {stop, Reason, State}
    end;
handle_cast(fullsync_complete, State=#state{partition=Partition}) ->
    %% sent from AAE fullsync worker
    lager:info("Fullsync for partition ~p complete.", [Partition]),
    {stop, normal, State};
handle_cast({connect_failed, _Pid, Reason},
     State = #state{cluster = Cluster}) ->
     lager:info("fullsync replication connection to cluster ~p failed ~p",
        [Cluster, Reason]),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Closed, Socket}, State=#state{socket=Socket})
        when Closed == tcp_closed; Closed == ssl_closed ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({Error, _Socket, Reason}, State)
        when Error == tcp_error; Error == ssl_error ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    case Msg == fullsync_complete of
        true ->
            %% sent from the keylist_client when it's done.
            %% stop on fullsync completion, which will call
            %% our terminate function and stop the keylist_server.
            {stop, normal, State};
        _ ->
            gen_fsm:send_event(State#state.fullsync_worker, Msg),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    lager:warning("Unexpected handle_info call ~p. Ignoring.", [Msg]),
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    %% check if process alive only if it's defined
    case is_pid(FSW) andalso is_process_alive(FSW) of
        false ->
            ok;
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop)
    end,
    case WorkDir of
        undefined -> ok;
        _ ->
            %% clean up work dir
            Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
            os:cmd(Cmd)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Start a connection to the remote sink node at IP, using the given fullsync strategy,
%% for the given partition. The protocol version will be determined from the strategy.
connect(IP, Strategy, Partition) ->
    lager:debug("connecting to remote ~p", [IP]),
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {active, false}],

    %% use 1,1 proto for new binary object
    %% use 2,0 for AAE fullsync + binary objects
    ProtocolVersion = case Strategy of
                          keylist -> {1,1};
                          aae -> {2,0}
                      end,

    ClientSpec = {{fullsync,[ProtocolVersion]}, {TcpOptions, ?MODULE, self()}},
    case riak_core_connection_mgr:connect({identity, IP}, ClientSpec) of
        {ok, Ref} ->
            lager:debug("connection ref ~p", [Ref]),
            {ok, #state{strategy = Strategy, ip = IP,
                        connection_ref = Ref, partition=Partition}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {error, Reason}
    end.
