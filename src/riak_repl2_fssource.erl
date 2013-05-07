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
        ver,
        strategy
    }).

start_link(Partition, IP) ->
    gen_server:start_link(?MODULE, [Partition, IP], []).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid, Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
        {connected, Socket, Transport, Endpoint, Proto, Props}).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync).

stop_fullsync(Pid) ->
    gen_server:call(Pid, stop_fullsync).

fullsync_complete(Pid) ->
    %% cast to avoid deadlock in terminate
    gen_server:cast(Pid, fullsync_complete).

%% get the cluster name
cluster_name(Pid) ->
    gen_server:call(Pid, cluster_name).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% gen server

init([Partition, IP]) ->
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {active, false}],

    DefaultStrategy = keylist,

    %% Determine what kind of fullsync worker strategy we want to start with,
    %% which could change if we talk to the sink and it can't speak AAE
    Strategy =
        case app_helper:get_env(riak_repl, fullsync_strategy, DefaultStrategy) of
            aae -> aae;
            keylist -> keylist;
            UnSupportedStrategy ->
                lager:warning("App config for riak_repl/fullsync_strategy ~p is unsupported. Using ~p",
                              [UnSupportedStrategy, DefaultStrategy]),
                DefaultStrategy
        end,

    %% use 1,1 proto for new binary object
    %% use 2,0 for AAE fullsync + binary objects
    {Major,Minor} = case Strategy of
                        keylist -> {1,1};
                        aae -> {2,0}
                    end,
    ClientSpec = {{fullsync,[{Major,Minor}]}, {TcpOptions, ?MODULE, self()}},

    %% TODO: check for bad remote name
    lager:info("connecting to remote ~p", [IP]),
    case riak_core_connection_mgr:connect({identity, IP}, ClientSpec) of
        {ok, Ref} ->
            lager:info("connection ref ~p", [Ref]),
            {ok, #state{ip = IP, connection_ref = Ref, partition=Partition}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, Proto, Props},
            _From, State=#state{ip=IP, partition=Partition}) ->
    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    lager:info("Negotiated ~p with ver ~p", [Proto, Ver]),
    Cluster = proplists:get_value(clustername, Props),
    lager:info("fullsync connection to ~p for ~p",[IP, Partition]),

    SocketTag = riak_repl_util:generate_socket_tag("fs_source", Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, source,
                                       SocketTag}, Transport),

    Transport:setopts(Socket, [{active, once}]),

    %% Strategy still depends on what the sink is capable of.
    {_Proto,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,
    Strategy =
        case CommonMajor of
            0 -> keylist; %% would be a bug to hit this case
            1 -> keylist; %% default uses keylist
            _ -> aae      %% AAE technology preview, first introduced in 1.4
        end,

    case Strategy of
        keylist ->
            %% Keylist server strategy
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            {ok, Client} = riak:local_client(),
            {ok, FullsyncWorker} = riak_repl_keylist_server:start_link(Cluster,
                                                                       Transport, Socket, WorkDir, Client),
            riak_repl_keylist_server:start_fullsync(FullsyncWorker, [Partition]),
            {reply, ok, State#state{transport=Transport, socket=Socket, cluster=Cluster,
                                    fullsync_worker=FullsyncWorker, work_dir=WorkDir, ver=Ver,
                                    strategy=keylist}};
        aae ->
            %% AAE strategy
            {ok, Client} = riak:local_client(),
            {ok, FullsyncWorker} = riak_repl_aae_source:start_link(Cluster, Client,
                                                                   Transport, Socket,
                                                                   Partition,
                                                                   self()),
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_source:start_exchange(FullsyncWorker),
            {reply, ok,
             State#state{transport=Transport, socket=Socket, cluster=Cluster,
                         fullsync_worker=FullsyncWorker, work_dir="/dev/null",
                         ver=Ver, strategy=aae}}
    end;
            
handle_call(start_fullsync, _From, State=#state{fullsync_worker=FSW,
                                                strategy=Strategy}) ->
    case Strategy of
        keylist ->
            riak_repl_keylist_server:start_fullsync(FSW);
        aae ->
            ok
    end,
    {reply, ok, State};
handle_call(stop_fullsync, _From, State=#state{fullsync_worker=FSW,
                                               strategy=Strategy}) ->
    case Strategy of
        keylist ->
            riak_repl_keylist_server:cancel_fullsync(FSW);
        aae ->
            riak_repl_aae_source:cancel_fullsync(FSW)
    end,
    {reply, ok, State};
handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket}) ->
    Res = case is_process_alive(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    SocketStats = riak_core_tcp_mon:format_socket_stats(
        riak_core_tcp_mon:socket_status(Socket), []),
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, fullsync},
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
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    %% check if process alive only if it's defined
    case is_pid(FSW) andalso is_process_alive(FSW) of
        false ->
            ok;
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop)
    end,
    %% clean up work dir
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


