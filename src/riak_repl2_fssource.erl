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
    DefaultStrategy = ?DEFAULT_FULLSYNC_STRATEGY,

    %% Determine what kind of fullsync worker strategy we want to start with,
    %% which could change if we talk to the sink and it can't speak AAE. If
    %% AAE is not enabled in KV, then we can't use aae strategy.
    OurCaps = decide_our_caps(DefaultStrategy),
    SupportedStrategy = proplists:get_value(strategy, OurCaps, DefaultStrategy),

    connect(IP, SupportedStrategy, Partition).

handle_call({connected, Socket, Transport, _Endpoint, Proto, Props},
            _From, State=#state{ip=IP, partition=Partition, strategy=DefaultStrategy}) ->
    Cluster = proplists:get_value(clustername, Props),
    lager:info("fullsync connection to ~p for ~p",[IP, Partition]),

    SocketTag = riak_repl_util:generate_socket_tag("fs_source", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, source,
                                       SocketTag}, Transport),

    %% Strategy still depends on what the sink is capable of.
    {_Proto,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,

    OurCaps = decide_our_caps(DefaultStrategy),
    TheirCaps = maybe_exchange_caps(CommonMajor, OurCaps, Socket, Transport),
    lager:info("Got caps: ~p", [TheirCaps]),
    Strategy = decide_common_strategy(OurCaps, TheirCaps),
    lager:info("Common strategy: ~p", [Strategy]),

    case Strategy of
        keylist ->
            %% Keylist server strategy
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            {ok, Client} = riak:local_client(),
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
            {ok, Client} = riak:local_client(),
            {ok, FullsyncWorker} = riak_repl_aae_source:start_link(Cluster, Client,
                                                                   Transport, Socket,
                                                                   Partition,
                                                                   self()),
            %% We want a 'DOWN' message when the aae worker stops itself for not_responsible
            erlang:monitor(process, FullsyncWorker),
            %% Give control of socket to AAE worker. It will consume all TCP messages.
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_source:start_exchange(FullsyncWorker),
            {reply, ok,
             State#state{transport=Transport, socket=Socket, cluster=Cluster,
                         fullsync_worker=FullsyncWorker, work_dir=undefined,
                         strategy=aae}}
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

handle_info({'DOWN', Ref, process, _Pid, not_responsible}, State=#state{partition=Partition}) ->
    lager:info("Fullsync of partition ~p stopped because AAE trees can't be compared.", [Partition]),
    lager:info("Probable cause is one or more differing bucket n_val properties between source and sink clusters."),
    lager:info("Restarting fullsync connection for partition ~p with keylist strategy.", [Partition]),
    Strategy = keylist,
    case connect(State#state.ip, Strategy, Partition) of
        {ok, State2} -> {noreply, State2};
        Error -> Error
    end;
handle_info({'DOWN', Ref, process, _Pid, Reason}, State) when Reason == normal orelse Reason == shutdown ->
    {stop, normal, State};
handle_info({'DOWN', Ref, process, _Pid, Reason}, State=#state{partition=Partition}) ->
    lager:info("Received: ~p, fullsync source stopping; will rety partition ~p later.",
               [Reason, Partition]),
    {stop, {error, Reason}, State};
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
    lager:info("ignored handle_info ~p", [Msg]),
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


%% Based on the agreed common protocol level and the supported
%% mode of AAE, decide what strategy we are capable of offering.
decide_our_caps(DefaultStrategy) ->
    SupportedStrategy =
        case {riak_kv_entropy_manager:enabled(),
              app_helper:get_env(riak_repl, fullsync_strategy, DefaultStrategy)} of
            {false,_} -> keylist;
            {true,aae} -> aae;
            {true,keylist} -> keylist;
            {true,UnSupportedStrategy} ->
                lager:warning("App config for riak_repl/fullsync_strategy ~p is unsupported. Using ~p",
                              [UnSupportedStrategy, DefaultStrategy]),
                DefaultStrategy
        end,
    [{strategy, SupportedStrategy}].

%% decide what strategy to use, given our own capabilties and those
%% of the remote source.
decide_common_strategy(_OurCaps, []) -> keylist;
decide_common_strategy(OurCaps, TheirCaps) ->
    OurStrategy = proplists:get_value(strategy, OurCaps, keylist),
    TheirStrategy = proplists:get_value(strategy, TheirCaps, keylist),
    case {OurStrategy,TheirStrategy} of
        {aae,aae} -> aae;
        {_,_}     -> keylist
    end.

%% Depending on the protocol version number, send our capabilities
%% as a list of properties, in binary.
maybe_exchange_caps(1, _Caps, _Socket, _Transport) ->
    [];
maybe_exchange_caps(_, Caps, Socket, Transport) ->
    TheirCaps =
        case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
            {ok, Data} ->
                binary_to_term(Data);
            {Error, Socket} ->
                throw(Error);
            {Error, Socket, Reason} ->
                throw({Error, Reason})
        end,
    Transport:send(Socket, term_to_binary(Caps)),
    TheirCaps.

%% Start a connection to the remote sink node at IP, using the given fullsync strategy,
%% for the given partition. The protocol version will be determined from the strategy.
connect(IP, Strategy, Partition) ->
    lager:info("connecting to remote ~p", [IP]),
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
            lager:info("connection ref ~p", [Ref]),
            {ok, #state{strategy = Strategy, ip = IP,
                        connection_ref = Ref, partition=Partition}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.
