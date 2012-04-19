%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_tcp_client).

%% @doc This module is responsible for the client-side TCP communication
%% during replication. A seperate instance of this module is started for every
%% replication connection that is established.
%%
%% Overall the architecture of this is very similar to the repl_tcp_server,
%% the main difference being that this module also manages a pool of put
%% workers to avoid running the VM out of processes during very heavy
%% replication load.

-include("riak_repl.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, status/1, status/2, async_connect/3, send/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        sitename,
        listeners,
        listener,
        socket,
        transport,
        pending,
        client,
        my_pi,
        ack_freq,
        count,
        work_dir,
        fullsync_worker,
        fullsync_strategy,
        pool_pid,
        keepalive_time
    }).



start_link(SiteName) ->
    gen_server:start_link(?MODULE, [SiteName], []).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

init([SiteName]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_site(Ring, SiteName) of 
        undefined ->
            %% Do not start
            {stop, {site_not_in_ring, SiteName}};
        Site ->
            lager:info("Starting replication site ~p to ~p",
                [Site#repl_site.name, [Host++":"++integer_to_list(Port) ||
                        {Host, Port} <- Site#repl_site.addrs]]),
            Listeners = Site#repl_site.addrs,
            State = #state{sitename=SiteName,
                listeners=Listeners,
                pending=Listeners},
            {ok, do_async_connect(State)}
    end.

handle_call(status, _From, #state{fullsync_worker=FSW} = State) ->
    Res = case is_pid(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    Desc =
        [
            {site, State#state.sitename},
            {strategy, State#state.fullsync_strategy},
            {fullsync_worker, State#state.fullsync_worker}
        ] ++
        [
            {put_pool_size,
                length(gen_fsm:sync_send_all_state_event(State#state.pool_pid,
                    get_all_workers, infinity))} || is_pid(State#state.pool_pid)
        ] ++
        case State#state.listener of
            undefined ->
                [{waiting_to_retry, State#state.listeners}];
            {connected, IPAddr, Port} ->
                [{connected, IPAddr, Port}];
            {Pid, IPAddr, Port} ->
                [{connecting, Pid, IPAddr, Port}]
        end,
    {reply, {status, Desc ++ Res}, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info({connected, Transport, Socket}, #state{listener={_, IPAddr, Port}} = State) ->
    lager:info("Connected to replication site ~p at ~p:~p",
        [State#state.sitename, IPAddr, Port]),
    ok = riak_repl_util:configure_socket(Transport, Socket),
    Transport:send(Socket, State#state.sitename),
    Props = riak_repl_fsm_common:common_init(Transport, Socket),
    NewState = State#state{
        listener = {connected, IPAddr, Port},
        socket=Socket,
        transport=Transport,
        client=proplists:get_value(client, Props),
        my_pi=proplists:get_value(my_pi, Props)},
    case riak_repl_util:maybe_use_ssl() of
        false ->
            send(Transport, Socket, {peerinfo, NewState#state.my_pi,
                    [bounded_queue, keepalive, {fullsync_strategies,
                            app_helper:get_env(riak_repl, fullsync_strategies,
                                [?LEGACY_STRATEGY])}]});
        _ ->
            %% Send a fake peerinfo that will cause a connection failure if
            %% we don't renegotiate SSL. This avoids leaking the ring and
            %% accidentally connecting to an insecure site
            send(Transport, Socket, {peerinfo,
                    riak_repl_util:make_fake_peer_info(), [ssl_required]})
    end,
    recv_peerinfo(NewState);
handle_info({connect_failed, Reason}, State) ->
    lager:debug("Failed to connect to site ~p: ~p", [State#state.sitename,
            Reason]),
    NewState = do_async_connect(State),
    {noreply, NewState};
handle_info({tcp_closed, Socket}, #state{socket = Socket} = State) ->
    lager:info("Connection to site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({tcp_closed, _Socket}, State) ->
    %% Ignore old sockets - e.g. after a redirect
    {noreply, State};
handle_info({ssl_closed, Socket}, #state{socket = Socket} = State) ->
    lager:info("Connection to site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({tcp_error, Socket, Reason}, #state{socket = Socket} = State) ->
    lager:error("Connection to site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({ssl_error, Socket, Reason}, #state{socket = Socket} = State) ->
    lager:error("Connection to site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{transport=Transport,socket=Socket}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    riak_repl_stats:client_bytes_recv(size(Data)),
    NewState = case Msg of
        {diff_obj, RObj} ->
            %% realtime diff object, or a fullsync diff object from legacy
            %% repl. Because you can't tell the difference this can screw up
            %% the acking, but there's not really a way to fix it, other than
            %% not using legacy.
            do_repl_put(RObj, State);
        {fs_diff_obj, RObj} ->
            %% fullsync diff objects
            Pool = State#state.pool_pid,
            Worker = poolboy:checkout(Pool, true, infinity),
            ok = riak_repl_fullsync_worker:do_put(Worker, RObj, Pool),
            State;
        keepalive ->
            send(Transport, Socket, keepalive_ack),
            State;
        keepalive_ack ->
            %% noop
            State;
        _ ->
            gen_fsm:send_event(State#state.fullsync_worker, Msg),
            State
    end,
    case NewState#state.keepalive_time of
        Time when is_integer(Time) ->
            {noreply, NewState, Time};
        _ ->
            {noreply, NewState}
    end;
handle_info(try_connect, State) ->
    NewState = do_async_connect(State),
    {noreply, NewState};
handle_info(timeout, State) ->
    case State#state.keepalive_time of
        Time when is_integer(Time) ->
            %% keepalive timeout fired
            send(State#state.transport, State#state.socket, keepalive),
            {noreply, State, Time};
        _ ->
            {noreply, State}
    end;
handle_info(_Event, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool_pid=Pool, fullsync_worker=FSW, work_dir=WorkDir}) ->
    case is_pid(Pool) of
        true ->
            poolboy:stop(Pool);
        false ->
            ok
    end,
    case is_pid(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        false ->
            ok
    end,
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

do_async_connect(#state{pending=[], sitename=SiteName} = State) ->
    %% re-read the listener config in case it has had IPs added
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Listeners = case riak_repl_ring:get_site(Ring, SiteName) of
        undefined ->
            %% this should never happen, if it does the site is probably
            %% slated for death anyway...
            State#state.listeners;
        Site ->
            Site#repl_site.addrs
    end,

    %% Start the retry timer
    RetryTimeout = app_helper:get_env(riak_repl, client_retry_timeout, 30000),
    lager:debug("Failed to connect to any listener for site ~p, retrying in ~p"
        " milliseconds", [State#state.sitename, RetryTimeout]),
    erlang:send_after(RetryTimeout, self(), try_connect),
    State#state{pending=Listeners};
do_async_connect(#state{pending=[{IPAddr, Port}|T]} = State) ->
    Pid = proc_lib:spawn_link(?MODULE, async_connect,
        [self(), IPAddr, Port]),
    State#state{pending=T, listener={Pid, IPAddr, Port}}.

%% Function spawned to do async connect
async_connect(Parent, IPAddr, Port) ->
    Timeout = app_helper:get_env(riak_repl, client_connect_timeout, 15000),
    Transport = ranch_tcp,
    case gen_tcp:connect(IPAddr, Port, [binary, 
                                        {packet, 4},
                                        {active, false},
                                        {keepalive, true},
                                        {nodelay, true}], Timeout) of
        {ok, Socket} ->
            ok = Transport:controlling_process(Socket, Parent),
            Parent ! {connected, Transport, Socket};
        {error, Reason} ->
            %% Send Reason so it shows in traces even if nothing is done with it
            Parent ! {connect_failed, Reason}
    end.

send(Transport, Socket, Data) when is_binary(Data) -> 
    R = Transport:send(Socket, Data),
    riak_repl_stats:client_bytes_sent(size(Data)),
    R;
send(Transport, Socket, Data) ->
    send(Transport, Socket, term_to_binary(Data)).

do_repl_put(Obj, State=#state{ack_freq = undefined, pool_pid=Pool}) -> % q_ack not supported
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    State;
do_repl_put(Obj, State=#state{count=C, ack_freq=F, pool_pid=Pool}) when (C < (F-1)) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    State#state{count=C+1};
do_repl_put(Obj, State=#state{transport=T,socket=S, ack_freq=F, pool_pid=Pool}) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    send(T, S, {q_ack, F}),
    State#state{count=0}.

recv_peerinfo(#state{transport=T,socket=Socket} = State) ->
    Proto = T:name(),
    case T:recv(Socket, 0, 60000) of
        {ok, Data} ->
            Msg = binary_to_term(Data),
            NeedSSL = riak_repl_util:maybe_use_ssl(),
            case Msg of
                {peerinfo, _, [ssl_required]} when Proto == tcp, NeedSSL /= false ->
                    case riak_repl_util:upgrade_client_to_ssl(Socket) of
                        {ok, SSLSocket} ->
                            lager:info("Upgraded replication connection to SSL"),
                            Transport = ranch_ssl,
                            %% re-send peer info
                            send(Transport, SSLSocket, {peerinfo, State#state.my_pi,
                                    [bounded_queue, {fullsync_strategies,
                                            app_helper:get_env(riak_repl, fullsync_strategies,
                                                [?LEGACY_STRATEGY])}]}),
                            recv_peerinfo(State#state{socket=SSLSocket,
                                    transport=Transport});
                        {error, Reason} ->
                            lager:error("Unable to comply with request to "
                                "upgrade connection with site ~p to SSL: ~p",
                                [State#state.sitename, Reason]),
                            riak_repl_client_sup:stop_site(State#state.sitename),
                            {stop, normal, State}
                    end;
                {peerinfo, _, [ssl_required]} ->
                    %% other side wants SSL, but we aren't configured for it
                    lager:error("Site ~p requires SSL to connect",
                        [State#state.sitename]),
                    riak_repl_client_sup:stop_site(State#state.sitename),
                    {stop, normal, State};
                {peerinfo, TheirPeerInfo} when Proto == ssl; NeedSSL == false ->
                    Capability = riak_repl_util:capability_from_vsn(TheirPeerInfo),
                    handle_peerinfo(State, TheirPeerInfo, Capability);
                {peerinfo, TheirPeerInfo, Capability}  when  Proto == ssl; NeedSSL == false->
                    handle_peerinfo(State, TheirPeerInfo, Capability);
                PeerInfo when element(1, PeerInfo) == peerinfo ->
                    %% SSL was required, but not provided by the other side
                    lager:error("Server for site ~p does not support SSL, refusing "
                        "to connect", [State#state.sitename]),
                    riak_repl_client_sup:stop_site(State#state.sitename),
                    {stop, normal, State};
                {redirect, IPAddr, Port} ->
                    case lists:member({IPAddr, Port}, State#state.listeners) of
                        false ->
                            lager:info("Redirected IP ~p not in listeners ~p",
                                [{IPAddr, Port}, State#state.listeners]);
                        _ ->
                            ok
                    end,
                    riak_repl_stats:client_redirect(),
                    catch gen_tcp:close(Socket),
                    self() ! try_connect,
                    {noreply, State#state{pending=[{IPAddr, Port} |
                                State#state.pending]}};
                Other ->
                    lager:error("Expected peer_info from ~p, but got something else: ~p.",
                        [State#state.sitename, Other]),
                    {stop, normal, State}
            end;
        _ ->
            %% the server will wait for 60 seconds for gen_leader to stabilize
            lager:error("Timed out waiting for peer info from ~p.",
                [State#state.sitename]),
            {stop, normal, State}
    end.

handle_peerinfo(#state{sitename=SiteName, transport=Transport, socket=Socket} = State, TheirPeerInfo, Capability) ->
    MyPeerInfo = State#state.my_pi,
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            ServerStrats = proplists:get_value(fullsync_strategies, Capability,
                [?LEGACY_STRATEGY]),
            ClientStrats = app_helper:get_env(riak_repl, fullsync_strategies,
                [?LEGACY_STRATEGY]),
            Strategy = riak_repl_util:choose_strategy(ServerStrats, ClientStrats),
            StratMod = riak_repl_util:strategy_module(Strategy, client),
            lager:info("Using fullsync strategy ~p with site ~p.", [StratMod,
                    State#state.sitename]),
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, SiteName),
            {ok, FullsyncWorker} = StratMod:start_link(SiteName, Transport,
                Socket, WorkDir),
            %% Set up for bounded queue if remote server supports it
            case proplists:get_bool(bounded_queue, Capability) of
                true ->
                    AckFreq = app_helper:get_env(riak_repl,client_ack_frequency,
                        ?REPL_DEFAULT_ACK_FREQUENCY),
                    State1 = State#state{count=0, 
                        ack_freq=AckFreq};
                false ->
                    State1 = State
            end,
            case proplists:get_bool(keepalive, Capability) of
                true ->
                    KeepaliveTime = ?KEEPALIVE_TIME;
                _ ->
                    KeepaliveTime = undefined
            end,
            TheirRing = riak_core_ring:upgrade(TheirPeerInfo#peer_info.ring),
            update_site_ips(riak_repl_ring:get_repl_config(TheirRing), SiteName),
            Transport:setopts(Socket, [{active, once}]),
            riak_repl_stats:client_connects(),
            MinPool = app_helper:get_env(riak_repl, min_put_workers, 5),
            MaxPool = app_helper:get_env(riak_repl, max_put_workers, 100),
            {ok, Pid} = poolboy:start_link([{worker_module, riak_repl_fullsync_worker},
                    {worker_args, []},
                    {size, MinPool}, {max_overflow, MaxPool}]),
            {noreply, State1#state{work_dir = WorkDir,
                    fullsync_worker=FullsyncWorker,
                    fullsync_strategy=StratMod,
                    keepalive_time=KeepaliveTime,
                    pool_pid=Pid}};
        false ->
            lager:error("Invalid peer info for site ~p, "
                "ring sizes do not match.", [SiteName]),
            riak_repl_client_sup:stop_site(SiteName),
            {stop, normal, State}
    end.

%% TODO figure out exactly what the point of this code is.
%% I *think* the idea is to prevent a screwy repl setup from connecting to
%% itself?
update_site_ips(TheirReplConfig, SiteName) ->
    {ok, OurRing} = riak_core_ring_manager:get_my_ring(),

    MyListeners = dict:fetch(listeners, riak_repl_ring:get_repl_config(OurRing)),
    MyIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- MyListeners]),

    TheirListeners = dict:fetch(listeners, TheirReplConfig),
    TheirIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- TheirListeners]),

    ToRemove = sets:subtract(MyIPAddrs, TheirIPAddrs),
    ToAdd = sets:subtract(TheirIPAddrs, MyIPAddrs),

    case sets:size(ToRemove) + sets:size(ToAdd) of
        0 ->
            %% nothing needs to be changed, so don't transform the ring for no reason
            ok;
        _ ->
            OurRing1 = lists:foldl(fun(E,A) -> riak_repl_ring:del_site_addr(A, SiteName, E) end,
                OurRing, sets:to_list(ToRemove)),
            OurRing2 = lists:foldl(fun(E,A) -> riak_repl_ring:add_site_addr(A, SiteName, E) end,
                OurRing1, sets:to_list(ToAdd)),

            MyNewRC = riak_repl_ring:get_repl_config(OurRing2),
            F = fun(InRing, ReplConfig) ->
                    {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
            end,
            {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, MyNewRC),
            ok
    end.
