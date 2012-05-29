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
-export([start_link/1, status/1, status/2, async_connect/3, send/2,
        handle_peerinfo/3, make_state/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
        sitename,
        listeners,
        listener,
        socket,
        pending,
        client,
        my_pi,
        partitions,
        ack_freq,
        count,
        work_dir,
        fullsync_worker,
        fullsync_strategy,
        pool_pid,
        keepalive_time
    }).

make_state(SiteName, Socket, MyPI, WorkDir, Client) ->
    {ok, {IP, Port}} = inet:sockname(Socket),
    #state{sitename=SiteName, socket=Socket, my_pi=MyPI, work_dir=WorkDir,
        client=Client, listeners=[], pending=[], listener={connected, IP,
            Port}}.

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

%% these fullsync control messages are for 'inverse' mode only, and only work
%% with the keylist strategy.
handle_call(start_fullsync, _From, #state{fullsync_worker=FSW} = State) ->
    gen_fsm:send_event(FSW, start_fullsync),
    {reply, ok, State};
handle_call(cancel_fullsync, _From, #state{fullsync_worker=FSW} = State) ->
    gen_fsm:send_event(FSW, cancel_fullsync),
    {reply, ok, State};
handle_call(pause_fullsync, _From, #state{fullsync_worker=FSW} = State) ->
    gen_fsm:send_event(FSW, pause_fullsync),
    {reply, ok, State};
handle_call(resume_fullsync, _From, #state{fullsync_worker=FSW} = State) ->
    gen_fsm:send_event(FSW, resume_fullsync),
    {reply, ok, State};

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

handle_info({connected, Socket}, #state{listener={_, IPAddr, Port}} = State) ->
    lager:info("Connected to replication site ~p at ~p:~p",
        [State#state.sitename, IPAddr, Port]),
    ok = riak_repl_util:configure_socket(Socket),
    gen_tcp:send(Socket, State#state.sitename),
    Props = riak_repl_fsm_common:common_init(Socket),
    {ok, {TheirIP, _}} = inet:peername(Socket),
    NewState = State#state{
        listener = {connected, IPAddr, Port},
        socket=Socket,
        client=proplists:get_value(client, Props),
        my_pi=proplists:get_value(my_pi, Props),
        partitions=proplists:get_value(partitions, Props)},
    send(Socket, {peerinfo, NewState#state.my_pi,
                  [bounded_queue, keepalive,
                   {fullsync_strategies,
                    app_helper:get_env(riak_repl, fullsync_strategies,
                                       [?LEGACY_STRATEGY])},
                   {connected_ip, TheirIP}
            ]}),
    inet:setopts(Socket, [{active, once}]),
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
handle_info({tcp_error, Socket, Reason}, #state{socket = Socket} = State) ->
    lager:error("Connection to site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({tcp, Socket, Data}, State=#state{socket=Socket}) ->
    inet:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    riak_repl_stats:client_bytes_recv(size(Data)),
    Reply = case Msg of
        {diff_obj, RObj} ->
            %% realtime diff object, or a fullsync diff object from legacy
            %% repl. Because you can't tell the difference this can screw up
            %% the acking, but there's not really a way to fix it, other than
            %% not using legacy.
            {noreply, do_repl_put(RObj, State)};
        {fs_diff_obj, RObj} ->
            %% fullsync diff objects
            Pool = State#state.pool_pid,
            Worker = poolboy:checkout(Pool, true, infinity),
            ok = riak_repl_fullsync_worker:do_put(Worker, RObj, Pool),
            {noreply, State};
        keepalive ->
            send(Socket, keepalive_ack),
            {noreply, State};
        keepalive_ack ->
            %% noop
            {noreply, State};
        {peerinfo, TheirPI, Capability} ->
            handle_peerinfo(State, TheirPI, Capability);
        _ ->
            gen_fsm:send_event(State#state.fullsync_worker, Msg),
            {noreply, State}
    end,
    case State#state.keepalive_time of
        Time when is_integer(Time) ->
            case Reply of 
                {noreply, NewState} ->
                    {noreply, NewState, Time};
                _ ->
                    Reply
            end;
        _ ->
            Reply
    end;
handle_info(try_connect, State) ->
    NewState = do_async_connect(State),
    {noreply, NewState};
handle_info(timeout, State) ->
    case State#state.keepalive_time of
        Time when is_integer(Time) ->
            %% keepalive timeout fired
            send(State#state.socket, keepalive),
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
    case gen_tcp:connect(IPAddr, Port, [binary, 
                                        {packet, 4},
                                        {active, false},
                                        {keepalive, true},
                                        {nodelay, true}], Timeout) of
        {ok, Socket} ->
            ok = gen_tcp:controlling_process(Socket, Parent),
            Parent ! {connected, Socket};
        {error, Reason} ->
            %% Send Reason so it shows in traces even if nothing is done with it
            Parent ! {connect_failed, Reason}
    end.

send(Socket, Data) when is_binary(Data) -> 
    R = gen_tcp:send(Socket, Data),
    riak_repl_stats:client_bytes_sent(size(Data)),
    R;
send(Socket, Data) ->
    send(Socket, term_to_binary(Data)).

do_repl_put(Obj, State=#state{ack_freq = undefined, pool_pid=Pool}) -> % q_ack not supported
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    State;
do_repl_put(Obj, State=#state{count=C, ack_freq=F, pool_pid=Pool}) when (C < (F-1)) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    State#state{count=C+1};
do_repl_put(Obj, State=#state{socket=S, ack_freq=F, pool_pid=Pool}) ->
    Worker = poolboy:checkout(Pool, true, infinity),
    ok = riak_repl_fullsync_worker:do_put(Worker, Obj, Pool),
    send(S, {q_ack, F}),
    State#state{count=0}.

recv_peerinfo(#state{socket=Socket} = State) ->
    receive
        {tcp, Socket, Data} ->
            Msg = binary_to_term(Data),
            case Msg of
                {peerinfo, TheirPeerInfo} ->
                    Capability = riak_repl_util:capability_from_vsn(TheirPeerInfo),
                    handle_peerinfo(State, TheirPeerInfo, Capability);
                {peerinfo, TheirPeerInfo, Capability} ->
                    handle_peerinfo(State, TheirPeerInfo, Capability);
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
            end
    after 60000 ->
            %% the server will wait for 60 seconds for gen_leader to stabilize
            lager:error("Timed out waiting for peer info from ~p.",
                [State#state.sitename]),
            {stop, normal, State}
    end.

handle_peerinfo(#state{sitename=SiteName, socket=Socket, listener={_, ConnIP, _Port}} = State,
                       TheirPeerInfo, Capability) ->
    MyPeerInfo = State#state.my_pi,
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            case app_helper:get_env(riak_repl, inverse_connection) == true
                andalso get(inverted) /= true of
                true ->
                    case riak_repl_leader:add_receiver_pid(self()) of
                        ok ->
                            lager:notice("added as receiver pid"),
                            self() ! {tcp, Socket, term_to_binary({peerinfo,
                                        TheirPeerInfo, Capability})},
                            put(inverted, true),
                            gen_server:enter_loop(riak_repl_tcp_server,
                                [],
                                riak_repl_tcp_server:make_state(SiteName,
                                    Socket, State#state.my_pi,
                                    State#state.work_dir,
                                    State#state.client)),
                            {stop, normal, State};
                        {error, _Reason} ->
                            %% election has not completed.. apparently
                            {stop, normal, State}
                    end;
                _ ->

                    ServerStrats = proplists:get_value(fullsync_strategies, Capability,
                        [?LEGACY_STRATEGY]),
                    ClientStrats = app_helper:get_env(riak_repl, fullsync_strategies,
                        [?LEGACY_STRATEGY]),
                    Strategy = riak_repl_util:choose_strategy(ServerStrats, ClientStrats),
                    StratMod = riak_repl_util:strategy_module(Strategy, client),
                    lager:info("Using fullsync strategy ~p with site ~p.", [StratMod,
                            State#state.sitename]),
                    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Socket, SiteName),
                    {ok, FullsyncWorker} = StratMod:start_link(SiteName, Socket,
                        WorkDir),
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
                    update_site_ips(riak_repl_ring:get_repl_config(TheirRing), SiteName, ConnIP),
                    inet:setopts(Socket, [{active, once}]),
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
                            pool_pid=Pid}}
            end;
        false ->
            lager:error("Invalid peer info for site ~p, "
                "ring sizes do not match.", [SiteName]),
            riak_repl_client_sup:stop_site(SiteName),
            {stop, normal, State}
    end.

%% @doc get public IP addrs from NAT, otherwise local IP listeners
%% ConnectedIP represents the IP addr of our connection to RemoteSite,
%% so if the ConnectedIP address is in the list of NAT'd listeners,
%% then use the NAT addresses. Otherwise, use local addresses. If
%% there are no NAT addresses, then use the local ones.
get_public_listener_addrs(ReplConfig, ConnectedIP) ->
    NatListeners = dict:fetch(natlisteners, ReplConfig),
    NatListenAddrs = [R#nat_listener.nat_addr || R <- NatListeners],
    UseNats = lists:keymember(ConnectedIP, 1, NatListenAddrs),
    case UseNats of
        false ->
            %% no NAT listeners, use the "private" listeners as public
            Listeners = dict:fetch(listeners, ReplConfig),
            [R#repl_listener.listen_addr || R <- Listeners];
        true ->
            NatListenAddrs
    end.

%% @doc get all Nat, Nat-listener, and listener addrs
get_all_listener_addrs(ReplConfig) ->
    Listeners = dict:fetch(listeners, ReplConfig),
    ListenAddrs = [R#repl_listener.listen_addr || R <- Listeners],
    NatListeners = dict:fetch(natlisteners, ReplConfig),
    NatAddrs = [R#nat_listener.nat_addr || R <- NatListeners],
    NatListenAddrs = [R#nat_listener.listen_addr || R <- NatListeners],
    ListenAddrs++NatAddrs++NatListenAddrs.

-spec(rewrite_config_site_ips_pure/4 :: (repl_config(),ring(),repl_sitename(),ip_addr_str())
                                        -> none|ring()).
%% @doc Update replication configuration with corrected set of IP addrs for a RemoteSite.
%%
%% Given a "remote" server's replication configuration and our own ring configuration,
%% update the list of IP addresses for the remote server. This function will ensure
%% that we don't add our own lisenter IP addresses to that list, even if we are setup
%% as a bi-directional connection. If no changes are required, a simple token is returned
%% so that the caller can avoid ring changes when not required. No side effects.
%% NAT-aware (for Network Address Translations where the cluster has different public
%% and private IP addresses across firewalls or routers). Also removes stale IP addrs.
%%
rewrite_config_site_ips_pure(TheirReplConfig, OurRing, RemoteSiteName, ConnectedIP) ->
    %% IP addresses that this client listens on (in case client is also a server for
    %% some other client site, which could be the RemoteSiteName's address(es) if
    %% the repl configs are such that we're bi-directional).
    OurReplConfig = riak_repl_ring:get_repl_config(OurRing),
    MyListenAddrs = get_all_listener_addrs(OurReplConfig),
    lager:debug("MyListenAddrs = ~p~n", [MyListenAddrs]),

    %% IP addresses that the remote server listens on.
    TheirListenAddrs = get_public_listener_addrs(TheirReplConfig, ConnectedIP),
    lager:debug("TheirListenAddrs = ~p~n", [TheirListenAddrs]),

    %% IP address that this client wants to connect to for RemoteSiteName
    RemoteSiteAddrs = case riak_repl_ring:get_site(OurRing, RemoteSiteName) of
                          undefined -> [];
                          #repl_site{addrs=Addrs} -> Addrs
                      end,
    lager:debug("RemoteSiteAddrs = ~p~n", [RemoteSiteAddrs]),

    %% We want to remove both stale IPs (that are no longer valid) and
    %% our own (in case they leaked in, to avoid connecting to ourself).
    %% StateAddrs = (RemoteSiteAddrs - TheirListenAddrs), ignoring Ports
    StaleAddrs = [ A || {_IP, _Port}=A <- RemoteSiteAddrs,
                        not lists:member(A, TheirListenAddrs)],
    %% MyLeakedInAddrs = (RemoteSiteAddrs ^ MyListenAddrs), ignoring Ports
    MyLeakedInAddrs = [ A || {IP, _Port}=A <- RemoteSiteAddrs,
                             lists:keymember(IP, 1, MyListenAddrs)],
    ToRemove = StaleAddrs ++ MyLeakedInAddrs,
    lager:debug("ToRemove = ~p~n", [ToRemove]),

    %% We want to add new addresses that the remote server listens on,
    %% but never ones that we are listening on!
    %% ToAdd = (TheirListenAddrs - RemoteSiteAddrs), ignoring Ports
    ToAdd = [ A || {IP, _Port}=A <- TheirListenAddrs,
                   (not lists:member(A, RemoteSiteAddrs)), %% ensure "new"
                   (not lists:keymember(IP, 1, MyListenAddrs))],  %% not ourself
    lager:debug("ToAdd = ~p~n", [ToAdd]),

    %% Do the add first in order to ensure we don't put ourself in the Addr list;
    %% ToRemove is the one that may contain our leaked address and will remove it
    %% from the RemoteSiteAddr list.

    case {ToAdd,ToRemove} of
        {[],[]} -> none; %% no changes to configuration are needed
        _ ->
            RingAdds = lists:foldl(
                         fun(A,R) ->
                                 lager:info("Adding ~p for site ~p~n",[A,RemoteSiteName]),
                                 riak_repl_ring:add_site_addr(R, RemoteSiteName, A)
                         end,
                         OurRing,
                         ToAdd),
            OurNewRing = lists:foldl(
                           fun(A,R) ->
                                   lager:info("Removing ~p from site ~p~n",[A,RemoteSiteName]),
                                   riak_repl_ring:del_site_addr(R, RemoteSiteName, A)
                           end,
                           RingAdds,
                           ToRemove),
            OurNewRing
    end.

-spec(update_site_ips/3 :: (repl_config(),repl_sitename(),ip_addr_str()) -> ok).
%% @doc update the ring configuration to include new remote IP site addresses for SiteName
%%
%% This also ensures that we remove our own IP addrs from the list, just in case they
%% leaked into the remote site's list of listen IP addresses. Not likely, but game over
%% if it happens.
update_site_ips(TheirReplConfig, SiteName, ConnectedIP) ->
    {ok, OurRing} = riak_core_ring_manager:get_my_ring(),
    NeededConfigChanges = rewrite_config_site_ips_pure(TheirReplConfig, OurRing,
                                                       SiteName, ConnectedIP),
    case NeededConfigChanges of
	none ->
            %% don't transform the ring for no reason
	    ok;
	MyNewRing ->
	    %% apply changes to the ring now
	    F = fun(InRing, ReplConfig) ->
                    {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
            end,
            MyNewRC = riak_repl_ring:get_repl_config(MyNewRing),
            {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, MyNewRC),
            ok
    end.

%% unit tests

-ifdef(TEST).
%% uses some test functions from riak_repl_ring.erl

mock_ring() ->
    riak_core_ring:fresh(16, 'mytest@mytest').

ensure_config_test() ->
    Ring = riak_repl_ring:ensure_config(mock_ring()),
    ?assertNot(undefined =:= riak_core_ring:get_meta(riak_repl_ring, Ring)),
    %% Add the remote site "test" to our ring configuration so that it's known.
    RemoteSiteName = "test",
    riak_repl_ring:add_site(Ring, #repl_site{name=RemoteSiteName}).

get_public_listener_addrs_test() ->
    Ring0 = riak_repl_ring:ensure_config_test(),
    NatListener1 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.1", 9000},
                                 nat_addr={"10.11.12.12", 9012}
                                },
    NatListener2 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.2", 9000},
                                 nat_addr={"10.11.12.13", 9013}
                                },
    NatListener3 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.3", 9000},
                                 nat_addr={"10.11.12.14", 9014}
                                },
    Listener = #repl_listener{nodename='test@test',
                              listen_addr={"198.162.1.2", 9010}
                             },
    Ring1 = riak_repl_ring:add_nat_listener(Ring0, NatListener1),
    Ring2 = riak_repl_ring:add_nat_listener(Ring1, NatListener2),
    Ring3 = riak_repl_ring:add_nat_listener(Ring2, NatListener3),
    Ring4 = riak_repl_ring:add_listener(Ring3, Listener),
    ReplConfig1 = riak_repl_ring:get_repl_config(Ring4),
    ConnectedIP = "10.11.12.12", %% existing connection is NAT'd
    [PubAddr3,PubAddr2,PubAddr1] = get_public_listener_addrs(ReplConfig1, ConnectedIP),
    ?assertEqual(PubAddr1, {"10.11.12.12", 9012}),
    ?assertEqual(PubAddr2, {"10.11.12.13", 9013}),
    ?assertEqual(PubAddr3, {"10.11.12.14", 9014}),
    Ring4.

get_public_listener_addrs_not_nat_connection_test() ->
    Ring0 = riak_repl_ring:ensure_config_test(),
    NatListener1 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.1", 9000},
                                 nat_addr={"10.11.12.12", 9012}
                                },
    NatListener2 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.2", 9000},
                                 nat_addr={"10.11.12.13", 9013}
                                },
    NatListener3 = #nat_listener{nodename='test@test', 
                                 listen_addr={"127.0.0.3", 9000},
                                 nat_addr={"10.11.12.14", 9014}
                                },
    Listener = #repl_listener{nodename='test@test',
                              listen_addr={"198.162.1.2", 9010}
                             },
    Ring1 = riak_repl_ring:add_nat_listener(Ring0, NatListener1),
    Ring2 = riak_repl_ring:add_nat_listener(Ring1, NatListener2),
    Ring3 = riak_repl_ring:add_nat_listener(Ring2, NatListener3),
    Ring4 = riak_repl_ring:add_listener(Ring3, Listener),
    ReplConfig1 = riak_repl_ring:get_repl_config(Ring4),
    ConnectedIP = "198.162.1.3", %% existing connection is not NAT'd.
    %% we should only listen on the local listener since we're on the same network
    [LocalAddr] = get_public_listener_addrs(ReplConfig1, ConnectedIP),
    ?assertEqual(LocalAddr, {"198.162.1.2", 9010}),
    Ring4.

get_all_listen_addrs_test() ->
    Ring = get_public_listener_addrs_test(),
    ReplConfig = riak_repl_ring:get_repl_config(Ring),
    [Addr4,PubAddr3,PubAddr2,PubAddr1,Addr3,Addr2,Addr1] = get_all_listener_addrs(ReplConfig),
    ?assertEqual(PubAddr1, {"10.11.12.12", 9012}),
    ?assertEqual(PubAddr2, {"10.11.12.13", 9013}),
    ?assertEqual(PubAddr3, {"10.11.12.14", 9014}),
    ?assertEqual(Addr4, {"198.162.1.2", 9010}),
    ?assertEqual(Addr3, {"127.0.0.3", 9000}),
    ?assertEqual(Addr2, {"127.0.0.2", 9000}),
    ?assertEqual(Addr1, {"127.0.0.1", 9000}).

rewrite_config_site_ips_pure_test() ->
    %% Their Ring has two NAT'd public listeners:
    %%  10.11.12.13 and 10.11.12.14
    TheirRing = get_public_listener_addrs_test(),

    %% My ring MUST already have the remote site "test" added.
    RemoteSiteName = "test",
    MyRing = ensure_config_test(),
    ?assertNotEqual(undefined, riak_repl_ring:get_site(MyRing, RemoteSiteName)),

    %% Our (client) Ring has one private listener that overlaps
    %% with the remote cluster (198.162.1.2) and one public NAT'd
    %% listener (10.11.12.14) that also has a collision on the
    %% local binding for that NAT (127.0.0.1), which shouldn't
    %% be added to the remote site.

    MyListener = #repl_listener{nodename='mytest@mytest',
                                listen_addr={"198.162.1.2", 9018}
                               },
    MyNatListener = #nat_listener{nodename='mytest@mytest',
                                  listen_addr={"127.0.0.1", 9015},
                                  nat_addr={"10.11.12.14", 9016}
                                 },
    MyRing1 = riak_repl_ring:add_listener(MyRing, MyListener),
    MyRing2 = riak_repl_ring:add_nat_listener(MyRing1, MyNatListener),
    %% make sure we added my listeners correctly
    MyReplConfig = riak_repl_ring:get_repl_config(MyRing2),
    ConnectedIP = "10.11.12.14",  %% we are connected via a NAT'd IP
    [MyPubAddr] = get_public_listener_addrs(MyReplConfig, ConnectedIP),
    ?assertEqual(MyPubAddr, {"10.11.12.14", 9016}),

    [MyListenAddr,MyNatAddr,MyNatBinding] = get_all_listener_addrs(MyReplConfig),
    ?assertEqual(MyNatAddr, {"10.11.12.14", 9016}),
    ?assertEqual(MyNatBinding, {"127.0.0.1", 9015}),
    ?assertEqual(MyListenAddr, {"198.162.1.2", 9018}),

    %% Put an already known "active" addr in our Site list and make sure it stays there.
    ActiveAddr = {"10.11.12.12", 9012}, %% connected on existing NAT'd IP
    MyRingActive = riak_repl_ring:add_site_addr(MyRing2, RemoteSiteName, ActiveAddr),
    case riak_repl_ring:get_site(MyRingActive, RemoteSiteName) of
        undefined -> ?assertEqual("", "empty site list for MyRingActive!");
        #repl_site{addrs=[Addr]} -> ?assertEqual(Addr, {"10.11.12.12", 9012})
    end,

    %% put a stale IP addr in my list of RemoteSite addrs and make sure it gets removed.
    %% My Ring now has a combination of Stale and Active IP addresses.
    StaleAddr = {"10.11.12.1", 9011},
    MyRingCombo = riak_repl_ring:add_site_addr(MyRingActive, RemoteSiteName, StaleAddr),

    %% Ok, finally, apply the remote replication update to our ring...
    TheirReplConfig = riak_repl_ring:get_repl_config(TheirRing),
    MyNewRing = rewrite_config_site_ips_pure(TheirReplConfig, MyRingCombo,
                                             RemoteSiteName, ConnectedIP),
    %% changes need to be made
    ?assertNotEqual(none, MyNewRing),
    
    %% we should have added one of their listeners to our Sites to connect to (.13)
    %% but not the other, which collides with us (.14). And we had one that was already
    %% active (.12).
    Site = riak_repl_ring:get_site(MyNewRing, RemoteSiteName),
    case Site of
        undefined ->
            ?assertEqual(undefined, false);
        #repl_site{addrs=Addrs} ->
            [NewAddr,ActiveAddr] = Addrs,
            ?assertEqual(ActiveAddr, {"10.11.12.12", 9012}),
            ?assertEqual(NewAddr, {"10.11.12.13", 9013})
    end.


rewrite_config_site_ips_pure_same_public_ip_test() ->
    %% Their Ring has thee NAT'd public listeners:
    %% 10.11.12.1:9090 and 10.11.12.1:9091 and 10.11.12.1:9092
    %% This can happen at a site with one public IP but multiple servers
    %% behind it
    Ring0 = riak_repl_ring:ensure_config_test(),
    NatListener1 = #nat_listener{nodename='test1@test',
                                 listen_addr={"127.0.0.1", 9000},
                                 nat_addr={"10.11.12.1", 9090}
                                },
    NatListener2 = #nat_listener{nodename='test2@test',
                                 listen_addr={"127.0.0.2", 9000},
                                 nat_addr={"10.11.12.1", 9091}
                                },
    NatListener3 = #nat_listener{nodename='test3@test',
                                 listen_addr={"127.0.0.3", 9000},
                                 nat_addr={"10.11.12.1", 9092}
                                },
    Ring1 = riak_repl_ring:add_nat_listener(Ring0, NatListener1),
    Ring2 = riak_repl_ring:add_nat_listener(Ring1, NatListener2),
    Ring3 = riak_repl_ring:add_nat_listener(Ring2, NatListener3),

    TheirRing = Ring3,

    Ring = riak_repl_ring:ensure_config(mock_ring()),
    %% Add the remote site "test" to our ring configuration so that it's known.
    %% The site has only one of the 3 remote ips in it
    RemoteSiteName = "test",
    MyRing = riak_repl_ring:add_site(Ring, #repl_site{name=RemoteSiteName,
            addrs=[{"10.11.12.1", 9092}]}),
    %% we have no local listeners

    %% Add a stale IP addr in our list of RemoteSite addrs and make sure it gets
    %% removed without removing the new ones.
    StaleAddr = {"10.11.12.1", 9089},
    MyRingCombo = riak_repl_ring:add_site_addr(MyRing, RemoteSiteName, StaleAddr),

    %% Ok, finally, apply the remote replication update to our ring...
    ConnectedIP = "10.11.12.1", %% we are connected via a NAT'd address
    TheirReplConfig = riak_repl_ring:get_repl_config(TheirRing),
    MyNewRing = rewrite_config_site_ips_pure(TheirReplConfig, MyRingCombo,
                                             RemoteSiteName, ConnectedIP),
    %% changes need to be made
    ?assertNotEqual(none, MyNewRing),

    %% We should have added three new IP/Port entries and removed the stale entry.
    Site = riak_repl_ring:get_site(MyNewRing, RemoteSiteName),
    case Site of
        undefined ->
            ?assertEqual(undefined, false);
        #repl_site{addrs=Addrs} ->
            [A,B,C] = Addrs,
            ?assertEqual(A, {"10.11.12.1", 9090}),
            ?assertEqual(B, {"10.11.12.1", 9091}),
            ?assertEqual(C, {"10.11.12.1", 9092})
    end.

-endif.
