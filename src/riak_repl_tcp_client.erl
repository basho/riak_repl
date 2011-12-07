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
-export([start_link/1, status/1, status/2, async_connect/3, send/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

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
        pool_pid
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
            lager:notice("repl to site ~p", [Site]),
            {ok, Pid} = poolboy:start_link([{worker_module, riak_repl_put_worker},
                    {worker_args, []},
                    %% TODO the overflow should be configurable
                    {size, 0}, {max_overflow, 100},
                    {checkout_blocks, true}]),
            Listeners = Site#repl_site.addrs,
            State = #state{sitename=SiteName,
                listeners=Listeners,
                pending=Listeners, pool_pid=Pid},
            {ok, do_async_connect(State)}
    end.

handle_call({connected, Socket}, _From, #state{listener={_, IPAddr, Port}} = State) ->
    lager:notice("Connected to replication site ~p at ~p:~p",
        [State#state.sitename, IPAddr, Port]),
    ok = riak_repl_util:configure_socket(Socket),
    gen_tcp:send(Socket, State#state.sitename),
    Props = riak_repl_fsm_common:common_init(Socket),
    NewState = State#state{
        listener = {connected, IPAddr, Port},
        socket=Socket, 
        client=proplists:get_value(client, Props),
        my_pi=proplists:get_value(my_pi, Props),
        partitions=proplists:get_value(partitions, Props)},
    send(Socket, {peerinfo, NewState#state.my_pi,
            [bounded_queue, {fullsync_strategies,
                    app_helper:get_env(riak_repl, fullsync_strategies,
                        [?LEGACY_STRATEGY])}]}),
    inet:setopts(Socket, [{active, once}]),
    recv_peerinfo(NewState);
handle_call({connect_failed, Reason}, _From, State) ->
    lager:debug("Failed to connect to site ~p: ~p", [State#state.sitename,
            Reason]),
    NewState = do_async_connect(State),
    {reply, ok, NewState};
handle_call(status, _From, #state{fullsync_worker=FSW} = State) ->
    Res = gen_fsm:sync_send_all_state_event(FSW, status),
    Desc =
        [
            {site, State#state.sitename},
            {strategy, State#state.fullsync_strategy}
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
    Msg = binary_to_term(Data),
    riak_repl_stats:client_bytes_recv(size(Data)),
    inet:setopts(Socket, [{active, once}]),
    case Msg of
        {diff_obj, RObj} ->
            {noreply, do_repl_put(RObj, State)};
        _ ->
            gen_fsm:send_event(State#state.fullsync_worker, Msg),
            {noreply, State}
    end;
handle_info(try_connect, State) ->
    NewState = do_async_connect(State),
    {noreply, NewState};
handle_info(_Event, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

do_async_connect(#state{pending=[], listeners=Listeners} = State) ->
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
                                        {keepalive, true},
                                        {nodelay, true}], Timeout) of
        {ok, Socket} ->
            ok = gen_tcp:controlling_process(Socket, Parent),
            gen_server:call(Parent, {connected, Socket});
        {error, Reason} ->
            %% Send Reason so it shows in traces even if nothing is done with it
            gen_server:call(Parent, {connect_failed, Reason})
    end.

send(Socket, Data) when is_binary(Data) -> 
    R = gen_tcp:send(Socket, Data),
    riak_repl_stats:client_bytes_sent(size(Data)),
    R;
send(Socket, Data) ->
    send(Socket, term_to_binary(Data)).

do_repl_put(Obj, State=#state{ack_freq = undefined, pool_pid=Pool}) -> % q_ack not supported
    Worker = worker_checkout(Pool),
    ok = riak_repl_put_worker:do_put(Worker, Obj, Pool),
    State;
do_repl_put(Obj, State=#state{count=C, ack_freq=F, pool_pid=Pool}) when (C < (F-1)) ->
    Worker = worker_checkout(Pool),
    ok = riak_repl_put_worker:do_put(Worker, Obj, Pool),
    State#state{count=C+1};
do_repl_put(Obj, State=#state{socket=S, ack_freq=F, pool_pid=Pool}) ->
    Worker = worker_checkout(Pool),
    ok = riak_repl_put_worker:do_put(Worker, Obj, Pool),
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
                Other ->
                    lager:error("Expected peer_info, but got something else: ~p.",
                        [Other]),
                    {stop, normal, State}
            end
    after 5000 ->
            lager:error("Timed out waiting for peer info."),
            {stop, normal, State}
    end.

handle_peerinfo(#state{sitename=SiteName, socket=Socket} = State, TheirPeerInfo, Capability) ->
    MyPeerInfo = State#state.my_pi,
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            ServerStrats = proplists:get_value(fullsync_strategies, Capability,
                [?LEGACY_STRATEGY]),
            ClientStrats = app_helper:get_env(riak_repl, fullsync_strategies,
                [?LEGACY_STRATEGY]),
            Strategy = riak_repl_util:choose_strategy(ServerStrats, ClientStrats),
            StratMod = riak_repl_util:strategy_module(Strategy, client),
            lager:notice("Using fullsync strategy ~p.", [StratMod]),
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
            TheirRing = riak_core_ring:upgrade(TheirPeerInfo#peer_info.ring),
            update_site_ips(riak_repl_ring:get_repl_config(TheirRing), SiteName),
            inet:setopts(Socket, [{active, once}]),
            riak_repl_stats:client_connects(),
            {reply, ok, State1#state{work_dir = WorkDir,
                    fullsync_worker=FullsyncWorker,
                    fullsync_strategy=StratMod}};
        false ->
            lager:error("Replication - invalid peer_info ~p",
                [TheirPeerInfo]),
            {stop, normal, State}
    end.

update_site_ips(TheirReplConfig, SiteName) ->
    {ok, OurRing} = riak_core_ring_manager:get_my_ring(),
    MyListeners = dict:fetch(listeners, riak_repl_ring:get_repl_config(OurRing)),
    MyIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- MyListeners]),
    TheirListeners = dict:fetch(listeners, TheirReplConfig),
    TheirIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- TheirListeners]),
    ToRemove = sets:subtract(MyIPAddrs, TheirIPAddrs),
    ToAdd = sets:subtract(TheirIPAddrs, MyIPAddrs),
    OurRing1 = lists:foldl(fun(E,A) -> riak_repl_ring:del_site_addr(A, SiteName, E) end,
                           OurRing, sets:to_list(ToRemove)),
    OurRing2 = lists:foldl(fun(E,A) -> riak_repl_ring:add_site_addr(A, SiteName, E) end,
                           OurRing1, sets:to_list(ToAdd)),
    MyNewRC = riak_repl_ring:get_repl_config(OurRing2),
    F = fun(InRing, ReplConfig) ->
                {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, MyNewRC),
    ok.

%% workaround for lack of infinitely blocking checkouts in the old version of
%% poolboy we are using
worker_checkout(Pool) ->
    gen_fsm:sync_send_event(Pool, checkout, infinity).

