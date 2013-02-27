%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_pg_block_requester).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/4, register_service/0, start_service/5, legacy_status/2,
         proxy_get/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, provider_cluster_id/1]).

-record(state, {
          transport,
          socket,
          cluster,
          proxy_gets = [],
          remote_cluster_id=nocluster,
          leader_mref=undefined
         }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).


%% Register with service manager
register_service() ->
    lager:debug("Registering proxy_get requester service"),
    ProtoPrefs = {proxy_get,[{1,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, false},
                  {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    lager:info("Proxy get service enabled"),
    {ok, Pid} = riak_repl2_pg_block_requester_sup:start_child(Socket, Transport,
        Proto, Props),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.


proxy_get(Pid, Bucket, Key, Options) ->
    gen_server:call(Pid, {proxy_get, Bucket, Key, Options}).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

provider_cluster_id(Pid) ->
    gen_server:call(Pid, provider_cluster_id).

%% gen server

init([Socket, Transport, _Proto, Props]) ->
    lager:info("Starting Proxy Get Block Requester"),

    %SocketTag = riak_repl_util:generate_socket_tag("pg_requester", Socket),
    %% TODO
    %lager:debug("Keeping stats for " ++ SocketTag),
    %riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, sink,
    %                                   SocketTag}, Transport),

    Cluster = proplists:get_value(clustername, Props),
    State0 = #state{cluster=Cluster, transport=Transport, socket=Socket},
    State = register_with_leader(State0),
    {ok, State}.

handle_call({proxy_get, Bucket, Key, Options}, From,
            State=#state{socket=Socket,transport=Transport}) ->
    lager:info("PROXY GETTING"),
    Ref = make_ref(),
    Data = term_to_binary({proxy_get, Ref, Bucket, Key, Options}),
    Transport:send(Socket, Data),
    {noreply, State#state{proxy_gets=[{Ref, From}|State#state.proxy_gets]}};
handle_call(provider_cluster_id, _From,
            State=#state{remote_cluster_id=ClusterID}) ->
    {reply, ClusterID, State};
handle_call(legacy_status, _From, State=#state{socket=_Socket}) ->
    Desc = [ {proxy_get, no_stats}],
    {reply, Desc, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};

handle_info({'DOWN', _MRef, process, _Pid, Reason}, State)
  when Reason == normal; Reason == shutdown ->
    {noreply, State};
handle_info({'DOWN', _MRef, process, _Pid, _Reason}, State0) ->
    lager:info("Re-registering pg_proxy service 2"),
    State = register_with_leader(State0),
    {noreply, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp;
        Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
%    riak_repl_stats:client_bytes_recv(size(Data)),
    Reply = 
        case Msg of
            stay_awake ->
                {noreply, State};
            {proxy_get_resp, Ref, Resp} ->
                case lists:keytake(Ref, 1, State#state.proxy_gets) of
                    false ->
                        lager:info("got unexpected proxy_get_resp message"),
                        {noreply, State};
                    {value, {Ref, From}, ProxyGets} ->
                        %% send the response to the patiently waiting client
                        gen_server:reply(From, Resp),
                        {noreply, State#state{proxy_gets=ProxyGets}}
                end;
            {get_cluster_id_resp, ClusterID} ->
                RemoteClusterID = list_to_binary(io_lib:format("~p",[ClusterID])),
                {noreply, State#state{remote_cluster_id=RemoteClusterID}};
            _ ->
                {noreply, State}
        end,
    Reply;
handle_info(init_ack, State=#state{socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),

    Data = term_to_binary(get_cluster_id),
    Transport:send(Socket, Data),

    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

pg_proxy_name(Remote) ->
    list_to_atom("pg_proxy_" ++ Remote).

make_pg_proxy(Remote) -> 
    Name = pg_proxy_name(Remote),
    {Name, {riak_repl2_pg_proxy, start_link, [Name]},
        transient, 5000, worker, [riak_repl2_pg_proxy, pg_proxy]}.

register_with_leader(#state{leader_mref=MRef, cluster=Cluster}=State) ->    
    case MRef of
        undefined -> ok;
        M -> 
            erlang:demonitor(M)
    end,
    Leader = riak_core_cluster_mgr:get_leader(),        
    ProxyForCluster = pg_proxy_name(Cluster),
    Child = [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({riak_repl2_pg_proxy_sup, Leader}), 
                      is_pid(Pid),
                      Remote == ProxyForCluster],
    case Child of
        [{_Remote, _Pid}] -> 
            lager:debug("Not starting a new proxy process, one already exists"),
            ok;
        _ -> 
            lager:debug("Starting a new proxy process"),
            supervisor:start_child({riak_repl2_pg_proxy_sup, Leader}, 
                                   make_pg_proxy(Cluster))
    end,
    gen_server:call({ProxyForCluster, Leader}, {register, Cluster, node()}),    
    Monitor = erlang:monitor(process, {ProxyForCluster, Leader}),
    State#state{leader_mref=Monitor}.
