%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
%%
%% block_provider services proxy_get requests originating from a
%% block_requester (which runs on the *SINK* cluster) to a *SOURCE* cluster.
%%
-module(riak_repl2_pg_block_provider).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/1, connected/6, connect_failed/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3, status/1, status/2]).

%% send a message every KEEPALIVE milliseconds to make sure the service is running on the sink
-define(KEEPALIVE, 1000).

-record(state,
        {
          transport,
          socket,
          ip,
          other_cluster,
          connection_ref,
          worker,
          client,
          keepalive_timer,
          proxy_gets_provided = 0
        }).

start_link(Cluster) ->
    gen_server:start_link(?MODULE, Cluster, []).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid, Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
                    %% Pid is running local when connection manager calls us here.
                    {connected, Socket, Transport, Endpoint, Proto, Props}, infinity).

connect_failed(_ClientProto, Reason, Pid) ->
    gen_server:cast(Pid, {connect_failed, self(), Reason}).


init(Cluster) ->
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {active, false}],
    ClientSpec = {{proxy_get,[{1,0}]}, {TcpOptions, ?MODULE, self()}},
    lager:info("proxy_get connecting to remote ~p", [Cluster]),
    case riak_core_connection_mgr:connect({proxy_get, Cluster}, ClientSpec) of
        {ok, Ref} ->
            lager:debug("proxy_get connection ref ~p", [Ref]),
            {ok, #state{other_cluster = Cluster, connection_ref = Ref}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, _Proto, Props}, _From,
            State=#state{other_cluster=OtherCluster}) ->
    Cluster = proplists:get_value(clustername, Props),
    lager:debug("proxy_get connected to ~p", [OtherCluster]),

    SocketTag = riak_repl_util:generate_socket_tag("pg_provider", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_PROXYGET_APP, source,
                                       SocketTag}, Transport),
    TRef = keepalive_timer(),
    Transport:setopts(Socket, [{active, once}]),
    {ok, Client} = riak:local_client(),
    {reply, ok, State#state{
                  transport=Transport,
                  socket=Socket,
                  other_cluster=Cluster,
                  client=Client,
                  keepalive_timer=TRef
                 }};
handle_call(status, _From, State=#state{socket=Socket,
                                        proxy_gets_provided=PGCount}) ->
    SocketStats = riak_core_tcp_mon:socket_status(Socket),
    FormattedSS =  {socket,
                    riak_core_tcp_mon:format_socket_stats(SocketStats,[])},

    Status = [ {provider_count, PGCount},
                FormattedSS ],
    {reply, Status, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.


handle_cast({connect_failed, _Pid, Reason},
            State = #state{other_cluster = Cluster}) ->
    lager:warning("proxy_get connection to cluster ~p failed ~p",
        [Cluster, Reason]),
    {stop, restart_it, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(keepalive, State=#state{socket=Socket, transport=Transport}) ->
    Data = term_to_binary(stay_awake),
    Transport:send(Socket, Data),
    {noreply, State};
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.other_cluster]),
    {stop, socket_closed, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.other_cluster, Reason]),
    {stop, socket_closed, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.other_cluster]),
    {stop, socket_closed, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.other_cluster, Reason]),
    {stop, socket_closed, State};
handle_info({Proto, Socket, Data},
            State0=#state{socket=Socket,transport=Transport, keepalive_timer=TRef})
        when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    timer:cancel(TRef),    
    Msg = binary_to_term(Data),
    %% restart the timer after each message has been processed
    State = State0#state{keepalive_timer=keepalive_timer()},
    handle_msg(Msg, State);
handle_info(_Msg, State) ->
    {noreply, State}.

handle_msg(get_cluster_info, State=#state{transport=Transport, socket=Socket}) ->
    ThisClusterName = riak_core_connection:symbolic_clustername(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ClusterID = riak_core_ring:cluster_name(Ring),
    lager:debug("Cluster ID=~p, Cluster Name = ~p",[ClusterID, ThisClusterName]),
    Data = term_to_binary({get_cluster_info_resp, ClusterID, ThisClusterName}),
    Transport:send(Socket, Data),
    {noreply, State};
handle_msg({proxy_get, Ref, Bucket, Key, Options},
            State=#state{transport=Transport, socket=Socket,
                         proxy_gets_provided=PGCount}) ->
    lager:debug("Got proxy_get for ~p:~p", [Bucket, Key]),
    C = State#state.client,
    Res = C:get(Bucket, Key, Options),
    Data = term_to_binary({proxy_get_resp, Ref, Res}),
    Transport:send(Socket, Data),
    {noreply, State#state{proxy_gets_provided=PGCount+1}}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

keepalive_timer() ->
    timer:send_interval(?KEEPALIVE, keepalive).

