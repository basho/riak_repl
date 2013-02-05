%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_pg_block_provider).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/1, connected/6, connect_failed/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-record(state, {
                transport,
                socket,
                ip,
                other_cluster,
                connection_ref,
                worker,
                client
                }).

start_link(Cluster) ->
    gen_server:start_link(?MODULE, Cluster, []).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, Pid, Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:call(Pid,
        {connected, Socket, Transport, Endpoint, Proto, Props}).

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
            lager:info("proxy_get connection ref ~p", [Ref]),
            {ok, #state{other_cluster = Cluster, connection_ref = Ref}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.

handle_call({connected, Socket, Transport, _Endpoint, _Proto, Props}, _From,
            State=#state{other_cluster=OtherCluster}) ->
    Cluster = proplists:get_value(clustername, Props),
    lager:debug("proxy_get to ~p", [OtherCluster]),
    
    %SocketTag = riak_repl_util:generate_socket_tag("fs_source", Socket),
    %lager:debug("Keeping stats for " ++ SocketTag),
    %riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, source,
    %                                   SocketTag}, Transport),
    
    Transport:setopts(Socket, [{active, once}]),
    {ok, Client} = riak:local_client(),
    {reply, ok, State#state{
            transport=Transport,
            socket=Socket,
            other_cluster=Cluster,
            client=Client}};

%handle_call(legacy_status, _From, State=#state{worker=FSW,
%                                               socket=Socket}) ->
%    Res = case is_pid(FSW) of
%        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
%        false -> []
%    end,
%    SocketStats = riak_core_tcp_mon:format_socket_stats(
%        riak_core_tcp_mon:socket_status(Socket), []),
%    Desc =
%        [
%            {node, node()},
%            {site, State#state.cluster},
%            {strategy, fullsync},
%            {worker, riak_repl_util:safe_pid_to_list(FSW)},
%            {socket, SocketStats}
%        ],
%    {reply, Desc ++ Res, State};
handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_msg({proxy_get, Ref, Bucket, Key, Options},
            State=#state{transport=Transport, socket=Socket}) ->
    lager:debug("Got proxy_get for ~p:~p", [Bucket, Key]),
    C = State#state.client,
    Res = C:get(Bucket, Key, Options),
    Data = term_to_binary({proxy_get_resp, Ref, Res}),
    Transport:send(Socket, Data),
    {noreply, State}.

handle_cast({connect_failed, _Pid, Reason},
            State = #state{other_cluster = Cluster}) ->
    lager:info("proxy_get connection to cluster ~p failed ~p",
        [Cluster, Reason]),
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.other_cluster]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.other_cluster, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for proxy_get ~p closed", [State#state.other_cluster]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for proxy_get ~p closed unexpectedly: ~p",
        [State#state.other_cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
            State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    lager:info("Message = ~p", [Msg]),
    handle_msg(Msg, State);

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


