%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_conn).

%% @doc Realtime replication source connection module
%%
%% High level responsibility...
%%

-behaviour(gen_server).
-include("riak_repl.hrl").

%% API
-export([start_link/1,
         stop/1,
         status/1, status/2,
         legacy_status/1, legacy_status/2]).

%% connection manager callbacks
-export([connected/6,
         connect_failed/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {remote,    % remote name
                address,   % {IP, Port}
                connection_ref, % reference handed out by connection manager
                transport, % transport module 
                socket,    % socket to use with transport 
                proto,     % protocol version negotiated
                helper_pid,% riak_repl2_rtsource_helper pid
                cont = <<>>}). % continuation from previous TCP buffer

%% API - start trying to send realtime repl to remote site
start_link(Remote) ->
    gen_server:start_link(?MODULE, [Remote], []).

stop(Pid) ->
    gen_server:call(Pid, stop).
    
status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% legacy status -- look like a riak_repl_tcp_server
legacy_status(Pid) ->
    legacy_status(Pid, infinity).

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% connection manager callbacks
connected(Socket, Transport, Endpoint, Proto, RtSourcePid, Props) ->
    _RemoteClusterName = proplists:get_value(clustername, Props),
    Transport:controlling_process(Socket, RtSourcePid),
    Transport:setopts(Socket, [{active, true}]),
    gen_server:call(RtSourcePid,
                    {connected, Socket, Transport, Endpoint, Proto}).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

%% gen_server callbacks

%% Initialize
init([Remote]) ->
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 0},
                  {active, false}],
    ClientSpec = {{realtime,[{1,0}]}, {TcpOptions, ?MODULE, self()}},

    %% Todo: check for bad remote name
    lager:debug("connecting to remote ~p", [Remote]),
    case riak_core_connection_mgr:connect({rt_repl, Remote}, ClientSpec) of
        {ok, Ref} ->
            lager:debug("connection ref ~p", [Ref]),
            {ok, #state{remote = Remote, connection_ref = Ref}};
        {error, Reason}->
            lager:warning("Error connecting to remote"),
            {stop, Reason}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(status, _From, State =
                #state{remote = R, address = _A, transport = T, socket = S,
                       helper_pid = H}) ->
    Props = case T of
                undefined ->
                    [{connected, false}];
                _ ->
                    SocketStats = riak_core_tcp_mon:socket_status(S),
                    [{connected, true},
                     %%{address, riak_repl_util:format_ip_and_port(A)},
                     {transport, T},
                     %%{socket_raw, S},
                     {socket,
                      riak_core_tcp_mon:format_socket_stats(SocketStats, [])},
                     %%{peername, peername(State)},
                     {helper_pid, riak_repl_util:safe_pid_to_list(H)}]
            end,
    HelperProps = case H of
                      undefined ->
                          [];
                      _ ->
                          try
                              Timeout = app_helper:get_env(
                                          riak_repl, status_helper_timeout,
                                          app_helper:get_env(riak_repl, status_timeout, 5000) - 1000),
                              riak_repl2_rtsource_helper:status(H, Timeout)
                          catch
                              _:{timeout, _} ->
                                  [{helper, timeout}]
                          end
                  end,
    FormattedPid = riak_repl_util:safe_pid_to_list(self()),
    Status = [{source, R}, {pid, FormattedPid}] ++ Props ++ HelperProps,
    {reply, Status, State};
handle_call(legacy_status, _From, State = #state{remote = Remote}) ->
    RTQStatus = riak_repl2_rtq:status(),
    QBS = proplists:get_value(bytes, RTQStatus),
    Consumers = proplists:get_value(consumers, RTQStatus),
    QStats = case proplists:get_value(Remote, Consumers) of
                 undefined ->
                     [];
                 Consumer ->
                     QL = proplists:get_value(pending, Consumer, 0) +
                         proplists:get_value(unacked, Consumer, 0),
                     DC = proplists:get_value(drops, Consumer),
                     [{dropped_count, DC},
                      {queue_length, QL},     % pending + unacknowledged for this conn
                      {queue_byte_size, QBS}] % approximation, this it total q size
             end,
    SocketStats = riak_core_tcp_mon:socket_status(State#state.socket),
    Status =
        [{node, node()},
         {site, Remote},
         {strategy, realtime},
         {socket, riak_core_tcp_mon:format_socket_stats(SocketStats, [])}],
        QStats,
    {reply, {status, Status}, State};
%% Receive connection from connection manager
handle_call({connected, Socket, Transport, EndPoint, Proto}, _From, 
            State = #state{remote = Remote}) ->
    %% Check the socket is valid, may have been an error 
    %% before turning it active (e.g. handoff of riak_core_service_mgr to handler
    case Transport:send(Socket, <<>>) of
        ok ->
            {ok, HelperPid} = riak_repl2_rtsource_helper:start_link(Remote, Transport, Socket),
            SocketTag = riak_repl_util:generate_socket_tag("rt_source", Socket),
            lager:debug("Keeping stats for " ++ SocketTag),
            riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, source,
                                               SocketTag}, Transport),
            {reply, ok, State#state{transport = Transport, 
                                    socket = Socket,
                                    address = EndPoint,
                                    proto = Proto,
                                    helper_pid = HelperPid}};
        ER ->
            {reply, ER, State}
    end.

%% Connection manager failed to make connection
%% TODO: Consider reissuing connect against another host - maybe that
%%   functionality should be in the connection manager (I want a connection to site X)
handle_cast({connect_failed, _HelperPid, Reason},
            State = #state{remote = Remote}) ->
    lager:warning("Realtime replication connection to site ~p failed - ~p\n",
                  [Remote, Reason]),
    {stop, normal, State}.
    
handle_info({tcp, _S, TcpBin}, State= #state{cont = Cont}) ->
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({tcp_closed, _S}, 
            State = #state{remote = Remote, cont = Cont}) ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            lager:warning("Realtime connection ~p to ~p closed with partial receive of ~b bytes\n",
                          [peername(State), Remote, NumBytes])
    end,
    %% go to sleep for 1s so a sink that opens the connection ok but then 
    %% dies will not make the server restart too fst.
    timer:sleep(1000),
    {stop, normal, State};
handle_info({tcp_error, _S, Reason}, 
            State = #state{remote = Remote, cont = Cont}) ->
    lager:warning("Realtime connection ~p to ~p network error ~p - ~b bytes pending\n",
                  [peername(State), Remote, Reason, size(Cont)]),
    {stop, normal, State}.


terminate(_Reason, #state{helper_pid = HelperPid}) ->
    %%TODO: check if this is called, don't think it is on normal supervisor
    %%      start/shutdown without trap exit set
    case HelperPid of 
        undefined ->
            ok;
        _ ->
            riak_repl2_rtsource_helper:stop(HelperPid)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

recv(TcpBin, State = #state{remote = Name}) ->
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            {noreply, State#state{cont = Cont}};
        {ok, {ack, Seq}, Cont} ->
            %% TODO: report this better per-remote
            riak_repl_stats:objects_sent(),
            ok = riak_repl2_rtq:ack(Name, Seq),
            recv(Cont, State);
        {error, Reason} ->
            %% Something bad happened
            {stop, {framing_error, Reason}, State}
    end.

peername(#state{transport = T, socket = S}) ->
    case T:peername(S) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            {lists:flatten(io_lib:format("error:~p", [Reason])), 0}
    end.
