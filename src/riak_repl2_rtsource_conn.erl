%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_conn).

%% @doc Realtime replication source connection module
%%
%% Works in tandem with rtsource_helper.  The helper interacts with
%% the RTQ to send queued traffic over the socket.  This rtsource_conn
%% process accepts the remote Acks and clears the RTQ.
%%
%% If both sides support heartbeat message, it is sent from the RT source
%% every {riak_repl, rt_heartbeat_interval} which default to 15s.  If
%% a response is not received in {riak_repl, rt_heartbeat_timeout}, also
%% default to 15s then the source connection exits and will be re-established
%% by the supervisor.
%%
%% 1. On startup/interval timer - rtsource_conn casts to rtsource_helper
%%    to send over the socket.  If TCP buffer is full or rtsource_helper
%%    is otherwise hung the rtsource_conn process will still continue.
%%    rtsource_conn sets up a heartbeat timeout.
%%
%% 2. At rtsink, on receipt of a heartbeat message it sends back
%%    a heartbeat message and stores the timestamp it last received one.
%%    The rtsink does not worry about detecting broken connections
%%    as new ones can be established harmlessly.  Keep it simple.
%%
%% 3. If rtsource receives the heartbeat back, it cancels the timer
%%    and updates the hearbeat round trip time (hb_rtt) then sets
%%    a new heartbeat_interval timer.
%%
%%    If the heartbeat_timeout fires, the rtsource connection terminates.
%%    The rtsource_helper:stop call is now wrapped in a timeout in
%%    case it is hung so we don't get nasty messages about rtsource_conn
%%    crashing when it's the helper that is causing the problems.

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

-define(DEFAULT_HBINTERVAL, timer:seconds(15)).
-define(DEFAULT_HBTIMEOUT, timer:seconds(15)).

-record(state, {remote,    % remote name
                address,   % {IP, Port}
                connection_ref, % reference handed out by connection manager
                transport, % transport module 
                socket,    % socket to use with transport 
                proto,     % protocol version negotiated
                ver,       % wire format negotiated
                helper_pid,% riak_repl2_rtsource_helper pid
                hb_interval,% milliseconds to send new heartbeat after last
                hb_timeout,% milliseconds to wait for heartbeat after send
                hb_timeout_tref,% heartbeat timeout timer reference
                hb_sent,   % now() last heartbeat was sent
                hb_rtt,    % RTT in milliseconds for last completed heartbeat
                cont = <<>>}). % continuation from previous TCP buffer

%% API - start trying to send realtime repl to remote site
start_link(Remote) ->
    gen_server:start_link(?MODULE, [Remote], []).

stop(Pid) ->
    gen_server:call(Pid, stop, ?LONG_TIMEOUT).
    
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
                    {connected, Socket, Transport, Endpoint, Proto},
                    ?LONG_TIMEOUT).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

%% gen_server callbacks

%% Initialize
init([Remote]) ->
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 0},
                  {active, false}],
    % nodes running 1.3.1 have a bug in the service_mgr module.
    % this bug prevents it from being able to negotiate a version list longer
    % than 2. Until we no longer support communicating with that version,
    % we need to artifically truncate the version list.
    % TODO: expand version list or remove comment when we no
    % longer support 1.3.1
    % prefered version list: [{2,0}, {1,5}, {1,1}, {1,0}]
    ClientSpec = {{realtime,[{2,0}, {1,5}]}, {TcpOptions, ?MODULE, self()}},

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
                       helper_pid = H,
                       hb_interval = HBInterval, hb_rtt = HBRTT}) ->
    Props = case T of
                undefined ->
                    [{connected, false}];
                _ ->
                    HBStats = case HBInterval of
                                  undefined ->
                                      [];
                                  _ ->
                                      [{hb_rtt, HBRTT}]
                              end,
                    SocketStats = riak_core_tcp_mon:socket_status(S),

                    [{connected, true},
                     %%{address, riak_repl_util:format_ip_and_port(A)},
                     {transport, T},
                     %%{socket_raw, S},
                     {socket,
                      riak_core_tcp_mon:format_socket_stats(SocketStats, [])},
                     %%{peername, peername(State)},
                     {helper_pid, riak_repl_util:safe_pid_to_list(H)}] ++
                        HBStats
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
            Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
            lager:debug("RT source connection negotiated ~p wire format from proto ~p", [Ver, Proto]),
            {_, ClientVer, _} = Proto,
            {ok, HelperPid} = riak_repl2_rtsource_helper:start_link(Remote, Transport, Socket, ClientVer),
            SocketTag = riak_repl_util:generate_socket_tag("rt_source", Transport, Socket),
            lager:debug("Keeping stats for " ++ SocketTag),
            riak_core_tcp_mon:monitor(Socket, {?TCP_MON_RT_APP, source,
                                               SocketTag}, Transport),
            State2 = State#state{transport = Transport, 
                                 socket = Socket,
                                 address = EndPoint,
                                 proto = Proto,
                                 helper_pid = HelperPid},
            lager:info("Established realtime connection to site ~p address ~s",
                      [Remote, peername(State2)]),
            case Proto of
                {realtime, _OurVer, {1, 0}} ->
                    {reply, ok, State2};
                _ ->
                    %% 1.1 and above, start with a heartbeat
                    HBInterval = app_helper:get_env(riak_repl, rt_heartbeat_interval,
                                                    ?DEFAULT_HBINTERVAL),
                    HBTimeout = app_helper:get_env(riak_repl, rt_heartbeat_timeout,
                                                   ?DEFAULT_HBTIMEOUT),
                    State3 = State2#state{hb_interval = HBInterval,
                                          hb_timeout = HBTimeout},
                    {reply, ok, send_heartbeat(State3)}
            end;
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

handle_info({Proto, _S, TcpBin}, State= #state{cont = Cont})
        when Proto == tcp; Proto == ssl ->
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({Closed, _S}, 
            State = #state{remote = Remote, cont = Cont})
        when Closed == tcp_closed; Closed == ssl_closed ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            riak_repl_stats:rt_source_errors(),
            lager:warning("Realtime connection ~s to ~p closed with partial receive of ~b bytes\n",
                          [peername(State), Remote, NumBytes])
    end,
    %% go to sleep for 1s so a sink that opens the connection ok but then 
    %% dies will not make the server restart too fst.
    timer:sleep(1000),
    {stop, normal, State};
handle_info({Error, _S, Reason}, 
            State = #state{remote = Remote, cont = Cont})
        when Error == tcp_error; Error == ssl_error ->
    riak_repl_stats:rt_source_errors(),
    lager:warning("Realtime connection ~s to ~p network error ~p - ~b bytes pending\n",
                  [peername(State), Remote, Reason, size(Cont)]),
    {stop, normal, State};
handle_info(send_heartbeat, State) ->
    {noreply, send_heartbeat(State)};
handle_info({heartbeat_timeout, HBSent}, State = #state{hb_sent = HBSent,
                                                        remote = Remote}) ->
    Duration = safe_now_diff(HBSent),
    lager:warning("Realtime connection ~s to ~p heartbeat timeout "
                  "after ~p milliseconds\n",
                  [peername(State), Remote, Duration]),
    {stop, normal, State};
handle_info({heartbeat_timeout, _HBSent}, State = #state{remote = Remote}) ->
    %% Timeout message was in the queue when we received the heartbeat
    %% already handled, ignore.
    lager:info("Realtime connection ~s to ~p received stale timeout\n",
               [peername(State), Remote]),
    {noreply, State};
handle_info(Msg, State) ->
    lager:warning("Unhandled info:  ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, #state{helper_pid = HelperPid}) ->
    %%TODO: check if this is called, don't think it is on normal supervisor
    %%      start/shutdown without trap exit set
    case HelperPid of 
        undefined ->
            ok;
        _ ->
            try
                riak_repl2_rtsource_helper:stop(HelperPid)
            catch
                _:Err ->
                    lager:info("Realtime source did not cleanly stop ~p - ~p\n",
                               [HelperPid, Err])
            end
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

recv(TcpBin, State = #state{remote = Name,
                            hb_sent = HBSent,
                            hb_timeout_tref = HBTRef}) ->
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            {noreply, State#state{cont = Cont}};
        {ok, {ack, Seq}, Cont} when State#state.proto == {2,0} ->
            %% TODO: report this better per-remote
            riak_repl_stats:objects_sent(),
            ok = riak_repl2_rtq:ack(Name, Seq),
            %% reset heartbeat timer, since we've seen activity from the peer
            case HBTRef of
                undefined ->
                    recv(Cont, State);
                _ ->
                    erlang:cancel_timer(HBTRef)
                    recv(Cont, schedule_heartbeat(State))
            end;
        {ok, {ack, Seq}, Cont} ->
            riak_repl2_rtsource_helper:v1_ack(State#state.helper_pid, Seq),
            %% reset heartbeat timer, since we've seen activity from the peer
            case HBTRef of
                undefined ->
                    recv(Cont, State);
                _ ->
                    erlang:cancel_timer(HBTRef)
                    recv(Cont, schedule_heartbeat(State))
            end;
        {ok, heartbeat, Cont} ->
            %% Compute last heartbeat roundtrip in msecs and
            %% reschedule next
            HBRTT = safe_now_diff(HBSent),
            case HBTRef of
                undefined ->
                    ok;
                _ ->
                    erlang:cancel_timer(HBTRef)
            end,
            State2 = State#state{hb_sent = undefined,
                                 hb_timeout_tref = undefined,
                                 hb_rtt = HBRTT},
            recv(Cont, schedule_heartbeat(State2));
        {error, Reason} ->
            %% Something bad happened
            riak_repl_stats:rt_source_errors(),
            {stop, {framing_error, Reason}, State}
    end.

peername(#state{transport = T, socket = S}) ->
    riak_repl_util:peername(S, T).

%% Heartbeat is disabled, do nothing
send_heartbeat(State = #state{hb_interval = undefined}) ->
    State;
%% Heartbeat supported and enabled, tell helper to send the message,
%% and start the timeout.  Managing heartbeat from this process
%% will catch any bug that causes the helper process to hang as
%% well as connection issues - either way we want to re-establish.
send_heartbeat(State = #state{hb_timeout = HBTimeout,
                              helper_pid = HelperPid}) ->
    Now = now(), % using now as need a unique reference for this heartbeat
                 % to spot late heartbeat timeout messages
    riak_repl2_rtsource_helper:send_heartbeat(HelperPid),
    TRef = erlang:send_after(HBTimeout, self(), {heartbeat_timeout, Now}),
    State#state{hb_sent = Now, hb_timeout_tref = TRef}.

%% Schedule the next heartbeat
schedule_heartbeat(State = #state{hb_interval = HBInterval}) ->
    erlang:send_after(HBInterval, self(), send_heartbeat),
    State.

safe_now_diff(NotNow) ->
    safe_now_diff(now(), NotNow).

safe_now_diff({_,_,_} = Sooner, {_,_,_} = Later) ->
    timer:now_diff(Sooner, Later) div 1000;
safe_now_diff(_,_) ->
    0.
