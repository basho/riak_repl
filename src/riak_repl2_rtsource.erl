%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource).

%% @doc Realtime replication source module
%%
%% High level responsibility...
%%

-behaviour(gen_server).
%% API
-export([start_link/1,
         stop/1,
         status/1, status/2]).

%% connection manager callbacks
-export([connected/5,
         connect_failed/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {remote,    % remote name
                address,   % {IP, Port}
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

%% connection manager callbacks
status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

connected(Socket, Transport, Endpoint, Proto, RtSourcePid) ->
    gen_server:call(RtSourcePid,
                    {connected, Socket, Transport, Endpoint, Proto}).

connect_failed(_ClientProto, Reason, RtSourcePid) ->
    gen_server:cast(RtSourcePid, {connect_failed, self(), Reason}).

%% gen_server callbacks

%% Initialize
init([Remote]) ->
    %% TODO: Find a good way to get remote ports, for now just use appenv
    %% {riak_repl, remotes, [{Site, {Ip, Port}}]}
    Remotes = app_helper:get_env(riak_repl, remotes, []),
    case lists:keyfind(Remote, 1, Remotes) of
        {Remote, Address} ->
                       %% TODO: switch back to conn_mgr when ready
            TcpOptions = [{keepalive, true},
                          {nodelay, true},
                          {packet, 4}, %% TODO: get rid of packet, 4
                          {reuseaddr, true},
                          {active, false}],
            riak_core_connection:connect(Address, {realtime,[{1,0}]}, TcpOptions, {?MODULE, self()}),
            {ok, #state{remote = Remote}};
        _ ->
            {stop, {no_remote, Remote}}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(status, _From, State = 
                #state{remote = R, address = A, transport = T, socket = S,
                       helper_pid = H}) ->
    Props = case T of
                undefined ->
                    [{connected, false}];
                _ ->
                    [{connected, true},
                     {address, A},
                     {transport, T},
                     {socket, S},
                     {peer, T:peername(S)}]
            end,
    HelperProps = try
                      riak_repl2_rtsource_helper:status(H)
                  catch
                      _:{timeout, _} ->
                          [{helper, timeout}]
                  end,
    Status = {R, self(), Props ++ HelperProps},
    {reply, Status, State};
%% Receive connection from connection manager
handle_call({connected, Socket, Transport, EndPoint, Proto}, _From, 
            State = #state{remote = Remote}) ->
    ok = Transport:setopts(Socket, [{active, true}]),
    {ok, HelperPid} = riak_repl2_rtsource_helper:start_link(Remote, Transport, Socket),
    {reply, ok, State#state{transport = Transport, 
                            socket = Socket,
                            address = EndPoint,
                            proto = Proto,
                            helper_pid = HelperPid}}.

%% Connection manager failed to make connection
%% TODO: Consider reissuing connect against another host - maybe that
%%   functionality should be in the connection manager (I want a connection to site X)
handle_cast({connect_failed, HelperPid, Reason}, State = #state{helper_pid = HelperPid}) ->
    {stop, {error, {connect_failed, Reason}}, State}.
    
handle_info({tcp, _S, TcpBin}, State= #state{cont = Cont}) ->
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({tcp_closed, _S}, 
            State = #state{transport = T, socket = S, remote = Remote, cont = Cont}) ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            lager:warning("Realtime connection ~p to ~p closed with partial receive of ~b bytes\n",
                          [T:peername(S), Remote, NumBytes])
    end,
    {stop, normal, State};
handle_info({tcp_error, _S, Reason}, 
            State = #state{transport = T, socket = S, remote = Remote, cont = Cont}) ->
    lager:warning("Realtime connection ~p to ~p network error ~p - ~b bytes pending\n",
                  [T:peername(S), Remote, Reason, size(Cont)]),
    {stop, normal, State}.

terminate(_Reason, #state{helper_pid = HelperPid}) ->
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
            ok = riak_repl2_rtq:ack(Name, Seq),
            recv(Cont, State);
        {error, Reason} ->
            %% Something bad happened
            {stop, {framing_error, Reason}, State}
    end.
