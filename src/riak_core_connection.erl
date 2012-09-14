%% Riak Replication Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_connection).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

%% public API
-export([connect/2,
         sync_connect/2]).

%% internal functions
-export([async_connect_proc/3]).

%% Make async connection request. The connection manager is responsible for retry/backoff
%% and calls your module's functions on success or error (asynchrously):
%%   Module:connected(Socket, TransportModule, {IpAddress, Port}, {Proto, MyVer, RemoteVer}, Args)
%%   Module:connect_failed(Proto, {error, Reason}, Args)
%%       Reason could be 'protocol_version_not_supported"
%%
%%
%% You can set options on the tcp connection, e.g.
%% [{packet, 4}, {active, false}, {keepalive, true}, {nodelay, true}]
%%
%% ClientProtocol specifies the preferences of the client, in terms of what versions
%% of a protocol it speaks. The host will choose the highest common major version and
%% inform the client via the callback Module:connected() in the HostProtocol parameter.
%%
%% Note: that the connection will initially be setup with the `binary` option
%% because protocol negotiation requires binary. TODO: should we allow non-binary
%% options? Check that binary is not overwritten.
%%
%% connect returns the pid() of the asynchronous process that will attempt the connection.

-spec(connect(ip_addr(), clientspec()) -> pid()).
connect({IP,Port}, ClientSpec) ->
    ?TRACE(?debugMsg("spawning async_connect link")),
    %% start a process to handle the connection request asyncrhonously
    proc_lib:spawn_link(?MODULE, async_connect_proc, [self(), {IP,Port}, ClientSpec]).

sync_connect({IP,Port}, ClientSpec) ->
    sync_connect_status(self(), {IP,Port}, ClientSpec).

%% @private

%% exchange brief handshake with client to ensure that we're supporting sub-protocols.
%% client -> server : Client-Hello
%% server -> client : Server-Hello
exchange_handshakes_with(host, Socket, Transport) ->
    ?TRACE(?debugFmt("exchange_handshakes: sending ~p to host", [?CTRL_HELLO])),
    case Transport:send(Socket, ?CTRL_HELLO) of
        ok ->
            ?TRACE(?debugFmt("exchange_handshakes: waiting for ~p from host", [?CTRL_ACK])),
            case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
                {ok, ?CTRL_ACK} ->
                    ?TRACE(?debugFmt("exchange_handshakes with host: got ~p", [?CTRL_ACK])),
                    ok;
                {error, Reason} ->
                    ?TRACE(?debugFmt("Failed to exchange handshake with host. Error = ~p", [Reason])),
                    lager:error("Failed to exchange handshake with host. Error = ~p", [Reason]),
                    {error, Reason}
            end;
        Error ->
            Error
    end.

async_connect_proc(Parent, {IP,Port}, ProtocolSpec) ->
    sync_connect_status(Parent, {IP,Port}, ProtocolSpec).

%% connect synchronously to remote addr/port and return status
sync_connect_status(_Parent, {IP,Port}, {ClientProtocol, {Options, Module, Args}}) ->
    Timeout = 15000,
    Transport = ranch_tcp,
    %%   connect to host's {IP,Port}
    ?TRACE(?debugFmt("sync_connect: connect to ~p", [{IP,Port}])),
    case gen_tcp:connect(IP, Port, ?CONNECT_OPTIONS, Timeout) of
        {ok, Socket} ->
            ?TRACE(?debugFmt("Setting system options on client side: ~p", [?CONNECT_OPTIONS])),
            Transport:setopts(Socket, ?CONNECT_OPTIONS),
            %% handshake to make sure it's a riak sub-protocol dispatcher
            case exchange_handshakes_with(host, Socket, Transport) of
                ok ->
                    %% ask for protocol, see what host has
                    case negotiate_proto_with_server(Socket, Transport, ClientProtocol) of
                        {ok,HostProtocol} ->
                            %% set client's requested Tcp options
                            ?TRACE(?debugFmt("Setting user options on client side; ~p", [Options])),
                            Transport:setopts(Socket, Options),
                            %% notify requester of connection and negotiated protocol from host
                            %% pass back returned value in case problem detected on connection
                            %% by module.  requestor is responsible for transferring control
                            %% of the socket.
                            Module:connected(Socket, Transport, {IP, Port}, HostProtocol, Args);
                        {error, Reason} ->
                            ?TRACE(?debugFmt("negotiate_proto_with_server returned: ~p", [{error,Reason}])),
                            Module:connect_failed(ClientProtocol, {error, Reason}, Args),
                            {error, Reason}
                    end;
                Error ->
                    %% failed to exchange handshakes, probably because the socket closed
                    Error
            end;
        {error, Reason} ->
            Module:connect_failed(ClientProtocol, {error, Reason}, Args),
            {error, Reason}
    end.

%% Negotiate the highest common major protocol revisision with the connected server.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]}
%% server -> client : selected version = {SubProto, {Major, HostMinor, ClientMinor}}
%%
%% returns {ok,{Proto,{Major,ClientMinor},{Major,HostMinor}}} | {error, Reason}
negotiate_proto_with_server(Socket, Transport, ClientProtocol) ->
    ?TRACE(?debugFmt("negotiate protocol with host, client proto = ~p", [ClientProtocol])),
    Transport:send(Socket, erlang:term_to_binary(ClientProtocol)),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, NegotiatedProtocolBin} ->
            case erlang:binary_to_term(NegotiatedProtocolBin) of
                {ok, {Proto,{CommonMajor,HMinor,CMinor}}} ->
                    {ok, {Proto,{CommonMajor,CMinor},{CommonMajor,HMinor}}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:error("Failed to receive protocol ~p response from server. Reason = ~p",
                        [ClientProtocol, Reason]),
            {error, connection_failed}
    end.
