%% Riak Replication Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_connection).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% Tcp options used during the connection and negotiation phase
-define(CONNECT_OPTIONS, [binary,
                          {keepalive, true},
                          {nodelay, true},
                          {packet, 4},
                          {reuseaddr, true},
                          {active, false}]).

%% public API
-export([start_dispatcher/3,
         stop_dispatcher/1,
         connect/2,
         sync_connect/2,
         start_link/4]).

%% internal functions
-export([async_connect_proc/3,
         dispatch_service/4,
         valid_host_ip/1,
         normalize_ip/1]).

%% @doc Start the connection dispatcher with a limit of MaxListeners
%% listener connections and supported sub-protocols. When a connection
%% request arrives, it is mapped via the associated Protocol atom to an
%% acceptor function called as Module:Function(Listener, Socket, Transport, Args),
%% which must create it's own process and return {ok, pid()}

-spec(start_dispatcher(ip_addr(), non_neg_integer(), [hostspec()]) -> {ok, pid()}).
start_dispatcher({IP,Port}, MaxListeners, SubProtocols) ->
    case valid_host_ip(IP) of
        true ->
            ?TRACE(?debugFmt("Connection manager: starting listener on ~s:~p", [IP, Port])),
            {ok, RawAddress} = inet_parse:address(IP),
            ranch:start_listener({IP,Port}, MaxListeners, ranch_tcp,
                [{ip, RawAddress}, {port, Port}], ?MODULE, SubProtocols);
        _ ->
            lager:error("Connection mananger: failed to start ranch listener "
                        "on ~s:~p - invalid address.", [IP, Port])
    end.

stop_dispatcher({IP,Port}) ->
    ranch:stop_listener({IP,Port}).

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

%% Returns true if the IP address given is a valid host IP address.
%% stolen from riak_repl_util.erl
valid_host_ip(IP) ->     
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:foldl(
        fun({_IF, Attrs}, Match) ->
                case lists:member({addr, NormIP}, Attrs) of
                    true ->
                        true;
                    _ ->
                        Match
                end
        end, false, IFs).

%% Convert IP address the tuple form
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when is_tuple(IP) ->
    {ok, IP}.

%% exchange brief handshake with client to ensure that we're supporting sub-protocols.
%% client -> server : Client-Hello
%% server -> client : Server-Hello
exchange_handshakes_with(client, Socket, Transport) ->
    ?TRACE(?debugFmt("exchange_handshakes: waiting for ~p from client", [?CTRL_HELLO])),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, ?CTRL_HELLO} ->
            Transport:send(Socket, ?CTRL_ACK);
        {error, Reason} ->
            lager:error("Failed to exchange handshake with client. Error = ~p", [Reason]),
            {error, Reason}
    end;
exchange_handshakes_with(host, Socket, Transport) ->
    ?TRACE(?debugFmt("exchange_handshakes: sending ~p to host", [?CTRL_HELLO])),
    ok = Transport:send(Socket, ?CTRL_HELLO),
    ?TRACE(?debugFmt("exchange_handshakes: waiting for ~p from host", [?CTRL_ACK])),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, ?CTRL_ACK} ->
            ?TRACE(?debugFmt("exchange_handshakes with host: got ~p", [?CTRL_ACK])),
            ok;
        {error, Reason} ->
            ?TRACE(?debugFmt("Failed to exchange handshake with host. Error = ~p", [Reason])),
            lager:error("Failed to exchange handshake with host. Error = ~p", [Reason]),
            {error, Reason}
    end.

async_connect_proc(Parent, {IP,Port}, ProtocolSpec) ->
    sync_connect_status(Parent, {IP,Port}, ProtocolSpec).

%% connect synchronously to remote addr/port and return status
sync_connect_status(Parent, {IP,Port}, {ClientProtocol, {Options, Module, Args}}) ->
    Timeout = 15000,
    Transport = ranch_tcp,
    %%   connect to host's {IP,Port}
    ?TRACE(?debugFmt("sync_connect: connect to ~p", [{IP,Port}])),
    case gen_tcp:connect(IP, Port, ?CONNECT_OPTIONS, Timeout) of
        {ok, Socket} ->
            ?TRACE(?debugFmt("Setting system options on client side: ~p", [?CONNECT_OPTIONS])),
            Transport:setopts(Socket, ?CONNECT_OPTIONS),
            %% handshake to make sure it's a riak sub-protocol dispatcher
            ok = exchange_handshakes_with(host, Socket, Transport),
            %% ask for protocol, see what host has
            case negotiate_proto_with_server(Socket, Transport, ClientProtocol) of
                {ok,HostProtocol} ->
                    %% set client's requested Tcp options
                    ?TRACE(?debugFmt("Setting user options on client side; ~p", [Options])),
                    Transport:setopts(Socket, Options),
                    %% transfer the socket to the process that requested the connection
                    ok = Transport:controlling_process(Socket, Parent),
                    %% notify requester of connection and negotiated protocol from host
                    Module:connected(Socket, Transport, {IP, Port}, HostProtocol, Args),
                    ok;
                {error, Reason} ->
                    ?debugFmt("negotiate_proto_with_server returned: ~p", [{error,Reason}]),
                    Module:connect_failed(ClientProtocol, {error, Reason}, Args),
                    {error, Reason}
            end;
        {error, Reason} ->
            Module:connect_failed(ClientProtocol, {error, Reason}, Args),
            {error, Reason}
    end.

%% Host callback function, called by ranch for each accepted connection by way of
%% of the ranch:start_listener() call above, specifying this module.
start_link(Listener, Socket, Transport, SubProtocols) ->
    ?TRACE(?debugMsg("Start_link dispatch_service")),
    {ok, spawn_link(?MODULE, dispatch_service, [Listener, Socket, Transport, SubProtocols])}.

%% Body of the main dispatch loop. This is instantiated once for each connection
%% we accept because it transforms itself into the SubProtocol once it receives
%% the sub protocol and version, negotiated with the client.
dispatch_service(Listener, Socket, Transport, SubProtocols) ->
    ?TRACE(?debugMsg("started dispatch_service")),
    %% tell ranch "we've got it. thanks pardner"
    ok = ranch:accept_ack(Listener),
    %% set some starting options for the channel; these should match the client
    ?TRACE(?debugFmt("setting system options on service side: ~p", [?CONNECT_OPTIONS])),
    ok = Transport:setopts(Socket, ?CONNECT_OPTIONS),
    ok = exchange_handshakes_with(client, Socket, Transport),
    Negotiated = negotiate_proto_with_client(Socket, Transport, SubProtocols),
    ?TRACE(?debugFmt("negotiated = ~p", [Negotiated])),
    start_negotiated_service(Socket, Transport, Negotiated).

%% start user's module:function and transfer socket to it's process.
start_negotiated_service(_Socket, _Transport, {error, Reason}) ->
    ?TRACE(?debugFmt("service dispatch failed with ~p", [{error, Reason}])),
    {error, Reason};
start_negotiated_service(Socket, Transport,
                         {NegotiatedProtocols, {Options, Module, Function, Args}}) ->
    %% Set requested Tcp socket options now that we've finished handshake phase
    ?TRACE(?debugFmt("Setting user options on service side; ~p", [Options])),
    Transport:setopts(Socket, Options),
    %% call service body function for matching protocol. The callee should start
    %% a process or gen_server or such, and return {ok, pid()}.
    case Module:Function(Socket, Transport, NegotiatedProtocols, Args) of
        {ok, Pid} ->
            %% transfer control of socket to new service process
            ok = Transport:controlling_process(Socket, Pid),
            {ok, Pid};
        {error, Reason} ->
            ?TRACE(?debugFmt("service dispatch of ~p:~p failed with ~p",
                             [Module, Function, Reason])),
            {error,Reason}
    end.

%% Negotiate the highest common major protocol revisision with the connected client.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]} as binary
%% server -> client : selected version = {SubProto, {Major, HostMinor}} as binary
%%
%% returns {ok,{{Proto,MyVer,RemoteVer},Options,Module,Function,Args}} | Error
negotiate_proto_with_client(Socket, Transport, HostProtocols) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, PrefsBin} ->
            {ClientProto,Versions} = erlang:binary_to_term(PrefsBin),
            case choose_version({ClientProto,Versions}, HostProtocols) of
                {error, Reason} ->
                    lager:error("Failed to negotiate protocol ~p from client because ~p",
                                [ClientProto, Reason]),
                    Transport:send(Socket, erlang:term_to_binary({error,Reason})),
                    {error, Reason};
                {ok,{ClientProto,Major,CN,HN}, Rest} ->
                    Transport:send(Socket, erlang:term_to_binary({ok,{ClientProto,{Major,HN,CN}}})),
                    {{ok,{ClientProto,{Major,HN},{Major,CN}}}, Rest};
                {error, Reason, Rest} ->
                    lager:error("Failed to negotiate protocol ~p from client because ~p",
                                [ClientProto, Reason]),
                    %% notify client it failed to negotiate
                    Transport:send(Socket, erlang:term_to_binary({error,Reason})),
                    {{error, Reason}, Rest}
            end;
        {error, Reason} ->
            lager:error("Failed to receive protocol request from client. Error = ~p",
                        [Reason]),
            connection_failed
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

choose_version({ClientProto,ClientVersions}=_CProtocol, HostProtocols) ->
    ?TRACE(?debugFmt("choose_version: client proto = ~p", [_CProtocol])),
    %% first, see if the host supports the subprotocol
    case [H || {{HostProto,_Versions},_Rest}=H <- HostProtocols, ClientProto == HostProto] of
        [] ->
            %% oops! The host does not support this sub protocol type
            lager:error("Failed to find host support for protocol: ~p", [ClientProto]),
            {error,protocol_not_supported};
        [{{_HostProto,HostVersions},Rest}=_Matched | _DuplicatesIgnored] ->
            ?TRACE(?debugFmt("choose_version: unsorted = ~p clientversions = ~p",
                             [_Matched, ClientVersions])),
            CommonVers = [{CM,CN,HN} || {CM,CN} <- ClientVersions, {HM,HN} <- HostVersions, CM == HM],
            ?TRACE(?debugFmt("common versions = ~p", [CommonVers])),
            %% sort by major version, highest to lowest, and grab the top one.
            case lists:reverse(lists:keysort(1,CommonVers)) of
                [] ->
                    %% oops! No common major versions for Proto.
                    ?TRACE(?debugFmt("Failed to find a common major version for protocol: ~p",
                                     [ClientProto])),
                    lager:error("Failed to find a common major version for protocol: ~p", [ClientProto]),
                    {error,protocol_version_not_supported,Rest};
                [{Major,CN,HN}] ->
                    {ok, {ClientProto,Major,CN,HN},Rest};
                [{Major,CN,HN}, _] ->
                    {ok, {ClientProto,Major,CN,HN},Rest}
            end
    end.
