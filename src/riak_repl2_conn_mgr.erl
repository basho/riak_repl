%% Riak Replication Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_repl2_conn_mgr).

-include("riak_repl.hrl").
-include_lib("eunit/include/eunit.hrl").

%% public API
-export([start_dispatcher/3, connect/4, start_link/4]).

%% internal functions
-export([async_connect/5, dispatch_service/4]).

-define(CTRL_HELLO, <<"riak-ctrl:hello">>).
-define(CTRL_ACK, <<"riak-ctrl:ack">>).

-type(rev() :: non_neg_integer()). %% major or minor revision number
-type(proto() :: {atom(), {rev(), rev()}}). %% e.g. {realtime_repl, 1, 0}
-type(protoprefs() :: {atom(), [{rev(), rev()}]}).

%% Function = fun(Socket, Transport, Protocol, Args) -> ok
%% Protocol :: proto()
-type(service_started_callback() :: fun((inet:socket(), module(), proto(), [any()]) -> no_return())).

-type(protospec() :: {protoprefs(), module(), service_started_callback(), [any()]}).

%% @doc Start the connection dispatcher with a limit of MaxListeners
%% listener connections and supported sub-protocols. When a connection
%% request arrives, it is mapped via the associated Protocol atom to an
%% acceptor function called as Module:Function(Listener, Socket, Transport, Args),
%% which must create it's own process and return {ok, pid()}

-spec(start_dispatcher(repl_addr(), non_neg_integer(), [protospec()]) -> {ok, pid()}).
start_dispatcher({IP,Port}, MaxListeners, SubProtocols) ->
    case riak_repl_util:valid_host_ip(IP) of
        true ->
            ?debugFmt("Connection manager: starting ranch listener on ~s:~p",
                      [IP, Port]),
            {ok, RawAddress} = inet_parse:address(IP),
            ranch:start_listener({IP,Port}, MaxListeners, ranch_tcp,
                [{ip, RawAddress}, {port, Port}], ?MODULE, SubProtocols);
        _ ->
            ?debugFmt("Connection mananger: failed to start ranch listener "
                    "on ~s:~p - invalid address.", [IP, Port])
    end.

%% Make async connection request. The connection manager is responsible for retry/backoff
%% and calls your module's functions on success or error (asynchrously):
%%   Module:connected(Socket, TransportModule, {IpAddress, Port}, HostProtocol, Args)
%%   Module:connect_failed(Proto, {error, Reason})
%%
%% You can set options on the tcp connection, e.g.
%% [{packet, 4}, {active, false}, {keepalive, true}, {nodelay, true}]
%%
%% Note: that the connection will initially be setup with the `binary` option
%% because protocol negotiation requires binary. TODO: should we allow non-binary
%% options? Check that binary is not overwritten.
%%
%% connect returns the pid() of the asynchronous process that will attempt the connection.

-spec(connect({repl_addr(),port()}, proto(), [any()], {module(),[any()]}) -> pid()).
connect({IP,Port}, {Protocol, {Major, Minor}}, Options, {Module, Args}) ->
    %% start a process to handle the connection request asyncrhonously
    proc_lib:spawn_link(?MODULE, async_connect, [self(), {IP,Port}, {Protocol, {Major, Minor}},
                                                 Options, {Module, Args}]).

%% @private

%% exchange brief handshake with client to ensure that we're supporting sub-protocols.
%% client -> server : Client-Hello
%% server -> client : Server-Hello
exchange_handshakes_with(client, Socket, Transport) ->
    ?debugMsg("exchange_handshakes with client"),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, ?CTRL_HELLO} ->
            Transport:send(Socket, ?CTRL_ACK);
        {error, Reason} ->
            riak_repl_stats:server_connect_errors(),
            lager:error("Failed to exchange handshake with client. Error = ~p", [Reason]),
            {error, Reason}
    end;
exchange_handshakes_with(host, Socket, Transport) ->
    ?debugFmt("exchange_handshakes with host: send ~p", [?CTRL_HELLO]),
    ok = Transport:send(Socket, ?CTRL_HELLO),
    ?debugMsg("exchange_handshakes with host: recv handshake"),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, ?CTRL_ACK} ->
            ?debugFmt("exchange_handshakes with host: got ~p", [?CTRL_ACK]),
            ok;
        {error, Reason} ->
            ?debugFmt("Failed to exchange handshake with host. Error = ~p", [Reason]),
            riak_repl_stats:server_connect_errors(),
            lager:error("Failed to exchange handshake with host. Error = ~p", [Reason]),
            {error, Reason}
    end.

%% Function spawned to do async connect
async_connect(Parent, {IP,Port}, ClientProtocol,  Options, {Module, Args}) ->
    %% TODO: move the timeout into the connect call to remove dep on repl.
    Timeout = app_helper:get_env(riak_repl, client_connect_timeout, 15000),
    Transport = ranch_tcp,
    %%   connect to host's {IP,Port}
    ?debugFmt("async_connect: connect to ~p", [{IP,Port}]),
    case gen_tcp:connect(IP, Port, [binary | Options], Timeout) of 
        {ok, Socket} ->
            %% handshake to make sure it's a riak sub-protocol dispatcher
            ok = exchange_handshakes_with(host, Socket, Transport),
            %% ask for protocol, see what host has
            {ok,HostProtocol} = negotiate_proto_with_server(Socket, Transport, ClientProtocol),
            %% transfer the socket to the process that requested the connection
            ok = Transport:controlling_process(Socket, Parent),
            %% notify requester of connection and negotiated protocol from host
            Module:connected(Socket, Transport, {IP, Port}, HostProtocol, Args);
        {error, Reason} ->
            Module:connect_failed(ClientProtocol, {error, Reason})
    end.

%% Host callback function, called by ranch for each accepted connection by way of
%% of the ranch:start_listener() call above, specifying this module.
start_link(Listener, Socket, Transport, SubProtocols) ->
    {ok, spawn_link(?MODULE, dispatch_service, [Listener, Socket, Transport, SubProtocols])}.

%% Body of the main dispatch loop. This is instantiated once for each connection
%% we accept because it transforms itself into the SubProtocol once it receives
%% the sub protocol and version, negotiated with the client.
dispatch_service(Listener, Socket, Transport, SubProtocols) ->
    ?debugFmt("dispatch_service: Listener=~p Socket=~p Transport=~p protocols: ~p",
              [Listener, Socket, Transport, SubProtocols]),
    %% tell ranch "we've got it. thanks pardner"
    ok = ranch:accept_ack(Listener),
    %% set some starting options for the channel; these should match the client
    ?debugMsg("dispatch_service setopts"),
    ok = Transport:setopts(Socket, [
            binary,
            {keepalive, true},
            {nodelay, true},
            {packet, 4},
            {reuseaddr, true},
            {active, false}]),
    ok = exchange_handshakes_with(client, Socket, Transport),
    {ok,Chosen} = negotiate_proto_with_client(Socket, Transport, SubProtocols),
    run_negotiated_service(Socket, Transport, Chosen).

run_negotiated_service(Socket, Transport, {Protocol, Module, Function, Args}) ->
    %% call service body function for matching protocol.
    %% does not return until service terminates itself or is killed.
    Module:Function(Socket, Transport, Protocol, Args).

%% Negotiate the highest common major protocol revisision with the connected client.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]} as binary
%% server -> client : selected version = {SubProto, {Major, HostMinor}} as binary
%%
%% returns {ok,{{Proto,{Major,ClientMinor}},Module,Function,Args}} | Error
negotiate_proto_with_client(Socket, Transport, HostProtocols) ->
    ?debugMsg("negotiate protocol with client"),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, PrefsBin} ->
            {ClientProto,Versions} = erlang:binary_to_term(PrefsBin),
            {ok,{{ClientProto,Major,CN,HN}, Module, Function, Args}} =
                choose_version({ClientProto,Versions}, HostProtocols),
            Transport:send(Socket, erlang:term_to_binary({ClientProto,{Major,HN}})),
            {ok,{{ClientProto,{Major,CN}}, Module, Function, Args}};
        {error, Reason} ->
            riak_repl_stats:server_connect_errors(),
            lager:error("Failed to receive protocol request from client. Error = ~p",
                        [Reason]),
            connection_failed
    end.

%% Negotiate the highest common major protocol revisision with the connected server.
%% client -> server : Prefs List = {SubProto, [{Major, Minor}]} as binary
%% server -> client : selected version = {SubProto, {Major, HostMinor}} as binary
%%
%% returns {ok,{Proto,{Major,HostMinor}}} | {error, Reason}
negotiate_proto_with_server(Socket, Transport, ClientProtocol) ->
    ?debugFmt("negotiate protocol with server, client proto = ~p", [ClientProtocol]),
    Transport:send(Socket, erlang:term_to_binary(ClientProtocol)),
    case Transport:recv(Socket, 0, ?PEERINFO_TIMEOUT) of
        {ok, HostProtocolBin} ->
            HostProtocol = erlang:binary_to_term(HostProtocolBin),
            {ok, HostProtocol};
        {error, Reason} ->
            riak_repl_stats:client_connect_errors(),
            lager:error("Failed to negotiate protocol ~p with server. Error = ~p",
                        [ClientProtocol, Reason]),
            connection_failed
    end.

choose_version({ClientProto,ClientVersions}, HostProtocols) ->
    ?debugFmt("choose_version: client proto = ~p", [ClientProto]),
    %% first, see if the host supports the subprotocol
    case [H || {{HostProto,_Versions},_M,_F,_A}=H <- HostProtocols, ClientProto == HostProto] of
        [] ->
            %% oops! The host does not support this sub protocol type
            lager:warn("Failed to find host support for protocol: ~p", [ClientProto]),
            protocol_not_supported;
        [{{_HostProto,HostVersions},M,F,A} | _DuplicatesIgnored] ->
            CommonVers = [{CM,CN,HN} || {CM,CN} <- ClientVersions, {HM,HN} <- HostVersions, CM == HM],
            %% sort by major version, highest to lowest, and grab the top one.
            case lists:reverse(lists:keysort(1,CommonVers)) of
                [] ->
                    %% oops! No common major versions for Proto.
                    lager:warn("Failed to find a common major version for protocol: ~p", [ClientProto]),
                    protocol_version_not_supported;
                [{Major,CN,HN}, _] ->
                    {ok, {{ClientProto,Major,CN,HN},M,F,A}}
            end
    end.
