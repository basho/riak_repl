%% Riak Replication Subprotocol Server Dispatcher
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_service_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(TRACE(Stmt),ok).
-endif.

-define(SERVER, riak_core_service_manager).
-define(MAX_LISTENERS, 100).

%% services := registered protocols, key :: proto_id()
-record(state, {dispatch_addr = {"localhost", 9000} :: ip_addr(),
                services = orddict:new() :: orddict:orddict(),
                dispatcher_pid = undefined :: pid()
               }).

-export([start_link/0, start_link/1,
         register_service/2,
         unregister_service/1,
         is_registered/1,
         stop/0
         ]).

%% ranch callbacks
-export([start_link/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal
-export([dispatch_service/4]).

%%%===================================================================
%%% API
%%%===================================================================

%% start the Service Manager on the default/configured Ip Address and Port.
%% All sub-protocols will be dispatched from there.
-spec(start_link() -> {ok, pid()}).
start_link() ->
    %% TODO: get this from the configuration env
    ServiceAddr = ?CLUSTER_MGR_SERVICE_ADDR,
    start_link(ServiceAddr).

%% start the Service Manager on the given Ip Address and Port.
%% All sub-protocols will be dispatched from there.
-spec(start_link(ip_addr()) -> {ok, pid()}).
start_link({IP,Port}) ->
    ?TRACE(?debugFmt("Starting Core Service Manager at ~p", [{IP,Port}])),
    lager:info("Starting Core Service Manager at ~p", [{IP,Port}]),
    Args = [{IP,Port}],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% Once a protocol specification is registered, it will be kept available by the
%% Service Manager.
-spec(register_service(hostspec(), service_scheduler_strategy()) -> ok).
register_service(HostProtocol, Strategy) ->
    %% only one strategy is supported as yet
    {round_robin, _NB} = Strategy,
    gen_server:cast(?SERVER, {register_service, HostProtocol, Strategy}).

%% Unregister the given protocol-id.
%% Existing connections for this protocol are not killed. New connections
%% for this protocol will not be accepted until re-registered.
-spec(unregister_service(proto_id()) -> ok).
unregister_service(ProtocolId) ->
    gen_server:cast(?SERVER, {unregister_service, ProtocolId}).

-spec(is_registered(proto_id()) -> boolean()).
is_registered(ProtocolId) ->
    gen_server:call(?SERVER, {is_registered, service, ProtocolId}).

%% abrubtly kill all connections and stop disptaching services
stop() ->
    gen_server:call(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([IpAddr]) ->
    {ok, Pid} = start_dispatcher(IpAddr, ?MAX_LISTENERS, []),
    {ok, #state{dispatch_addr = IpAddr, dispatcher_pid=Pid}}.

handle_call({is_registered, service, ProtocolId}, _From, State) ->
    Found = orddict:is_key(ProtocolId, State#state.services),
    {reply, Found, State};

handle_call(get_services, _From, State) ->
    {reply, orddict:to_list(State#state.services), State};

handle_call(stop, _From, State) ->
    ranch:stop_listener(State#state.dispatch_addr),
    {stop, normal, ok, State};

handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.

handle_cast({register_service, Protocol, Strategy}, State) ->
    {{ProtocolId,_Revs},_Rest} = Protocol,
    NewDict = orddict:store(ProtocolId, {Protocol, Strategy}, State#state.services),
    {noreply, State#state{services=NewDict}};
 
handle_cast({unregister_service, ProtocolId}, State) ->
    NewDict = orddict:erase(ProtocolId, State#state.services),
    {noreply, State#state{services=NewDict}};

handle_cast(_Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [_Unhandled])),
    {error, unhandled}. %% this will crash the server

handle_info(_Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [_Unhandled])),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% Host callback function, called by ranch for each accepted connection by way of
%% of the ranch:start_listener() call above, specifying this module.
start_link(Listener, Socket, Transport, SubProtocols) ->
    ?TRACE(?debugMsg("Start_link dispatch_service")),
    {ok, spawn_link(?MODULE, dispatch_service, [Listener, Socket, Transport, SubProtocols])}.

%% Body of the main dispatch loop. This is instantiated once for each connection
%% we accept because it transforms itself into the SubProtocol once it receives
%% the sub protocol and version, negotiated with the client.
dispatch_service(Listener, Socket, Transport, _Args) ->
    %% tell ranch "we've got it. thanks pardner"
    ok = ranch:accept_ack(Listener),
    %% set some starting options for the channel; these should match the client
    ?TRACE(?debugFmt("setting system options on service side: ~p", [?CONNECT_OPTIONS])),
    ok = Transport:setopts(Socket, ?CONNECT_OPTIONS),
    ok = exchange_handshakes_with(client, Socket, Transport),
    %% get latest set of registered services from gen_server and do negotiation
    Services = gen_server:call(?SERVER, get_services),
    SubProtocols = [Protocol || {_Key,{Protocol,_Strategy}} <- Services],
    ?TRACE(?debugFmt("started dispatch_service with protocols: ~p",
                     [SubProtocols])),
    Negotiated = negotiate_proto_with_client(Socket, Transport, SubProtocols),
    ?TRACE(?debugFmt("negotiated = ~p", [Negotiated])),
    start_negotiated_service(Socket, Transport, Negotiated).

%% start user's module:function and transfer socket to it's process.
start_negotiated_service(_Socket, _Transport, {error, Reason}) ->
    ?TRACE(?debugFmt("service dispatch failed with ~p", [{error, Reason}])),
    lager:error("service dispatch failed with ~p", [{error, Reason}]),
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
        Error ->
            ?TRACE(?debugFmt("service dispatch of ~p:~p failed with ~p",
                             [Module, Function, Error])),
            lager:error("service dispatch of ~p:~p failed with ~p",
                        [Module, Function, Error]),
            Error
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

choose_version({ClientProto,ClientVersions}=_CProtocol, HostProtocols) ->
    ?TRACE(?debugFmt("choose_version: client proto = ~p, HostProtocols = ~p",
                     [_CProtocol, HostProtocols])),
    %% first, see if the host supports the subprotocol
    case [H || {{HostProto,_Versions},_Rest}=H <- HostProtocols, ClientProto == HostProto] of
        [] ->
            %% oops! The host does not support this sub protocol type
            lager:error("Failed to find host support for protocol: ~p", [ClientProto]),
            ?TRACE(?debugMsg("choose_version: no common protocols")),
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
    end.

%% Returns true if the IP address given is a valid host IP address.
%% stolen from riak_repl_util.erl
valid_host_ip("0.0.0.0") ->
    true;
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

%% @doc Start the connection dispatcher with a limit of MaxListeners
%% listener connections and supported sub-protocols. When a connection
%% request arrives, it is mapped via the associated Protocol atom to an
%% acceptor function called as Module:Function(Listener, Socket, Transport, Args),
%% which must create it's own process and return {ok, pid()}

-spec(start_dispatcher(ip_addr(), non_neg_integer(), [hostspec()]) -> {ok, pid()}).
start_dispatcher({IP,Port}, MaxListeners, SubProtocols) ->
    case valid_host_ip(IP) of
        true ->
            {ok, RawAddress} = inet_parse:address(IP),
            {ok, Pid} = ranch:start_listener({IP,Port}, MaxListeners, ranch_tcp,
                                      [{ip, RawAddress}, {port, Port}],
                                      ?MODULE, SubProtocols),
            lager:info("Service manager: listening on ~s:~p", [IP, Port]),
            {ok, Pid};
        _ ->
            lager:error("Service Mananger: failed to start on ~s:~p - invalid address.",
                        [IP, Port])
    end.
