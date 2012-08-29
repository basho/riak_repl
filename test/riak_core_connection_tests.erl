-module(riak_core_connection_tests).

-include_lib("eunit/include/eunit.hrl").

-export([test1service/4, connected/5, connect_failed/3]).

%% host service functions
test1service(_Socket, _Transport, {error, Reason}, Args) ->
    ?assert(Args == failed_host_args),
    ?assert(Reason == protocol_version_not_supported);
test1service(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == test1proto),
    timer:sleep(2000),
    {ok, self()}.

%% client connection callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(Proto == test1proto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({Proto,_Vers}, {error, Reason}, Args) ->
    ?assert(Args == failed_client_args),
    ?assert(Reason == protocol_version_not_supported),
    ?assert(Proto == test1protoFailed).


protocol_match_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    Port = 10365,
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{test1proto, [{2,1}, {1,0}]}, ?MODULE, test1service, [{1,0}, {1,1}]}],
    riak_core_connection:start_dispatcher({IP,Port}, MaxListeners, TcpOptions, SubProtocols),

    %% try to connect via a client that speaks 0.1 and 1.1
    ClientProtocol = {test1proto, [{0,1},{1,1}]},
    riak_core_connection:connect({IP,Port}, ClientProtocol, TcpOptions, {?MODULE, [{1,1},{1,0}]}),

    timer:sleep(2000),
    application:stop(ranch),
    ok.

%% test that a mismatch of client and host args will notify both host and client
%% of a failed negotiation.
failed_protocol_match_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    Port = 10364,
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{test1protoFailed, [{2,1}, {1,0}]}, ?MODULE, test1service, [failed_host_args]}],
    riak_core_connection:start_dispatcher({IP,Port}, MaxListeners, TcpOptions, SubProtocols),

    %% try to connect via a client that speaks 0.1 and 3.1. No Match with host!
    ClientProtocol = {test1protoFailed, [{0,1},{3,1}]},
    riak_core_connection:connect({IP,Port}, ClientProtocol, TcpOptions, {?MODULE, failed_client_args}),

    timer:sleep(2000),
    application:stop(ranch),
    ok.
