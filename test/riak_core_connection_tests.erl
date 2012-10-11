-module(riak_core_connection_tests).

-include_lib("eunit/include/eunit.hrl").

-export([test1service/5, connected/6, connect_failed/3]).

-define(TEST_ADDR, { "127.0.0.1", 4097}).
-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

%% host service functions
test1service(_Socket, _Transport, {error, Reason}, Args, _Props) ->
    ?debugFmt("test1service failed with {error, ~p}", [Reason]),
    ?assert(Args == failed_host_args),
    ?assert(Reason == protocol_version_not_supported),
    {error, Reason};
test1service(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args, Props) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    RemoteClusterName = proplists:get_value(clustername, Props),
    ?debugFmt("test1service started with Args ~p Props ~p", [Args, Props]),
    ?assert(RemoteClusterName == "undefined"),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == test1proto),
    timer:sleep(2000),
    {ok, self()}.

%% client connection callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args, Props) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    RemoteClusterName = proplists:get_value(clustername, Props),
    ?debugFmt("connected with Args ~p Props ~p", [Args, Props]),
    ?assert(RemoteClusterName == "undefined"),
    ?assert(Proto == test1proto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({Proto,_Vers}, {error, Reason}, Args) ->
    ?debugFmt("connect_failed: Reason = ~p Args = ~p", [Reason, Args]),
    ?assert(Args == failed_client_args),
    ?assert(Reason == protocol_version_not_supported),
    ?assert(Proto == test1protoFailed).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% normally, ranch would be started as part of a supervisor tree, but we
    %% need to start it here so that a supervision tree will be created.
    ok = application:start(ranch),
    {Ok, _Pid} = riak_core_service_mgr:start_link(?TEST_ADDR),
    ?assert(Ok == ok).

%% set/get the local cluster's name
set_get_name_test() ->
    riak_core_connection:set_symbolic_clustername("undefined"),
    MyName = riak_core_connection:symbolic_clustername(),
    ?assert("undefined" == MyName).

%% register a service and confirm added
register_service_test() ->
    ExpectedRevs = [{1,0}, {1,1}],
    ServiceProto = {test1proto, [{2,1}, {1,0}]},
    ServiceSpec = {ServiceProto, {?TCP_OPTIONS, ?MODULE, test1service, ExpectedRevs}},
    riak_core_service_mgr:register_service(ServiceSpec, {round_robin,?MAX_CONS}),
    ?assert(riak_core_service_mgr:is_registered(test1proto) == true).

%% unregister and confirm removed
unregister_service_test() ->
    TestProtocolId = test1proto,
    riak_core_service_mgr:unregister_service(TestProtocolId),
    ?assert(riak_core_service_mgr:is_registered(TestProtocolId) == false).

protocol_match_test() ->
    %% re-register the test protocol and confirm registered
    register_service_test(),
    %% try to connect via a client that speaks 0.1 and 1.1
    ClientProtocol = {test1proto, [{0,1},{1,1}]},
    ClientSpec = {ClientProtocol, {?TCP_OPTIONS, ?MODULE, [{1,1},{1,0}]}},
    riak_core_connection:connect(?TEST_ADDR, ClientSpec),

    timer:sleep(1000).

%% test that a mismatch of client and host args will notify both host and client
%% of a failed negotiation.
failed_protocol_match_test() ->
    %% start service
    SubProtocol = {{test1protoFailed, [{2,1}, {1,0}]},
                   {?TCP_OPTIONS, ?MODULE, test1service, failed_host_args}},
    riak_core_service_mgr:register_service(SubProtocol, {round_robin,?MAX_CONS}),
    ?assert(riak_core_service_mgr:is_registered(test1protoFailed) == true),

    %% try to connect via a client that speaks 0.1 and 3.1. No Match with host!
    ClientProtocol = {test1protoFailed, [{0,1},{3,1}]},
    ClientSpec = {ClientProtocol, {?TCP_OPTIONS, ?MODULE, failed_client_args}},
    riak_core_connection:connect(?TEST_ADDR, ClientSpec),

    timer:sleep(2000),
    ok.

cleanup_test() ->
    riak_core_service_mgr:stop(),
    application:stop(ranch).
