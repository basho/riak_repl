%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_conn_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

%% internal functions
-export([testService/4,
         connected/5, connect_failed/3
        ]).

-define(TEST_ADDR, {"127.0.0.1", 4097}).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% normally, ranch would be started as part of a supervisor tree, but we
    %% need to start it here so that a supervision tree will be created.
    application:start(ranch),
    {Ok, _Pid} = riak_core_conn_mgr:start_link(?TEST_ADDR),
    ?assert(Ok == ok).

%% conn_mgr should start up paused
is_paused_test() ->
    ?assert(riak_core_conn_mgr:is_paused() == true).

%% resume and check that it's not paused
resume_test() ->
    riak_core_conn_mgr:resume(),
    ?assert(riak_core_conn_mgr:is_paused() == false).

%% pause and check that it's paused
pause_test() ->
    riak_core_conn_mgr:pause(),
    ?assert(riak_core_conn_mgr:is_paused() == true).

%% set/get the cluster manager finding function
set_get_finder_function_test() ->
    FinderFun = fun() -> {ok, ?TEST_ADDR} end,
    riak_core_conn_mgr:set_cluster_finder(FinderFun),
    FoundFun = riak_core_conn_mgr:get_cluster_finder(),
    ?assert(FinderFun == FoundFun).

%% register a service and confirm added
register_protocol_test() ->
    ExpectedRevs = [{1,0}, {1,0}],
    TestProtocol = {{testproto, [{1,0}]}, ?MODULE, testService, ExpectedRevs},
    riak_core_conn_mgr:register_protocol(TestProtocol),
    ?assert(riak_core_conn_mgr:is_registered(testproto) == true).

%% unregister and confirm removed
unregister_protocol_id_test() ->
    TestProtocolId = testproto,
    riak_core_conn_mgr:unregister_protocol_id(TestProtocolId),
    ?assert(riak_core_conn_mgr:is_registered(testproto) == false).

%% start a service via normal sequence
start_service_test() ->
    %% pause and confirm paused
    pause_test(),
    %% re-register the test protocol and confirm registered
    register_protocol_test(),
    %% resume and confirm not paused, which should cause service to start
    resume_test(),
    %% try to connect via a client that speaks our test protocol
    ClientProtocol = {testproto, [{1,0}]},
    ExpectedRevs = [{1,0}, {1,0}],
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    riak_repl2_connection:connect(?TEST_ADDR, ClientProtocol, TcpOptions, {?MODULE, ExpectedRevs}),
    %% allow client and server to connect and make assertions of success/failure
    timer:sleep(4000).

cleanup_test() ->
    application:stop(ranch).

%%------------------------
%% Helper functions
%%------------------------

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args) ->
    ?debugMsg("testService started"),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == testproto),
    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args) ->
    ?debugMsg("testClient connected"),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(Proto == testproto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({_Proto,_Vers}, {error, _Reason}, _Args) ->
    ?assert(false).
