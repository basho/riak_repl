%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_connection_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

%% internal functions
-export([testService/4,
         connected/5, connect_failed/3
        ]).

%% my name and remote same because I need to talk to myself for testing
-define(MY_CLUSTER_NAME, "bob").
-define(REMOTE_CLUSTER_NAME, "bob").

-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4096}).
-define(TEST_ADDR, {"127.0.0.1", 4097}).
-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% normally, ranch would be started as part of a supervisor tree, but we
    %% need to start it here so that a supervision tree will be created.
    application:start(ranch),
    {Ok, _Pid} = riak_core_connection_mgr:start_link(),
    ?assert(Ok == ok).

set_peers_test() ->
    PeerNodeAddrs = [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}],
    riak_core_connection_mgr:set_peers(PeerNodeAddrs),
    ?assert(PeerNodeAddrs == riak_core_connection_mgr:get_peers()).

%% conn_mgr should start up paused
is_paused_test() ->
    ?assert(riak_core_connection_mgr:is_paused() == true).

%% resume and check that it's not paused
resume_test() ->
    riak_core_connection_mgr:resume(),
    ?assert(riak_core_connection_mgr:is_paused() == false).

%% pause and check that it's paused
pause_test() ->
    riak_core_connection_mgr:pause(),
    ?assert(riak_core_connection_mgr:is_paused() == true).

%% set/get the cluster manager finding function
set_get_finder_function_test() ->
    FinderFun = fun() -> {ok, node()} end,
    riak_core_connection_mgr:set_cluster_finder(FinderFun),
    FoundFun = riak_core_connection_mgr:get_cluster_finder(),
    ?assert(FinderFun == FoundFun).

start_cluster_manager_test() ->
    %% start a local cluster manager
    riak_core_cluster_mgr:start_link(),
    riak_core_cluster_mgr:set_name(?MY_CLUSTER_NAME),
    %% verify it's running
    ?assert(?MY_CLUSTER_NAME == riak_core_cluster_mgr:get_name()).

start_service() ->
    %% start dispatcher
    ExpectedRevs = [{1,0}, {1,0}],
    TestProtocol = {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, testService, ExpectedRevs}},
    MaxListeners = 10,
    riak_core_connection:start_dispatcher(?TEST_ADDR, MaxListeners, [TestProtocol]).

client_connection_test() ->
    %% start a test service
    start_service(),
    %% do async connect via connection_mgr
    ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
    riak_core_connection_mgr:connect({addr, ?TEST_ADDR},
                               {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, ExpectedArgs}}),
    timer:sleep(1000).

client_connect_via_cluster_name_test() ->
    start_service(),
    %% do async connect via connection_mgr
    ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
    riak_core_connection_mgr:connect({name, ?REMOTE_CLUSTER_NAME},
                               {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, ExpectedArgs}}),
    timer:sleep(1000).

client_retries_test() ->
    ?TRACE(?debugMsg(" --------------- retry test ------------- ")),
    %% start the service a while after the client has been started so the client
    %% will do retries.

    %% do async connect via connection_mgr
    ExpectedArgs = {retry_test, [{1,0}, {1,0}]},
    riak_core_connection_mgr:connect({addr, ?TEST_ADDR},
                                     {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, ExpectedArgs}}),
    %% delay so the client will keep trying
    ?TRACE(?debugMsg(" ------ sleeping 3 sec")),
    timer:sleep(3000),
    %% resume and confirm not paused, which should cause service to start and connection :-)
    ?TRACE(?debugMsg(" ------ resuming services")),
    start_service(),
    %% allow connection to setup
    ?TRACE(?debugMsg(" ------ sleeping 2 sec")),
    timer:sleep(1000).
    

cleanup_test() ->
    application:stop(ranch).

%%------------------------
%% Helper functions
%%------------------------

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args) ->
    ?TRACE(?debugMsg("testService started")),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == testproto),
    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args) ->
    ?TRACE(?debugMsg("testClient started")),
    {_TestType, [ExpectedMyVer, ExpectedRemoteVer]} = Args,
    ?assert(Proto == testproto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({_Proto,_Vers}, {error, Reason}, Args) ->
    case Args of
        expectedToFail ->
            ?assert(Reason == econnrefused);
        {retry_test, _Stuff} ->
            ?TRACE(?debugFmt("connect_failed: during retry test: ~p", [Reason])),
            ok;
        _Other ->
            ?TRACE(?debugFmt("connect_failed: ~p with args = ~p", [Reason, _Other])),
            ?assert(false)
    end,
    timer:sleep(1000).
