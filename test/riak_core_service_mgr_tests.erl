%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_service_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

%% internal functions
-export([testService/5,
         connected/6, connect_failed/3
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
    ok = application:start(ranch),
    {Ok, _Pid} = riak_core_service_mgr:start_link(?TEST_ADDR),
    ?assert(Ok == ok).

get_services_test() ->
    Services = gen_server:call(riak_core_service_manager, get_services),
    ?assert([] == Services).

%% register a service and confirm added
register_service_test() ->
    ExpectedRevs = [{1,0}, {1,0}],
    TestProtocol = {{testproto, [{1,0}]}, {?TCP_OPTIONS, ?MODULE, testService, ExpectedRevs}},
    riak_core_service_mgr:register_service(TestProtocol, {round_robin,?MAX_CONS}),
    ?assert(riak_core_service_mgr:is_registered(testproto) == true).

%% unregister and confirm removed
unregister_service_test() ->
    TestProtocolId = testproto,
    riak_core_service_mgr:unregister_service(TestProtocolId),
    ?assert(riak_core_service_mgr:is_registered(testproto) == false).

register_stats_fun_test() ->
    Fun = fun(Stats) ->
                  ?assert(Stats == [{testproto,0}]) end,
    riak_core_service_mgr:register_stats_fun(Fun).

%% start a service via normal sequence
start_service_test() ->
    %% re-register the test protocol and confirm registered
    register_service_test(),
    %% try to connect via a client that speaks our test protocol
    ExpectedRevs = {expectedToPass, [{1,0}, {1,0}]},
    riak_core_connection:connect(?TEST_ADDR, {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedRevs}}),
    %% allow client and server to connect and make assertions of success/failure
    timer:sleep(1000),
    Stats = riak_core_service_mgr:get_stats(),
    ?assert(Stats == [{testproto,0}]).

pause_existing_services_test() ->
    riak_core_service_mgr:stop(),
    %% there should be no services running now.
    %% now start a client and confirm failure to connect
    ExpectedArgs = expectedToFail,
    riak_core_connection:connect(?TEST_ADDR, {{testproto, [{1,0}]},
                                              {?TCP_OPTIONS, ?MODULE, ExpectedArgs}}),
    %% allow client and server to connect and make assertions of success/failure
    timer:sleep(1000).

cleanup_test() ->
    %riak_core_service_mgr:stop(),
    application:stop(ranch).

%%------------------------
%% Helper functions
%%------------------------

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args, _Props) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args, _Props) ->
    ?TRACE(?debugMsg("testService started")),
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == testproto),
%%    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args, _Props) ->
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
