%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_conn_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

%% internal functions
-export([testService/4
        ]).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    {ok, Pid} = riak_core_conn_mgr:start_link(),
    ?debugFmt("started connection manager, Pid = ~p", [Pid]).

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
    FinderFun = fun() -> {ok, {"localhost",4092}} end,
    riak_core_conn_mgr:set_cluster_finder(FinderFun),
    FoundFun = riak_core_conn_mgr:get_cluster_finder(),
    ?assert(FinderFun == FoundFun).

%% register a service
register_protocol_test() ->
    ExpectedRevs = [{1,0}, {1,0}],
    TestProtocol = {{testproto, [{1,0}]}, ?MODULE, testService, ExpectedRevs},
    riak_core_conn_mgr:register_protocol(TestProtocol),
    ?assert(riak_core_conn_mgr:is_registered(testproto) == true).

unregister_protocol_id_test() ->
    TestProtocolId = testproto,
    riak_core_conn_mgr:unregister_protocol_id(TestProtocolId),
    ?assert(riak_core_conn_mgr:is_registered(testproto) == false).

%%------------------------
%% Helper functions
%%------------------------

%% Protocol Service functions
testService(_Socket, _Transport, {error, _Reason}, _Args) ->
    ?assert(false);
testService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == testproto),
    timer:sleep(2000),
    {ok, self()}.
