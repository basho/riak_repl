%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_connection_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% internal functions
-export([testService/4,
         connected/5, connect_failed/3
        ]).

%% Locator selector types
-define(REMOTE_LOCATOR_TYPE, remote).
-define(ADDR_LOCATOR_TYPE, addr).

%% My cluster
-define(MY_CLUSTER_NAME, "bob").
-define(MY_CLUSTER_ADDR, {"127.0.0.1", 4097}).

%% Remote cluster
-define(REMOTE_CLUSTER_NAME, "betty").
-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4096}).
-define(REMOTE_ADDRS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003},
                       ?REMOTE_CLUSTER_ADDR]).

-define(MAX_CONS, 2).
-define(TCP_OPTIONS, [{keepalive, true},
                      {nodelay, true},
                      {packet, 4},
                      {reuseaddr, true},
                      {active, false}]).

%% Tell the connection manager how to find out "remote" end points.
%% For testing, we just make up some local addresses.
register_remote_locator() ->
    Remotes = orddict:from_list([{?REMOTE_CLUSTER_NAME, ?REMOTE_ADDRS}]),
    Locator = fun(Name, _Policy) ->
                      case orddict:find(Name, Remotes) of
                          false ->
                              {error, {unknown, Name}};
                          OKEndpoints ->
                              OKEndpoints
                      end
              end,
    ok = riak_core_connection_mgr:register_locator(?REMOTE_LOCATOR_TYPE, Locator).

register_addr_locator() ->
    Locator = fun(Name, _Policy) -> {ok, [Name]} end,
    ok = riak_core_connection_mgr:register_locator(?ADDR_LOCATOR_TYPE, Locator).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% normally, ranch would be started as part of a supervisor tree, but we
    %% need to start it here so that a supervision tree will be created.
    application:start(ranch),
    {Ok, _Pid} = riak_core_connection_mgr:start_link(),
    ?assert(Ok == ok).

register_locator_remote_test() ->
    register_remote_locator(),
    Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
    Strategy = default,
    case riak_core_connection_mgr:apply_locator(Target, Strategy) of
        {ok, Addrs} ->
            ?assert(Addrs == ?REMOTE_ADDRS);
        Error ->
            ?assert(ok == Error)
    end.

register_locator_addr_test() ->
    register_addr_locator(),
    Target = {?ADDR_LOCATOR_TYPE, ?REMOTE_CLUSTER_ADDR},
    Strategy = default,
    case riak_core_connection_mgr:apply_locator(Target, Strategy) of
        {ok, Addrs} ->
            ?assert(Addrs == [?REMOTE_CLUSTER_ADDR]);
        Error ->
            ?assert(ok == Error)
    end.

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
    riak_core_connection:start_dispatcher(?REMOTE_CLUSTER_ADDR, MaxListeners, [TestProtocol]).

client_connection_test() ->
    ?debugMsg("Starting client_connection_test"),
    %% start a test service
    start_service(),
    %% do async connect via connection_mgr
    ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
    Target = {?ADDR_LOCATOR_TYPE, ?REMOTE_CLUSTER_ADDR},
    Strategy = default,
    riak_core_connection_mgr:connect(Target,
                                     {{testproto, [{1,0}]},
                                      {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                     Strategy),
    timer:sleep(1000).

client_connect_via_cluster_name_test() ->
    start_service(),
    %% do async connect via connection_mgr
    ExpectedArgs = {expectedToPass, [{1,0}, {1,0}]},
    Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
    Strategy = default,
    riak_core_connection_mgr:connect(Target,
                                     {{testproto, [{1,0}]},
                                      {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                     Strategy),
    timer:sleep(1000).

client_retries_test() ->
    ?TRACE(?debugMsg(" --------------- retry test ------------- ")),
    %% start the service a while after the client has been started so the client
    %% will do retries.

    %% do async connect via connection_mgr
    ExpectedArgs = {retry_test, [{1,0}, {1,0}]},
    Target = {?REMOTE_LOCATOR_TYPE, ?REMOTE_CLUSTER_NAME},
    Strategy = default,

    riak_core_connection_mgr:connect(Target,
                                     {{testproto, [{1,0}]},
                                      {?TCP_OPTIONS, ?MODULE, ExpectedArgs}},
                                     Strategy),
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
        Other ->
            ?TRACE(?debugFmt("connect_failed: ~p with args = ~p", [Reason, Other])),
            ?assert(false == Other)
    end,
    timer:sleep(1000).
