%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_cluster_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% internal functions
-export([]).

%% My cluster
-define(MY_CLUSTER_NAME, "bob").
-define(MY_CLUSTER_ADDR, {"127.0.0.1", 4097}).

%% Remote cluster
-define(REMOTE_CLUSTER_NAME, "betty").
-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4096}).
-define(REMOTE_ADDRS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% need to start it here so that a supervision tree will be created.
    application:start(ranch),
    %% we also need to start the other connection servers
    {ok, _Pid1} = riak_core_service_mgr:start_link(?MY_CLUSTER_ADDR),
    {ok, _Pid2} = riak_core_connection_mgr:start_link(),
    %% now start cluster manager
    {ok, _Pid3 } = riak_core_cluster_mgr:start_link().

%% set/get the local cluster's name
set_get_name_test() ->
    riak_core_cluster_mgr:set_my_name(?MY_CLUSTER_NAME),
    MyName = riak_core_cluster_mgr:get_my_name(),
    ?assert(?MY_CLUSTER_NAME == MyName).

%% conn_mgr should start up not as the leader
is_leader_test() ->
    ?assert(riak_core_cluster_mgr:get_is_leader() == false).

%% become the leader
leader_test() ->
    riak_core_cluster_mgr:set_is_leader(true),
    ?assert(riak_core_cluster_mgr:get_is_leader() == true).

%% become a proxy
not_the_leader_test() ->
    riak_core_cluster_mgr:set_is_leader(false),
    ?assert(riak_core_cluster_mgr:get_is_leader() == false).

cleanup_test() ->
    application:stop(ranch).
