%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_cluster_mgr_tests).

-include("riak_core_connection.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% internal functions
-export([ctrlService/4, ctrlServiceProcess/5]).

%% For testing, both clusters have to look like they are on the same machine
%% and both have the same port number for offered sub-protocols. "my cluster"
%% is using the standard "cluster_mgr" protocol id, while "remote cluster" is
%% using a special test protocol "test_cluster_mgr" to allow us to fake out
%% the service manager and let two cluster managers run on the same IP and Port.

%% My cluster
-define(MY_CLUSTER_NAME, "bob").
-define(MY_CLUSTER_ADDR, {"127.0.0.1", 4097}).

%% Remote cluster
-define(REMOTE_CLUSTER_NAME, "betty").
-define(REMOTE_CLUSTER_ADDR, {"127.0.0.1", 4097}).
-define(REMOTE_MEMBERS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).

%% this test runs first and leaves the server running for other tests
start_link_test() ->
    %% need to start it here so that a supervision tree will be created.
    application:start(ranch),
    %% we also need to start the other connection servers
    {ok, _Pid1} = riak_core_service_mgr:start_link(?MY_CLUSTER_ADDR),
    {ok, _Pid2} = riak_core_connection_mgr:start_link(),
    {ok, _Pid3} = riak_core_cluster_conn_sup:start_link(),
    %% now start cluster manager
    {ok, _Pid4 } = riak_core_cluster_mgr:start_link().

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
    riak_core_cluster_mgr:set_leader(node()),
    ?assert(node() == riak_core_cluster_mgr:get_leader()),
    ?assert(riak_core_cluster_mgr:get_is_leader() == true).

%% become a proxy
no_leader_test() ->
    riak_core_cluster_mgr:set_leader(undefined),
    ?assert(riak_core_cluster_mgr:get_is_leader() == false).

register_member_fun_test() ->
    MemberFun = fun() -> ?REMOTE_MEMBERS end,
    riak_core_cluster_mgr:register_member_fun(MemberFun),
    Members = gen_server:call(?CLUSTER_MANAGER_SERVER, get_my_members),
    ?assert(Members == ?REMOTE_MEMBERS).

register_sites_fun_test() ->
    SitesFun = fun() -> [?REMOTE_CLUSTER_ADDR] end,
    riak_core_cluster_mgr:register_sites_fun(SitesFun),
    ok.

get_known_clusters_when_empty_test() ->
    Clusters = riak_core_cluster_mgr:get_known_clusters(),
    ?debugFmt("get_known_clusters_when_empty_test(): ~p", [Clusters]),
    ?assert({ok,[]} == Clusters).

get_ipaddrs_of_cluster_unknown_name_test() ->
    ?assert({ok,[]} == riak_core_cluster_mgr:get_ipaddrs_of_cluster("unknown")).

add_remote_cluster_multiple_times_cant_resolve_test() ->
    ?debugMsg("------- add_remote_cluster_multiple_times_cant_resolve_test ---------"),
    %% adding multiple times should not cause multiple entries in unresolved list
    riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
    ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()),
    riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
    ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()).

add_remotes_while_leader_test() ->
    ?debugMsg("------- add_remotes_while_leader_test ---------"),
    ?assert(riak_core_cluster_mgr:get_is_leader() == false),
    leader_test(),
    add_remote_cluster_multiple_times_cant_resolve_test().

connect_to_remote_cluster_test() ->
    ?debugMsg("------- connect_to_remote_cluster_test ---------"),
    start_fake_remote_cluster_service(),
    leader_test(),
    timer:sleep(2000),
    %% should have resolved the remote cluster by now
    ?assert({ok,[?REMOTE_CLUSTER_NAME]} == riak_core_cluster_mgr:get_known_clusters()),
    ?assert({ok,?REMOTE_MEMBERS} == riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME)).

cleanup_test() ->
    application:stop(ranch).

%%--------------------------
%% helper functions
%%--------------------------

start_fake_remote_cluster_service() ->
    %% start our cluster_mgr service under a different protocol id,
    %% which the cluster manager will use during testing to connect to us.
    ServiceProto = {test_cluster_mgr, [{1,0}]},
    ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, ?MODULE, ctrlService, []}},
    riak_core_service_mgr:register_service(ServiceSpec, {round_robin,10}).

%%-----------------------------------
%% control channel services EMULATION
%%-----------------------------------

%% Note: this service module matches on test_cluster_mgr, so it will fail with
%% a function_clause error if the cluster manager isn't using our special test
%% protocol-id. Of course, it did once or I wouldn't have written this note :-)

ctrlService(_Socket, _Transport, {error, Reason}, _Args) ->
    ?TRACE(?debugFmt("Failed to accept control channel connection: ~p", [Reason]));
ctrlService(Socket, Transport, {ok, {test_cluster_mgr, MyVer, RemoteVer}}, Args) ->
    ?TRACE(?debugMsg("ctrlService: spawning service process...")),
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlServiceProcess,
                              [Socket, Transport, MyVer, RemoteVer, Args]),
    Transport:controlling_process(Socket, Pid),
    {ok, Pid}.

%% process instance for handling control channel requests from remote clusters.
ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args) ->
    case Transport:recv(Socket, 0, infinity) of
        {ok, ?CTRL_ASK_NAME} ->
            %% remote wants my name
            ?TRACE(?debugMsg("wants my name")),
            MyName = ?REMOTE_CLUSTER_NAME,
            Transport:send(Socket, term_to_binary(MyName)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {ok, ?CTRL_ASK_MEMBERS} ->
            ?TRACE(?debugMsg("wants my members")),
            %% remote wants list of member machines in my cluster
            Members = ?REMOTE_MEMBERS,
            Transport:send(Socket, term_to_binary(Members)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {error, Reason} ->
            ?debugFmt("Failed recv on control channel. Error = ~p", [Reason]),
            % nothing to do now but die
            {error, Reason};
        Other ->
            ?debugFmt("Recv'd unknown message on cluster control channel: ~p",
                      [Other]),
            % ignore and keep trying to be a nice service
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args)
    end.
