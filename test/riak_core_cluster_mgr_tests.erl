%% Eunit test cases for the Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_cluster_mgr_tests).

-include("riak_core_connection.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).

%% internal functions
-export([ctrlService/5, ctrlServiceProcess/5]).

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

running_test_() ->
    {setup, fun start_link_setup/0, fun(_) -> cleanup() end, fun(_) -> [

        {"is leader", ?_assert(riak_core_cluster_mgr:get_is_leader() == false)},

        {"become leader", fun() ->
            become_leader(),
            ?assert(node() == riak_core_cluster_mgr:get_leader()),
            ?assert(riak_core_cluster_mgr:get_is_leader() == true)
        end},

        {"no leader, become proxy", fun() ->
            riak_core_cluster_mgr:set_leader(undefined, self()),
            ?assert(riak_core_cluster_mgr:get_is_leader() == false)
        end},

        {"register member fun", fun() ->
            MemberFun = fun(_Addr) -> ?REMOTE_MEMBERS end,
            riak_core_cluster_mgr:register_member_fun(MemberFun),
            Members = gen_server:call(?CLUSTER_MANAGER_SERVER, {get_my_members, ?MY_CLUSTER_ADDR}),
            ?assert(Members == ?REMOTE_MEMBERS)
        end},

        {"register save cluster members", fun() ->
            Fun = fun(_C,_M) -> ok end,
            riak_core_cluster_mgr:register_save_cluster_members_fun(Fun)
        end},

        {"regsiter restor cluster members fun", fun() ->
            Fun = fun() -> [{test_name_locator,?REMOTE_CLUSTER_ADDR}] end,
            riak_core_cluster_mgr:register_restore_cluster_targets_fun(Fun),
            ok
        end},

        {"get known clusters when empty", fun() ->
            Clusters = riak_core_cluster_mgr:get_known_clusters(),
            ?debugFmt("get_known_clusters_when_empty_test(): ~p", [Clusters]),
            ?assert({ok,[]} == Clusters)
        end},

        {"get ipaddrs of cluster with unknown name", ?_assert({ok,[]} == riak_core_cluster_mgr:get_ipaddrs_of_cluster("unknown"))},

        {"add remote cluster multiple times but can still resolve", fun() ->
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters())
        end},

        {"add remote while leader", fun() ->
            ?assert(riak_core_cluster_mgr:get_is_leader() == false),
            become_leader(),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters()),
            riak_core_cluster_mgr:add_remote_cluster(?REMOTE_CLUSTER_ADDR),
            ?assert({ok,[]} == riak_core_cluster_mgr:get_known_clusters())
        end},

        {"connect to remote cluster", fun() ->
            start_fake_remote_cluster_service(),
            become_leader(),
            timer:sleep(2000),
            %% should have resolved the remote cluster by now
            ?assert({ok,[?REMOTE_CLUSTER_NAME]} == riak_core_cluster_mgr:get_known_clusters())
        end},

        {"get ipaddres of cluster", fun() ->
            Original = [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}],
            Rotated1 = [{"127.0.0.1",5002}, {"127.0.0.1",5003}, {"127.0.0.1",5001}],
            Rotated2 = [{"127.0.0.1",5003}, {"127.0.0.1",5001}, {"127.0.0.1",5002}],
            ?assert({ok,Original} == riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME)),
            ?assert({ok,Rotated1} == riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME)),
            ?assert({ok,Rotated2} == riak_core_cluster_mgr:get_ipaddrs_of_cluster(?REMOTE_CLUSTER_NAME))
        end}

    ] end }.

%% this test runs first and leaves the server running for other tests
%start_link_test() ->
start_link_setup() ->
    %% need to start it here so that a supervision tree will be created.
    ok = application:start(ranch),
    %% we also need to start the other connection servers
    {ok, _Pid1} = riak_core_service_mgr:start_link(?MY_CLUSTER_ADDR),
    {ok, _Pid2} = riak_core_connection_mgr:start_link(),
    {ok, Pid3} = riak_core_cluster_conn_sup:start_link(),
    unlink(Pid3),
    %% now start cluster manager
    {ok, _Pid4 } = riak_core_cluster_mgr:start_link().


%cleanup_test() ->
cleanup() ->
    riak_core_service_mgr:stop(),
    riak_core_connection_mgr:stop(),
    %% tough to stop a supervisor
    case whereis(riak_core_cluster_conn_sup) of
        undefined -> ok;
        Sup -> exit(Sup, kill)
    end,
    riak_core_cluster_mgr:stop(),
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

become_leader() ->
    riak_core_cluster_mgr:set_leader(node(), self()).

%%-----------------------------------
%% control channel services EMULATION
%%-----------------------------------

%% Note: this service module matches on test_cluster_mgr, so it will fail with
%% a function_clause error if the cluster manager isn't using our special test
%% protocol-id. Of course, it did once or I wouldn't have written this note :-)

ctrlService(_Socket, _Transport, {error, Reason}, _Args, _Props) ->
    ?TRACE(?debugFmt("Failed to accept control channel connection: ~p", [Reason]));
ctrlService(Socket, Transport, {ok, {test_cluster_mgr, MyVer, RemoteVer}}, Args, Props) ->
    RemoteClusterName = proplists:get_value(clustername, Props),
    ?TRACE(?debugFmt("ctrlService: received connection from cluster: ~p", [RemoteClusterName])),
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlServiceProcess,
                              [Socket, Transport, MyVer, RemoteVer, Args]),
    Transport:controlling_process(Socket, Pid),
    {ok, Pid}.

read_ip_address(Socket, Transport, Remote) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinAddr} ->
            MyAddr = binary_to_term(BinAddr),
            ?TRACE(?debugFmt("Cluster Manager: remote thinks my addr is ~p", [MyAddr])),
            lager:info("Cluster Manager: remote thinks my addr is ~p", [MyAddr]),
            MyAddr;
        Error ->
            lager:error("Cluster mgr: failed to receive ip addr from remote ~p: ~p",
                        [Remote, Error]),
            undefined
    end.

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
            MyAddr = read_ip_address(Socket, Transport, ?MY_CLUSTER_ADDR),
            ?TRACE(?debugFmt("  client thinks my Addr is ~p", [MyAddr])),
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
            {error, bad_cluster_mgr_message}
    end.
