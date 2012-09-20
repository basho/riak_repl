%% Riak Core Cluster Manager Connections to Remote Clusters
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% Connections get started by who-ever and they report to the cluster manager.
%% Once an ip-address has been resolved to a cluster, the cluster manager
%% might remove the connection if it already has a connection to that cluster.

-module(riak_core_cluster_conn).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% For testing, we need to have two different cluster manager services running
%% on the same node, which is normally not done. The remote cluster service is
%% the one we're testing, so use a different protocol for the client connection
%% during eunit testing, which will emulate a cluster manager from the test.
-define(REMOTE_CLUSTER_PROTO_ID, test_cluster_mgr).
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(REMOTE_CLUSTER_PROTO_ID, ?CLUSTER_PROTO_ID).
-define(TRACE(Stmt),ok).
-endif.

-export([start_link/1]).

-export([connected/5, connect_failed/3, ctrlClientProcess/2]).

%%%===================================================================
%%% API
%%%===================================================================

%% start a connection with a locator type, either {cluster_by_name, clustername()}
%% or {cluster_by_addr, ip_addr()}. This is asynchronous. If it dies, the connection
%% supervisior will restart it.
-spec(start_link(term()) -> ok).
start_link(Remote) ->
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlClientProcess,
                              [Remote, unconnected]),
    {ok, Pid}.

%%%===================================================================
%% private
%%%===================================================================

%% request a connection from connection manager. When connected, our
%% module's "connected" function will be called, which sends a message
%% "connected_to_remote", which we forward to the cluster manager. Dying
%% is ok; we'll get restarted by a supervisor.
ctrlClientProcess(Remote, unconnected) ->
    ?TRACE(?debugFmt("cluster_conn: starting managed connection to ~p", [Remote])),
    Args = {Remote, self()},
    {ok,_Ref} = riak_core_connection_mgr:connect(
                  Remote,
                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,0}]},
                   {?CTRL_OPTIONS, ?MODULE, Args}},
                  default),
    %% wait a long-ish time for the connection to establish
    receive
        {_From, {connected_to_remote, Socket, Transport, Addr}} ->
            ?TRACE(?debugFmt("cluster_conn: connected_to_remote ~p at ~p",
                             [Remote, Addr])),
            %% when first connecting to a remote node, we ask it's
            %% and member list, even if it's a previously resolved
            %% cluster. Then we can sort everything out in the
            %% gen_server. If the name or members fails, these matches
            %% will fail and the connection will get restarted.
            {ok, Name} = ask_cluster_name(Socket, Transport, Remote),
            {ok, Members} = ask_member_ips(Socket, Transport, Remote),
            gen_server:cast(?CLUSTER_MANAGER_SERVER,
                            {connected_to_remote, Name, Members, Addr, Remote}),
            ctrlClientProcess(Remote, {Name, Socket, Transport});
        Other ->
            ?TRACE(?debugFmt("cluster_conn: unexpected recv from remote: ~p, ~p",
                             [Remote, Other])),
            lager:error("Unexpected recv from remote: ~p, ~p", [Remote, Other])
        after ?CONNECTION_SETUP_TIMEOUT ->
                %% die with error
                ?TRACE(?debugFmt("cluster_conn: timed out waiting for ~p", [Remote])),
                {error, timed_out_waiting_for_connection}
        end;
ctrlClientProcess(Remote, {Name, Socket, Transport}) ->
    %% trade our time between checking for updates from the remote cluster
    %% and commands from our local cluster manager.
    receive
        %% cluster manager asking us to poll the remove cluster
        {_From, poll_cluster} ->
            {ok, Members} = ask_member_ips(Socket, Transport, Remote),
            gen_server:cast(?CLUSTER_MANAGER_SERVER,
                            {cluster_updated, Name, Members, Remote})
    after 250 ->
          %% check for push notifications from remote cluster about member changes
              case Transport:recv(Socket, 0, 250) of
                  {ok, {cluster_members_changed, BinMembers}} ->
                      Members = {ok, binary_to_term(BinMembers)},
                      gen_server:cast(?CLUSTER_MANAGER_SERVER,
                                      {cluster_updated, Members, Remote});
                  {ok, Other} ->
                      lager:error("Unexpected recv from remote: ~p, ~p", [Remote, Other]);
                  {error, Reason} ->
                      lager:error("Error recv'ing from remote: ~p, ~p", [Remote, Reason])
              end
    end,
    ctrlClientProcess(Remote, {Name, Socket, Transport}).

ask_cluster_name(Socket, Transport, Remote) ->
    Transport:send(Socket, ?CTRL_ASK_NAME),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinName} ->
            {ok, binary_to_term(BinName)};
        Error ->
            lager:error("Failed to receive name from remote cluster at ~p because ~p",
                        [Remote, Error]),
            Error
    end.

ask_member_ips(Socket, Transport, Remote) ->
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinMembers} ->
            {ok, binary_to_term(BinMembers)};
        Error ->
            lager:error("Failed to receive members from remote cluster at ~p because ~p",
                        [Remote, Error]),
            Error
    end.

connected(Socket, Transport, Addr, {?REMOTE_CLUSTER_PROTO_ID, _MyVer, _RemoteVer}, {_Remote,Client}) ->
    ?TRACE(?debugFmt("cluster_conn: ~p connected to ~p", [Client, Addr])),
    %% give control over the socket to the Client process
    Transport:controlling_process(Socket, Client),
    Client ! {self(), {connected_to_remote, Socket, Transport, Addr}},
    ok.

connect_failed({_Proto,_Vers}, {error, _Reason}, _Args) ->
    ?TRACE(?debugFmt("cluster_conn: connect_failed to ~p because ~p",
                     [_Args, _Reason])),
    %% It's ok, the connection manager will keep trying.
    %% TODO: mark this addr/cluster as having connection issues.
    ok.
