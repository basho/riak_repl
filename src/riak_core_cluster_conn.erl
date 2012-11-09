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

-export([connected/6, connect_failed/3, ctrlClientProcess/3]).

%%%===================================================================
%%% API
%%%===================================================================

%% start a connection with a locator type, either {cluster_by_name, clustername()}
%% or {cluster_by_addr, ip_addr()}. This is asynchronous. If it dies, the connection
%% supervisior will restart it.
-spec(start_link(term()) -> {ok,pid()}).
start_link(Remote) ->
    Members = [],
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlClientProcess,
                              [Remote, unconnected, Members]),
    {ok, Pid}.

%%%===================================================================
%% private
%%%===================================================================

%% request a connection from connection manager. When connected, our
%% module's "connected" function will be called, which sends a message
%% "connected_to_remote", which we forward to the cluster manager. Dying
%% is ok; we'll get restarted by a supervisor.
ctrlClientProcess(Remote, unconnected, Members0) ->
    Args = {Remote, self()},
    {ok,_Ref} = riak_core_connection_mgr:connect(
                  Remote,
                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,0}]},
                   {?CTRL_OPTIONS, ?MODULE, Args}},
                  default),
    ctrlClientProcess(Remote, connecting, Members0);
%% We're trying to connect via the connection manager now
ctrlClientProcess(Remote, connecting, Members0) ->
    %% wait a long-ish time for the connection to establish
    receive
        {From, status} ->
            %% someone wants our status. Don't do anything that blocks!
            From ! {self(), connecting, Remote},
            ctrlClientProcess(Remote, connecting, Members0);
        {_From, {connect_failed, Error}} ->
            lager:warning("ClusterManager Client: connect_failed to ~p because ~p. Will retry.",
                          [Remote, Error]),
            %% This is fatal! We are being supervised by conn_sup and if we
            %% die, it will restart us.
            {error, Error};
        {_From, {connected_to_remote, Socket, Transport, Addr, Props}} ->
            RemoteName = proplists:get_value(clustername, Props),
            lager:debug("Cluster Manager control channel client connected to remote ~p at ~p named ~p",
                       [Remote, Addr, RemoteName]),
            %% ask it's name and member list, even if it's a previously
            %% resolved cluster. Then we can sort everything out in the
            %% gen_server. If the name or members fails, these matches
            %% will fail and the connection will get restarted.
            case ask_cluster_name(Socket, Transport, Remote) of
                {ok, Name} ->
                    case ask_member_ips(Socket, Transport, Addr, Remote) of
                        {ok, Members} ->
                            %% This is the first time we're updating the cluster manager
                            %% with the name of this cluster, so it's old name is undefined.
                            OldName = "undefined",
                            gen_server:cast(?CLUSTER_MANAGER_SERVER,
                                            {cluster_updated, OldName, Name, Members, Addr, Remote}),
                            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members);
                        {error, closed} ->
                            {error, connection_closed};
                        Error ->
                            Error
                    end;
                {error, closed} ->
                    {error, connection_closed};
                Error ->
                    Error
            end;
        {_From, poll_cluster} ->
            %% cluster manager doesn't know we haven't connected yet.
            %% just ignore this while we're waiting to connect or fail
            ctrlClientProcess(Remote, connecting, Members0);
        Other ->
            lager:error("cluster_conn: client got unexpected msg from remote: ~p, ~p",
                        [Remote, Other]),
            ctrlClientProcess(Remote, connecting, Members0)
    after (?CONNECTION_SETUP_TIMEOUT + 5000) ->
            %% die with error once we've passed the timeout period that the
            %% core_connection module will expire. Go round and let the connection
            %% manager keep trying.
            lager:debug("cluster_conn: client timed out waiting for ~p", [Remote]),
            ctrlClientProcess(Remote, connecting, Members0)
    end;
ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members0) ->
    %% trade our time between checking for updates from the remote cluster
    %% and commands from our local cluster manager. TODO: what if the name
    %% of the remote cluster changes?
    receive
        %% cluster manager asking us to poll the remove cluster
        {_From, poll_cluster} ->
            case ask_cluster_name(Socket, Transport, Remote) of
                {ok, NewName} ->
                    case ask_member_ips(Socket, Transport, Addr, Remote) of
                        {ok, Members} ->
                            gen_server:cast(?CLUSTER_MANAGER_SERVER,
                                            {cluster_updated, Name, NewName, Members, Addr, Remote}),
                            ctrlClientProcess(Remote, {NewName, Socket, Transport, Addr}, Members);
                        {error, closed} ->
                            {error, connection_closed};
                        Error ->
                            Error
                    end;
                {error, closed} ->
                    {error, connection_closed};
                Error ->
                    Error
            end;
        %% request for our connection status
        {From, status} ->
            %% don't try talking to the remote cluster; we don't want to stall our status
            Status = {Addr, Transport, Name, Members0},
            From ! {self(), status, Status},
            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members0)
    after 1000 ->
            %% check for push notifications from remote cluster about member changes
            Members1 =
                case Transport:recv(Socket, 0, 250) of
                    {ok, {cluster_members_changed, BinMembers}} ->
                        Members = {ok, binary_to_term(BinMembers)},
                        gen_server:cast(?CLUSTER_MANAGER_SERVER,
                                        {cluster_updated, Name, Name, Members, Addr, Remote}),
                        Members;
                    {ok, Other} ->
                        lager:error("cluster_conn: client got unexpected msg from remote: ~p, ~p",
                                    [Remote, Other]),
                        Members0;
                    {error, timeout} ->
                        %% timeouts are ok; we'll just go round and try again
                        Members0;
                    {error, closed} ->
                        %%erlang:exit(connection_closed);
                        {error, connection_closed};
                    {error, ebadf} ->
                        %% like a closed file descriptor
                        {error, connection_closed};
                    {error, Reason} ->
                        lager:error("cluster_conn: client got error from remote: ~p, ~p",
                                    [Remote, Reason]),
                        {error, Reason}
                end,
            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members1)
    end.

ask_cluster_name(Socket, Transport, Remote) ->
    Transport:send(Socket, ?CTRL_ASK_NAME),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinName} ->
            {ok, binary_to_term(BinName)};
        {error, closed} ->
            %% the other side hung up. Stop quietly.
            {error, closed};
        Error ->
            lager:error("cluster_conn: failed to recv name from remote cluster at ~p because ~p",
                        [Remote, Error]),
            Error
    end.

ask_member_ips(Socket, Transport, Addr, Remote) ->
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    Transport:send(Socket, term_to_binary(Addr)),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinMembers} ->
            {ok, binary_to_term(BinMembers)};
        {error, closed} ->
            %% the other side hung up. Stop quietly.
            {error, closed};
        Error ->
            lager:error("cluster_conn: failed to recv members from remote cluster at ~p because ~p",
                        [Remote, Error]),
            Error
    end.

connected(Socket, Transport, Addr,
          {?REMOTE_CLUSTER_PROTO_ID, _MyVer, _RemoteVer},
          {_Remote,Client},
          Props) ->
    %% give control over the socket to the Client process.
    %% tell client we're connected and to whom
    Transport:controlling_process(Socket, Client),
    Client ! {self(), {connected_to_remote, Socket, Transport, Addr, Props}},
    ok.

connect_failed({_Proto,_Vers}, {error, _Reason}=Error, {_Remote,Client}) ->
    %% tell client we bombed and why
    Client ! {self(), {connect_failed, Error}},
    ok.
