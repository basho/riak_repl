%% Riak Core Cluster Manager Connection Supervisor
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
%%
%% Mission: ensure connections from a cluster manager to other clusters
%% in order to resolve ip addresses into known clusters or to refresh
%% the list of remote cluster members and observe their status.

-module(riak_core_cluster_conn_sup).
-behaviour(supervisor).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0,
         add_remote_connection/1,
         remove_remote_connection/1,
         connections/0,
         is_connected/1]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give cluster_conn processes to shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% a remote connection if we don't already have one
add_remote_connection(Remote) ->
    case is_connected(Remote) of
        false ->
            lager:info("Connecting to remote cluster: ~p", [Remote]),
            ChildSpec = make_remote(Remote),
            {ok, _Pid} = supervisor:start_child(?MODULE, ChildSpec),
            ok;
        _ ->
            lager:debug("Already connected to remote cluster: ~p", [Remote]),
            ok
    end.

remove_remote_connection(Remote) ->
    lager:debug("Disconnecting from remote cluster at: ~p", [Remote]),
    %% remove supervised cluster connection
    ok = supervisor:terminate_child(?MODULE, Remote),
    ok = supervisor:delete_child(?MODULE, Remote),
    %% This seems hacky, but someone has to tell the connection manager to stop
    %% trying to reach this target if it hasn't connected yet. It's the supervised
    %% cluster connection that requests the connection, but it's going to die, so
    %% it can't un-connect itself.
    riak_core_connection_mgr:disconnect(Remote).

connections() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE), is_pid(Pid)].

is_connected(Remote) ->
    Connections = connections(),
    lists:any(fun({R,_Pid}) -> R == Remote end, Connections).

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

make_remote(Remote) ->
    {Remote, {riak_core_cluster_conn, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_core_cluster_conn]}.
