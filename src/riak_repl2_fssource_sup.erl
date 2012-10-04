%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_fssource_sup).
-behaviour(supervisor).
-export([start_link/0, enable/2, disable/2, enabled/0, enabled/1, set_leader/2]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%TODO: Rename enable/disable something better - start/stop is a bit overloaded
enable(Node, Remote) ->
    lager:info("Starting replication fullsync source ~p", [Remote]),
    ChildSpec = make_remote(Remote),
    supervisor:start_child({?MODULE, Node}, ChildSpec).

disable(Node, Remote) ->
    lager:info("Stopping replication fullsync source ~p", [Remote]),
    supervisor:terminate_child({?MODULE, Node}, Remote),
    supervisor:delete_child({?MODULE, Node}, Remote).

enabled() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children(?MODULE), is_pid(Pid)].

enabled(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

set_leader(LeaderNode, LeaderPid) ->
    case node() of
        LeaderNode ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Fullsyncs = riak_repl_ring:fs_enabled(Ring),
            [enable(node(), Remote) || Remote <- Fullsyncs];
        _ ->
            [disable(node(), Remote) || Remote <- enabled()]
    end.


%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_fssource, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_repl2_fssource]}.
