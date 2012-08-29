%% Riak EnterpriseDS
%% Copyright 2007-2012 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl2_rtsource_sup).
-behaviour(supervisor).
-export([start_link/0, enable/1, disable/1, enabled/0]).
-export([init/1]).

-define(SHUTDOWN, 5000). % how long to give rtsource processes to persist queue/shutdown

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%TODO: Rename enable/disable something better - start/stop is a bit overloaded
enable(Remote) ->
    lager:info("Starting replication realtime source ~p", [Remote]),
    ChildSpec = make_remote(Remote),
    supervisor:start_child(?MODULE, ChildSpec).

disable(Remote) ->
    lager:info("Stopping replication realtime source ~p", [Remote]),
    supervisor:terminate_child(?MODULE, Remote),
    supervisor:delete_child(?MODULE, Remote).

enabled() ->
    [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE)].

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

make_remote(Remote) ->
    {Remote, {riak_repl2_rtsource, start_link, [Remote]},
        permanent, ?SHUTDOWN, worker, [riak_repl2_rtsource]}.
