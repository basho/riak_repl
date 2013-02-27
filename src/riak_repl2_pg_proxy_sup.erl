%%%-------------------------------------------------------------------
%%% Created : 21 Feb 2013 by Dave Parfitt
%%%-------------------------------------------------------------------
-module(riak_repl2_pg_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, set_leader/2, started/1, pg_proxy_for_cluster/1, 
         start_proxy/1, make_remote/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

set_leader(_Node, _Pid) ->
    lager:info("riak_repl2_pg_proxy:set_leader()").
    %% case node() of
    %%     Node ->
    %%         {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    %%         ProxyGets = riak_repl_ring:pg_enabled(Ring),
    %%         lager:info("Starting pg_proxies for ~p", [ProxyGets]),
    %%         %%[start_proxy(node(), Remote) || Remote <- ProxyGets];          
    %%         start_proxy(node(), "");
    %%     _ ->
    %%         lager:info("Stopping pg_proxies for ~p", [started()]),
    %%         [stop_proxy(node(), Remote) || Remote <- started()]
    %% end.

%% start_proxy(Node, Remote) ->
%%     lager:info("Starting pg_proxy for ~p", [Remote]),
%%     Childspec = make_remote(Remote),
%%     supervisor:start_child({?MODULE, Node}, Childspec).

start_proxy(Remote) ->
    lager:info("Starting pg_proxy for ~p", [Remote]),
    Childspec = make_remote(Remote),
    supervisor:start_child({?MODULE, node()}, Childspec).

%% stop_proxy(Node, Remote) ->
%%     lager:info("Stopping pg_proxy for ~p", [Remote]),
%%     supervisor:terminate_child({?MODULE, Node}, Remote),
%%     supervisor:delete_child({?MODULE, Node}, Remote).

%% started() ->
%%     [{Remote, Pid} || {Remote, Pid, _, _} <-
%%         supervisor:which_children(?MODULE), is_pid(Pid)].

started(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

pg_proxy_for_cluster(Cluster) ->
    Coords = [{Remote, Pid} || {Remote, Pid, _, _} <- supervisor:which_children(?MODULE),
                      is_pid(Pid), Remote == Cluster],
    case Coords of
        [] -> undefined;
        [{_,CoordPid}|_] -> CoordPid
    end.

init(_) ->
    {ok, {{one_for_one, 10, 5}, []}}.

make_remote(Remote) -> 
    Name = list_to_atom("pg_proxy_" ++ Remote),
    lager:info("make_remote ~p", [Name]),
    {Name, {riak_repl2_pg_proxy, start_link, [Name]},
        transient, ?SHUTDOWN, worker, [riak_repl2_pg_proxy, pg_proxy]}.

