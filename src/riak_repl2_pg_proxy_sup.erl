%% Riak EnterpriseDS
%% Copyright 2007-2014 Basho Technologies, Inc. All Rights Reserved.

-module(riak_repl2_pg_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         set_leader/2,
         started/1,
         start_proxy/1,
         make_remote/1,
         stop_proxy/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(SHUTDOWN, 5000).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

set_leader(Node, _Pid) ->
    case node() of
        Node -> ok;
        _ ->
          [  begin
                ok = supervisor:terminate_child(?MODULE, Remote),
                ok = supervisor:delete_child(?MODULE, Remote)
             end
                || {Remote, Pid, _, _} <-
                      supervisor:which_children(?MODULE), is_pid(Pid)
           ]
    end.

start_proxy(Remote) ->
    lager:debug("Starting pg_proxy for ~p", [Remote]),
    Childspec = make_remote(Remote),
    supervisor:start_child({?MODULE, node()}, Childspec).

stop_proxy(Node, Remote) ->
    lager:debug("Stopping pg_proxy for ~p", [Remote]),
    ok = supervisor:terminate_child({?MODULE, Node}, Remote),
    ok = supervisor:delete_child({?MODULE, Node}, Remote).

started(Node) ->
    [{Remote, Pid} || {Remote, Pid, _, _} <-
        supervisor:which_children({?MODULE, Node}), is_pid(Pid)].

init(_) ->
    {ok, {{one_for_one, 10, 5}, []}}.

make_remote(Remote) ->
    Name = riak_repl_util:make_pg_proxy_name(Remote),
    {Name, {riak_repl2_pg_proxy, start_link, [Name]},
        transient, ?SHUTDOWN, worker, [riak_repl2_pg_proxy, pg_proxy]}.
