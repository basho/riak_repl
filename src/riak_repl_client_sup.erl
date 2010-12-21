%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_client_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).
-include("riak_repl.hrl").
-export([start_link/0, init/1, stop/1]).
-export([start_site/1, stop_site/1, running_site_procs/0]).

start_site(SiteName) ->
    ChildSpec = {SiteName, {riak_repl_tcp_client, start_link, [SiteName]},
                 permanent, brutal_kill, worker, [riak_repl_tcp_client]},
    supervisor:start_child(?MODULE, ChildSpec).

stop_site(SiteName) ->
    supervisor:terminate_child(?MODULE, SiteName),
    supervisor:delete_child(?MODULE, SiteName).

running_site_procs() ->
    [{SiteName, Pid} || {SiteName, Pid, _, _} <- supervisor:which_children(?MODULE)].

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.

