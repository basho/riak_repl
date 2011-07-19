%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_listener_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).
-include("riak_repl.hrl").
-export([start_link/0, init/1, stop/1]).
-export([start_listener/1]).

start_listener(#repl_listener{listen_addr={IP, Port}}) ->
    lager:info("Starting replication listener on ~s:~p",
                          [IP, Port]),
    supervisor:start_child(?MODULE, [IP, Port]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_S) -> ok.

%% @private
init([]) ->
    {ok, 
     {{simple_one_for_one, 100, 10}, 
      [{undefined,
        {riak_repl_listener, start_link, []},
        transient, brutal_kill, worker, [riak_repl_listener]}]}}.
