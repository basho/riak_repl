%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_fsm_common).
-author('Andy Gross <andy@basho.com').
-include("riak_repl.hrl").
-export([common_init/1,
        work_dir/2]).

%% @doc Common functions for both the repl server and client FSMs.

common_init(Socket) ->
    inet:setopts(Socket, ?FSM_SOCKOPTS),
    {ok, Client} = riak:local_client(),
    PI = riak_repl_util:make_peer_info(),
    Partitions = riak_repl_util:get_partitions(PI#peer_info.ring),    
    [{client, Client},
     {partitions, Partitions},
     {my_pi, PI}].

work_dir(Socket, SiteName) ->
    {ok, WorkRoot} = application:get_env(riak_repl, work_dir),
    SiteDir = SiteName ++ "-" ++ riak_repl_util:format_socketaddrs(Socket),
    WorkDir = filename:join(WorkRoot, SiteDir),
    ok = filelib:ensure_dir(filename:join(WorkDir, "empty")),
    {ok, WorkDir}.
