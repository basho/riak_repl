%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_app).
-author('Andy Gross <andy@basho.com>').
-behaviour(application).
-export([start/2,stop/1]).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak_repl.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_repl),

    %% Ensure that the KV service has fully loaded.
    riak_core:wait_for_service(riak_kv),

    IncarnationId = erlang:phash2({make_ref(), now()}),
    application:set_env(riak_repl, incarnation, IncarnationId),
    ok = ensure_dirs(),

    riak_core:register([{bucket_fixup, riak_repl}]),

    %% Register our capabilities
    riak_core_capability:register({riak_repl, bloom_fold},
                                  [true, false], %% prefer to use bloom_fold in new code
                                  false),        %% the default is false for legacy code

    %% skip Riak CS blocks
    case riak_repl_util:proxy_get_active() of
        true -> riak_core:register([{repl_helper, riak_repl_cs}]);
        false -> lager:info("REPL CS block skip disabled")
    end,

    ok = riak_api_pb_service:register(riak_repl_pb_get, 128, 129),

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_repl_cinfo),

    %% Spin up supervisor
    case riak_repl_sup:start_link() of
        {ok, Pid} ->
            riak_core:register(riak_repl, [{stat_mod, riak_repl_stats}]),
            ok = riak_core_ring_events:add_guarded_handler(riak_repl_ring_handler, []),
            %% Add routes to webmachine
            [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_repl_web:dispatch_table()) ],
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak_repl.
stop(_State) -> ok.

ensure_dirs() ->
    {ok, DataRoot} = application:get_env(riak_repl, data_root),
    LogDir = filename:join(DataRoot, "logs"),
    case filelib:ensure_dir(filename:join(LogDir, "empty")) of
        ok ->
            application:set_env(riak_repl, log_dir, LogDir),
            ok;
        {error, Reason} ->
            Msg = io_lib:format("riak_repl couldn't create log dir ~p: ~p~n", [LogDir, Reason]),
            riak:stop(lists:flatten(Msg))
    end,
    {ok, Incarnation} = application:get_env(riak_repl, incarnation),
    WorkRoot = filename:join([DataRoot, "work"]),
    prune_old_workdirs(WorkRoot),
    WorkDir = filename:join([WorkRoot, integer_to_list(Incarnation)]),
    case filelib:ensure_dir(filename:join([WorkDir, "empty"])) of
        ok ->
            application:set_env(riak_repl, work_dir, WorkDir),
            ok;
        {error, R} ->
            M = io_lib:format("riak_repl couldn't create work dir ~p: ~p~n", [WorkDir,R]),
            riak:stop(lists:flatten(M)),
            {error, R}
    end.

prune_old_workdirs(WorkRoot) ->
    case file:list_dir(WorkRoot) of
        {ok, SubDirs} ->
            DirPaths = [filename:join(WorkRoot, D) || D <- SubDirs],
            Cmds = [lists:flatten(io_lib:format("rm -rf ~s", [D])) || D <- DirPaths],
            [os:cmd(Cmd) || Cmd <- Cmds];
        _ ->
            ignore
    end.
