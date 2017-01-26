-module(riak_repl_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/riak_repl.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_repl.schema"], [], context()),
    io:format("~p~n", [Config]),

    cuttlefish_unit:assert_config(Config, "riak_repl.data_root", "./repl/root"),
    cuttlefish_unit:assert_config(Config, "riak_core.cluster_mgr", {"1.2.3.4", 1234}),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_cluster", 5),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_node", 1),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssink_node", 1),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_on_connect", true),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval", 360),
    cuttlefish_unit:assert_config(Config, "riak_repl.rtq_max_bytes", 104857600),
    cuttlefish_unit:assert_config(Config, "riak_repl.proxy_get", disabled),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_interval", 15),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_timeout", 15),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_use_background_manager", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_repl.fullsync_stat_refresh_interval"),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_strategy", keylist),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
            {["mdc", "data_root"], "/some/repl/place"},
            {["mdc", "cluster_manager"], {"4.3.2.1", 4321}},
            {["mdc", "fullsync", "strategy"], aae},
            {["mdc", "fullsync", "source", "max_workers_per_cluster"], 10},
            {["mdc", "fullsync", "source", "max_workers_per_node"], 2},
            {["mdc", "fullsync", "sink", "max_workers_per_node"], 4},
            {["mdc", "fullsync", "start_on_connect"], off},
            {["mdc", "fullsync", "interval"], per_sink},
            {["mdc", "fullsync", "interval", "cluster1"], "15m"},
            {["mdc", "fullsync", "interval", "cluster2"], "1h"},
            {["mdc", "realtime", "queue_max_bytes"], "50MB"},
            {["mdc", "proxy_get"], on},
            {["mdc", "realtime", "heartbeat", "interval"], "15m"},
            {["mdc", "realtime", "heartbeat", "timeout"], "15d"},
            {["mdc", "fullsync", "background_manager"], on},
            {["mdc", "fullsync", "source", "metrics_refresh_interval"], "30s"}
           ],

    %% The defaults are defined in ../priv/riak_repl.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/riak_repl.schema"], Conf, context()),

    cuttlefish_unit:assert_config(Config, "riak_repl.data_root", "/some/repl/place"),
    cuttlefish_unit:assert_config(Config, "riak_core.cluster_mgr", {"4.3.2.1", 4321}),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_cluster", 10),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssource_node", 2),
    cuttlefish_unit:assert_config(Config, "riak_repl.max_fssink_node", 4),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_on_connect", false),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval.cluster1", 15),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval.cluster2", 60),
    cuttlefish_unit:assert_config(Config, "riak_repl.rtq_max_bytes", 52428800),
    cuttlefish_unit:assert_config(Config, "riak_repl.proxy_get", enabled),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_interval", 900),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_timeout", 1296000),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_use_background_manager", true),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_stat_refresh_interval", 30000),
    cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_strategy", aae),
    ok.

heartbeat_interval_test() ->
    Conf = [{["mdc", "realtime", "heartbeat"], false},
            {["mdc", "realtime", "heartbeat", "interval"], 300}],
    Config = cuttlefish_unit:generate_templated_config(["../priv/riak_repl.schema"], Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_repl.rt_heartbeat_interval", undefined).

fullsync_interval_test_() ->
    [
     {"interval=never but sink intervals set results in warning", 
      fun() -> 
              riak_repl_lager_test_backend:bounce(warning),
              Conf = [{["mdc", "fullsync", "interval"], never},
                      {["mdc", "fullsync", "interval", "cluster1"], 360}],
              Config = cuttlefish_unit:generate_templated_config(["../priv/riak_repl.schema"], Conf, context()),
              cuttlefish_unit:assert_config(Config, "riak_repl.fullsync_interval", disabled),
              Logs = riak_repl_lager_test_backend:get_logs(),
              ?assertMatch([_|_], Logs),
              [ ?assertEqual("mdc.fullsync.interval is set to never, sink specific intervals are ignored",
                             Message) || [_Time, " ", ["[","warning","]"], Message] <- Logs ]
      end},
     {"interval=per_sink but no sink intervals is invalid", 
      fun() -> 
              Conf = [{["mdc", "fullsync", "interval"], per_sink}],
              Config = cuttlefish_unit:generate_templated_config(["../priv/riak_repl.schema"], Conf, context()),
              cuttlefish_unit:assert_error_message(Config, 
                                                   "Translation for 'riak_repl.fullsync_interval' found invalid configuration: "
                                                   "Cannot set mdc.fullsync.interval = per_sink and"
                                                   " omit sink-specific intervals, set sink-specific"
                                                   " intervals or use 'never'")
      end},
     {"interval=Time and per-sink intervals is invalid", 
      fun() ->
              Conf = [{["mdc", "fullsync", "interval"], 60},
                      {["mdc", "fullsync", "interval", "cluster1"], 360}],
              Config = cuttlefish_unit:generate_templated_config(["../priv/riak_repl.schema"], Conf, context()),
              cuttlefish_unit:assert_error_message(Config, 
                                                   "Translation for 'riak_repl.fullsync_interval' found invalid configuration: "
                                                   "Cannot set both mdc.fullsync.interval and sink-specific intervals")
      end}
    ].



%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_jmx's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() ->
    [
        {repl_data_root, "./repl/root"},
        {cluster_manager_ip, "1.2.3.4"},
        {cluster_manager_port, 1234}
    ].
