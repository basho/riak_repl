%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_stats).
-author('Andy Gross <andy@basho.com>').
-behaviour(gen_server).
-include("riak_repl.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([start_link/0,
         stop/0,
         update/1,
         update/2,
         update/3,
         register_stats/0,
         get_stats/0,
         produce_stats/0,
         get_stats_values/0,
         rt_source_errors/0,
         rt_sink_errors/0,
         clear_rt_dirty/0,
         touch_rt_dirty_file/0,
         remove_rt_dirty_file/0,
         is_rt_dirty/0]).

-define(APP, riak_repl).
-define(PREFIX, riak).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

register_stats() ->
  register_stats(stats()),
  update([last,report], tstamp(), gauge).

register_stats(Stats) ->
    lists:foreach(fun(Stat) -> stats:register([?PREFIX,?APP| Stat]) end, Stats).

%% If any source errors are detected, write a file out to persist this status
%% across restarts
rt_source_errors() ->
    update([rt,source,errors]),
    rt_dirty().

%% If any sink errors are detected, write a file out to persist this status
%% across restarts
rt_sink_errors() ->
    update([rt,sink,errors]),
    rt_dirty().

rt_dirty() ->
    touch_rt_dirty_file(),
    update([rt,dirty]),
    Stat = lookup_stat([rt,dirty]) + 1,  % we know it's at least 1
    % increment counter is a cast, so if the number is relatively small
    % then notify the server. otherwise, we don't want to spam the
    % coordinator. Once this value is reset to 0, we'll notify the
    % coordinator again.
    case Stat > 0 andalso Stat < 5 of
        true ->
            %% the coordinator might not be up yet
            lager:debug("Notifying coordinator of rt_dirty"),
            % notify the coordinator at a later time, hopefully
            % after the fscoordinator is started. If it's not, then just
            % log a warning.
            spawn(fun() ->
                      timer:sleep(30000),
                      try
                        riak_repl2_fscoordinator:node_dirty(node())
                      catch
                        _:_ ->
                         lager:debug("Failed to notify coordinator of rt_dirty status")
                      end
            end),
            ok;
        false -> ok
    end.

%% @doc assumed counter as default @end
update(Name) ->
    update(Name, 1, counter).
update(Name, IncrBy) ->
    update(Name, IncrBy, counter).
update(Name, IncrBy, Type) ->
    StatName = lists:flatten([?PREFIX, ?APP | Name]),
    stats:update(StatName, IncrBy, Type).

get_stats() ->
  case erlang:whereis(riak_repl_stats) of
        Pid when is_pid(Pid) ->
            Stats = produce_stats(),
            get_stats_values(Stats);
        _ -> []
    end.

produce_stats() ->
    {Stats,_} = get_app_stats(), Stats.

get_app_stats() ->
    stats:get_stats([[?PREFIX,?APP|'_']]).

get_stats_values() ->
  get_stats_values(?APP).
get_stats_values(Arg) ->
    stats:get_value(Arg).


%%%----------------------------------------------------------------%%%

init([]) ->
    register_stats(),
    schedule_report_bw(),
    case is_rt_dirty() of
        true ->
            lager:warning("RT marked as dirty upon startup"),
            update(rt_dirty, 1, counter),
            % let the coordinator know about the dirty state when the node
            % comes back up
            lager:debug("Notifying coordinator of rt_dirty state"),
            riak_repl2_fscoordinator:node_dirty(node());
        false ->
            lager:debug("RT is NOT dirty")
    end,
    {ok, ok}.

stats() ->
    [{[server,bytes,sent],          counter,   [], [{value, server_bytes_sent}]},
     {[server,bytes,recv],          counter,   [], [{value, server_bytes_recv}]},
     {[server,connects],            counter,   [], [{value, server_connects}]},
     {[server,connect,errors],      counter,   [], [{value, server_connect_errors}]},
     {[server,fullsyncs],           counter,   [], [{value, server_fullsyncs}]},
     {[client,bytes,sent],          counter,   [], [{value, client_bytes_sent}]},
     {[client,bytes,recv],          counter,   [], [{value, client_bytes_recv}]},
     {[client,connects],            counter,   [], [{value, client_connects}]},
     {[client,connect,errors],      counter,   [], [{value, client_connect_errors}]},
     {[client,redirect],            counter,   [], [{value, client_redirect}]},
     {[objects,dropped,no,clients], counter,   [], [{value, objects_dropped_no_clients}]},
     {[objects,dropped,no,leader],  counter,   [], [{value, objects_dropped_no_leader}]},
     {[objects,sent],               counter,   [], [{value, objects_sent}]},
     {[objects,forwarded],          counter,   [], [{value, objects_forwarded}]},
     {[elections,elected],          counter,   [], [{value, elections_elected}]},
     {[elections,leader,changed],   counter,   [], [{value, elections_leader_changed}]},
     {[client,rx,kbps],             histogram, [], [{mean  , client_rx_kbps_mean},
                                                  {median, client_rx_kbps_median},
                                                  {95    , client_rx_kbps_95},
                                                  {99    , client_rx_kbps_99},
                                                  {max   , client_rx_kbps_100}]},
     {[client,tx,kbps],             histogram, [], [{mean  , client_tx_kbps_mean},
                                                  {median, client_tx_kbps_median},
                                                  {95    , client_tx_kbps_95},
                                                  {99    , client_tx_kbps_99},
                                                  {max   , client_tx_kbps_100}]},
     {[server,rx,kbps],             histogram, [], [{mean  , server_rx_kbps_mean},
                                                  {median, server_rx_kbps_median},
                                                  {95    , server_rx_kbps_95},
                                                  {99    , server_rx_kbps_99},
                                                  {max   , server_rx_kbps_100}]},
     {[server,tx,kbps],             histogram, [], [{mean  , server_tx_kbps_mean},
                                                  {median, server_tx_kbps_median},
                                                  {95    , server_tx_kbps_95},
                                                  {99    , server_tx_kbps_99},
                                                  {max   , server_tx_kbps_100}]},
     {[last,report],                gauge},
     {[last,client,bytes,sent],     gauge},
     {[last,client,bytes,recv],     gauge},
     {[last,server,bytes,sent],     gauge},
     {[last,server,bytes,recv],     gauge},
     {[rt,source,errors],           counter,   [], [{value, rt_source_errors}]},
     {[rt,sink,errors],             counter,   [], [{value, rt_sink_errors}]},
     {[rt,dirty],                   counter,   [], [{value, rt_dirty}]}].

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report_bw, State) ->
    ThisClientBytesSent=lookup_stat([client,bytes,sent]),
    ThisClientBytesRecv=lookup_stat([client,bytes,recv]),
    ThisServerBytesSent=lookup_stat([server,bytes,sent]),
    ThisServerBytesRecv=lookup_stat([server,bytes,recv]),

    Now = tstamp(),
    DeltaSecs = now_diff(Now, lookup_stat([last,report])),
    ClientTx = bytes_to_kbits_per_sec(ThisClientBytesSent, lookup_stat([last,client,bytes,sent]), DeltaSecs),
    ClientRx = bytes_to_kbits_per_sec(ThisClientBytesRecv, lookup_stat([last,client,bytes,recv]), DeltaSecs),
    ServerTx = bytes_to_kbits_per_sec(ThisServerBytesSent, lookup_stat([last,server,bytes,sent]), DeltaSecs),
    ServerRx = bytes_to_kbits_per_sec(ThisServerBytesRecv, lookup_stat([last,server,bytes,recv]), DeltaSecs),

      _ = [update(Metric, Reading, histogram)
     || {Metric, Reading} <- [{[client,tx,kbps], ClientTx},
                              {[client,rx,kbps], ClientRx},
                              {[server,tx,kbps], ServerTx},
                              {[server,rx,kbps], ServerRx}]],

      _ = [update(Metric, Reading, gauge)
     || {Metric, Reading} <- [{[last,client,bytes,sent], ThisClientBytesSent},
                              {[last,client,bytes,recv], ThisClientBytesRecv},
                              {[last,server,bytes,sent], ThisServerBytesSent},
                              {[last,server,bytes,recv], ThisServerBytesRecv}]],

    schedule_report_bw(),
    update([last,report], Now, gauge),
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule_report_bw() ->
    BwHistoryInterval = app_helper:get_env(riak_repl, bw_history_interval, 60000),
    erlang:send_after(BwHistoryInterval, self(), report_bw).

%% Convert two values in bytes to a kbits/sec
bytes_to_kbits_per_sec(This, Last, Delta) when is_number(This), is_number(Last), is_number(Delta), Delta > 1.0e-6 ->
    trunc((This - Last) / (128 * Delta));  %% x8/1024 = x/128
bytes_to_kbits_per_sec(_, _, _) ->
    undefined.

lookup_stat(Name) ->
    stats:get_value([?PREFIX,?APP|Name]).

now_diff(NowSecs, ThenSecs) when is_number(NowSecs), is_number(ThenSecs) ->
    NowSecs - ThenSecs;
now_diff(_, _) ->
    undefined.

tstamp() ->
    stats:timestamp().

rt_dirty_filename() ->
    %% or riak_repl/work_dir?
    P_DataDir = app_helper:get_env(riak_core, platform_data_dir),
    filename:join([P_DataDir, "riak_repl", "rt_dirty"]).

touch_rt_dirty_file() ->
    DirtyRTFile = rt_dirty_filename(),
    ok = filelib:ensure_dir(DirtyRTFile),
    case file:write_file(DirtyRTFile, "") of
        ok -> lager:debug("RT dirty file written to ~p", [DirtyRTFile]);
        ER -> lager:warning("Can't write to file ~p due to ~p",[DirtyRTFile,
                                                                ER])
    end.

remove_rt_dirty_file() ->
    DirtyRTFile = rt_dirty_filename(),
    case file:delete(DirtyRTFile) of
        ok -> lager:debug("RT dirty flag cleared");
        {error, Reason} ->
            %% this is a lager:debug because each fullsync
            %% would display this warning.
            lager:debug("Can't clear RT dirty flag: ~p",
                                       [Reason])
    end.


clear_rt_dirty() ->
    remove_rt_dirty_file(),
    exometer:reset([?PREFIX,?APP,rt,dirty]),
    register(?APP, {[rt,dirty], counter,[], [{value, rt_dirty}]}).

is_rt_dirty() ->
    DirtyRTFile = rt_dirty_filename(),
    case file:read_file_info(DirtyRTFile) of
        {error, enoent} -> false;
        {error, Reason} ->
                  lager:warning("Error reading RT dirty file: ~p", [Reason]),
                  %% assume it's not there
                  false;
        {ok, _Data} -> true
    end.

-ifdef(TEST).

repl_stats_test_() ->
    error_logger:tty(false),
    {"stats test", setup, fun() ->
                    meck:new(exometer_util, [passthrough]),
                    application:start(bear),
                    ok = exometer:start(),
                    meck:new(riak_core_cluster_mgr, [passthrough]),
                    meck:new(riak_repl2_fscoordinator_sup, [passthrough]),
                    meck:expect(riak_core_cluster_mgr, get_leader, fun() ->
                                node() end),
                    meck:expect(riak_repl2_fscoordinator_sup, started,
                        fun(_Node) -> [] end),
                    {ok, _CPid} = riak_core_stat_cache:start_link(),
                    {ok, Pid} = riak_repl_stats:start_link(),
                    tick(1000, 0),
                    Pid
            end,
     fun(_Pid) ->
             exometer:stop(),
             riak_repl_stats:stop(),
             riak_core_stat_cache:stop(),
             meck:unload(exometer_util),
             meck:unload(riak_core_cluster_mgr),
             meck:unload(riak_repl2_fscoordinator_sup)
     end,
     [{"Register stats", fun test_register_stats/0},
      {"Populate stats", fun test_populate_stats/0},
      {"Check stats", fun test_check_stats/0},
      {"check report", fun  test_report/0}]
    }.

test_register_stats() ->
   error_logger:tty(false),
    register_stats(),
    RegisteredReplStats = [Stat || {App, Stat} <- get_stats(),
                                   App == riak_repl],
    {Stats, _Types} = lists:unzip(stats()),
    ?assertEqual(lists:sort(Stats), lists:sort(RegisteredReplStats)).

test_populate_stats() ->
    error_logger:tty(false),
    Bytes = 1000,
    ok = update([client,bytes,sent],Bytes),
    ok = update([client,bytes,recv],Bytes),
    ok = update([client,connects]),
    ok = update([client,connect,errors]),
    ok = update([client,redirect]),
    ok = update([server,bytes,sent],Bytes),
    ok = update([server,bytes,recv],Bytes),
    ok = update([server,connects]),
    ok = update([server,connect,errors]),
    ok = update([server,fullsyncs]),
    ok = update([objects,dropped,no,clients]),
    ok = update([objects,dropped,no,leader]),
    ok = update([objects,sent]),
    ok = update([objects,forwarded]),
    ok = update([elections,elected]),
    ok = update([elections,leader,changed]),
    ok = rt_source_errors(),
    ok = rt_sink_errors().

test_check_stats() ->
   error_logger:tty(false),
    Expected = [ %% Test, values might be different from exometer.
        {[server,bytes,sent],1000},
        {[server,bytes,recv],1000},
        {[server,connects],1},
        {[server,connect,errors],1},
        {[server,fullsyncs],1},
        {[client,bytes,sent],1000},
        {[client,bytes,recv],1000},
        {[client,connects],1},
        {[client,connect,errors],1},
        {[client,redirect],1},
        {[objects,dropped,no,clients],1},
        {[objects,dropped,no,leader],1},
        {[objects,sent],1},
        {[objects,forwarded],1},
        {[elections,elected],1},
        {[elections,leader,changed],1},
        {[client,rx,kbps],[]},
        {[client,tx,kbps],[]},
        {[server,rx,kbps],[]},
        {[server,tx,kbps],[]},
        {[rt,source,errors],1},
        {[rt,sink,errors], 1},
        {[rt,dirty], 2}],
    Result = get_stats(),

    ?assertEqual(Expected,
        [{K1, V} || {K1, V} <- Result, {K2, _} <- Expected, K1 == K2]).


test_report() ->
    Bytes = 1024 * 60,
    Now = 10000,
    ReportMinutes = 10,
    for_n_minutes(ReportMinutes, Now, Bytes),
    %% I think the cache is getting in the way here?
    timer:sleep(1000),
    [begin
         ?assertEqual(8, length(Values)),
         [?assertEqual(8, Val) || Val <- Values]
     end || {Name, Values} <- get_stats(),
            lists:member(Name, [[client,rx,kbps],
                                [client,tx,kbps],
                                [server,rx,kbps],
                                [server,tx,kbps]])].

for_n_minutes(0, _Time, _Bytes) ->
    ok;
for_n_minutes(Minutes, Time0, Bytes) ->
    Time = tick(Time0, 60),
  [update(Name, Bytes,
                      counter) || Name <-
                        [[client,bytes,sent],
                          [client,bytes,recv],
                          [server,bytes,sent],
                          [server,bytes,recv]]],

    riak_repl_stats:handle_info(report_bw, ok),
    for_n_minutes(Minutes - 1, Time, Bytes).

tick(Time, Interval) ->
    NewTime = Time+Interval,
    meck:expect(exometer_util, timestamp, fun() ->
                                                 NewTime end),
    NewTime.

-endif.
