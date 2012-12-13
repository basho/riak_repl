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
         client_bytes_sent/1,
         client_bytes_recv/1,
         client_connects/0,
         client_connect_errors/0,
         client_redirect/0,
         server_bytes_sent/1,
         server_bytes_recv/1,
         server_connects/0,
         server_connect_errors/0,
         server_fullsyncs/0,
         objects_dropped_no_clients/0,
         objects_dropped_no_leader/0,
         objects_sent/0,
         objects_forwarded/0,
         elections_elected/0,
         elections_leader_changed/0,
         register_stats/0,
         get_stats/0,
         produce_stats/0,
         rt_source_errors/0,
         rt_sink_errors/0]).

-define(APP, riak_repl).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

register_stats() ->
    [(catch folsom_metrics:delete_metric({?APP, Name})) || {Name, _Type} <- stats()],
    [register_stat(Name, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}),
    folsom_metrics:notify_existing_metric({?APP, last_report}, tstamp(), gauge).

client_bytes_sent(Bytes) ->
    increment_counter(client_bytes_sent, Bytes).

client_bytes_recv(Bytes) ->
    increment_counter(client_bytes_recv, Bytes).

client_connects() ->
    increment_counter(client_connects).

client_connect_errors() ->
    increment_counter(client_connect_errors).

client_redirect() ->
    increment_counter(client_redirect).

server_bytes_sent(Bytes) ->
    increment_counter(server_bytes_sent, Bytes).

server_bytes_recv(Bytes) ->
    increment_counter(server_bytes_recv, Bytes).

server_connects() ->
    increment_counter(server_connects).

server_connect_errors() ->
    increment_counter(server_connect_errors).

server_fullsyncs() ->
    increment_counter(server_fullsyncs).

objects_dropped_no_clients() ->
    increment_counter(objects_dropped_no_clients).

objects_dropped_no_leader() ->
    increment_counter(objects_dropped_no_leader).

objects_sent() ->
    increment_counter(objects_sent).

objects_forwarded() ->
    increment_counter(objects_forwarded).

elections_elected() ->
    increment_counter(elections_elected).

elections_leader_changed() ->
    increment_counter(elections_leader_changed).

get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

produce_stats() ->
    lists:flatten([backwards_compat(Stat, Type) ||
        {Stat, Type} <- stats()]).

rt_source_errors() ->
    increment_counter(rt_source_errors).

rt_sink_errors() ->
    increment_counter(rt_sink_errors).

init([]) ->
    register_stats(),
    schedule_report_bw(),
    {ok, ok}.

register_stat(Name, counter) ->
    folsom_metrics:new_counter({?APP, Name});
register_stat(Name, history) ->
    BwHistoryLen =  get_bw_history_len(),
    folsom_metrics:new_history({?APP, Name}, BwHistoryLen);
register_stat(Name, gauge) ->
    folsom_metrics:new_gauge({?APP, Name}).

stats() ->
    [{server_bytes_sent, counter},
     {server_bytes_recv, counter},
     {server_connects, counter},
     {server_connect_errors, counter},
     {server_fullsyncs, counter},
     {client_bytes_sent, counter},
     {client_bytes_recv, counter},
     {client_connects, counter},
     {client_connect_errors, counter},
     {client_redirect, counter},
     {objects_dropped_no_clients, counter},
     {objects_dropped_no_leader, counter},
     {objects_sent, counter},
     {objects_forwarded, counter},
     {elections_elected, counter},
     {elections_leader_changed, counter},
     {client_rx_kbps, history},
     {client_tx_kbps, history},
     {server_rx_kbps, history},
     {server_tx_kbps, history},
     {last_report, gauge},
     {last_client_bytes_sent, gauge},
     {last_client_bytes_recv, gauge},
     {last_server_bytes_sent, gauge},
     {last_server_bytes_recv, gauge},
     {rt_source_errors, counter},
     {rt_sink_errors, counter}].

increment_counter(Name) ->
    increment_counter(Name, 1).

increment_counter(Name, IncrBy) when is_atom(Name) andalso is_integer(IncrBy) ->
    gen_server:cast(?MODULE, {increment_counter, Name, IncrBy}).

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({increment_counter, Name, IncrBy}, State) ->
    folsom_metrics:notify_existing_metric({?APP, Name}, {inc, IncrBy}, counter),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report_bw, State) ->
    ThisClientBytesSent=lookup_stat(client_bytes_sent),
    ThisClientBytesRecv=lookup_stat(client_bytes_recv),
    ThisServerBytesSent=lookup_stat(server_bytes_sent),
    ThisServerBytesRecv=lookup_stat(server_bytes_recv),

    Now = tstamp(),
    DeltaSecs = now_diff(Now, lookup_stat(last_report)),
    ClientTx = bytes_to_kbits_per_sec(ThisClientBytesSent, lookup_stat(last_client_bytes_sent), DeltaSecs),
    ClientRx = bytes_to_kbits_per_sec(ThisClientBytesRecv, lookup_stat(last_client_bytes_recv), DeltaSecs),
    ServerTx = bytes_to_kbits_per_sec(ThisServerBytesSent, lookup_stat(last_server_bytes_sent), DeltaSecs),
    ServerRx = bytes_to_kbits_per_sec(ThisServerBytesRecv, lookup_stat(last_server_bytes_recv), DeltaSecs),

    [folsom_metrics:notify_existing_metric({?APP, Metric}, Reading, history)
     || {Metric, Reading} <- [{client_tx_kbps, ClientTx},
                              {client_rx_kbps, ClientRx},
                              {server_tx_kbps, ServerTx},
                              {server_rx_kbps, ServerRx}]],

    [folsom_metrics:notify_existing_metric({?APP, Metric}, Reading, gauge)
     || {Metric, Reading} <- [{last_client_bytes_sent, ThisClientBytesSent},
                              {last_client_bytes_recv, ThisClientBytesRecv},
                              {last_server_bytes_sent, ThisServerBytesSent},
                              {last_server_bytes_recv, ThisServerBytesRecv}]],

    schedule_report_bw(),
    folsom_metrics:notify_existing_metric({?APP, last_report}, Now, gauge),
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule_report_bw() ->
    BwHistoryInterval = app_helper:get_env(riak_repl, bw_history_interval, 60000),
    erlang:send_after(BwHistoryInterval, self(), report_bw).

%% Convert two values in bytes to a kbits/sec
bytes_to_kbits_per_sec(This, Last, Delta) ->
    trunc((This - Last) / (128 * Delta)).  %% x8/1024 = x/128

lookup_stat(Name) ->
    folsom_metrics:get_metric_value({?APP, Name}).

now_diff(NowSecs, ThenSecs) ->
    NowSecs - ThenSecs.

tstamp() ->
    folsom_utils:now_epoch().

get_bw_history_len() ->
    app_helper:get_env(riak_repl, bw_history_len, 8).

backwards_compat(Name, history) ->
    Stats = folsom_metrics:get_history_values({?APP, Name}, get_bw_history_len()),
    Readings = [[Reading || {event, Reading} <- Events] || {_Moment, Events} <- Stats],
    {Name, lists:flatten(Readings)};
backwards_compat(_Name, gauge) ->
    [];
backwards_compat(Name,  _Type) ->
    {Name, lookup_stat(Name)}.

-ifdef(TEST).

repl_stats_test_() ->
    {setup, fun() ->
                    folsom:start(),
                    meck:new(folsom_utils, [passthrough]),
                    {ok, _CPid} = riak_core_stat_cache:start_link(),
                    {ok, Pid} = riak_repl_stats:start_link(),
                    tick(1000, 0),
                    Pid
            end,
     fun(_Pid) ->
             folsom:stop(),
             riak_repl_stats:stop(),
             riak_core_stat_cache:stop(),
             meck:unload(folsom_utils)
     end,
     [{"Register stats", fun test_register_stats/0},
      {"Populate stats", fun test_populate_stats/0},
      {"Check stats", fun test_check_stats/0},
      {"check report", fun  test_report/0}]
    }.

test_register_stats() ->
    register_stats(),
    RegisteredReplStats = [Stat || {App, Stat} <- folsom_metrics:get_metrics(),
                                   App == riak_repl],
    {Stats, _Types} = lists:unzip(stats()),
    ?assertEqual(lists:sort(Stats), lists:sort(RegisteredReplStats)).

test_populate_stats() ->
    Bytes = 1000,
    ok = client_bytes_sent(Bytes),
    ok = client_bytes_recv(Bytes),
    ok = client_connects(),
    ok = client_connect_errors(),
    ok = client_redirect(),
    ok = server_bytes_sent(Bytes),
    ok = server_bytes_recv(Bytes),
    ok = server_connects(),
    ok = server_connect_errors(),
    ok = server_fullsyncs(),
    ok = objects_dropped_no_clients(),
    ok = objects_dropped_no_leader(),
    ok = objects_sent(),
    ok = objects_forwarded(),
    ok = elections_elected(),
    ok = elections_leader_changed(),
    ok = rt_source_errors(),
    ok = rt_sink_errors().

test_check_stats() ->
    ?assertEqual([{server_bytes_sent,1000},
                  {server_bytes_recv,1000},
                  {server_connects,1},
                  {server_connect_errors,1},
                  {server_fullsyncs,1},
                  {client_bytes_sent,1000},
                  {client_bytes_recv,1000},
                  {client_connects,1},
                  {client_connect_errors,1},
                  {client_redirect,1},
                  {objects_dropped_no_clients,1},
                  {objects_dropped_no_leader,1},
                  {objects_sent,1},
                  {objects_forwarded,1},
                  {elections_elected,1},
                  {elections_leader_changed,1},
                  {client_rx_kbps,[]},
                  {client_tx_kbps,[]},
                  {server_rx_kbps,[]},
                  {server_tx_kbps,[]},
                  {rt_source_errors,1},
                  {rt_sink_errors, 1}], get_stats()).

test_report() ->
    Bytes = 1024 * 60,
    Now = 10000,
    ReportMinutes = 10,
    for_n_minutes(ReportMinutes, Now, Bytes),
    %% Rates = [Stat || {Name, _Val}=Stat <- get_stats(),
    %%                  lists:member(Name, [client_rx_kbps,
    %%                                      client_tx_kbps,
    %%                                      server_rx_kbps,
    %%                                      server_tx_kbps])],
    [begin
         ?assertEqual(8, length(Values)),
         [?assertEqual(8, Val) || Val <- Values]
     end || {Name, Values} <- get_stats(),
            lists:member(Name, [client_rx_kbps,
                                client_tx_kbps,
                                server_rx_kbps,
                                server_tx_kbps])].

for_n_minutes(0, _Time, _Bytes) ->
    ok;
for_n_minutes(Minutes, Time0, Bytes) ->
    Time = tick(Time0, 60),
    [folsom_metrics:notify_existing_metric({?APP, Name},
                                           {inc, Bytes},
                                           counter) || Name <-
                                                           [client_bytes_sent,
                                                            client_bytes_recv,
                                                            server_bytes_sent,
                                                            server_bytes_recv]],

    riak_repl_stats:handle_info(report_bw, ok),
    for_n_minutes(Minutes - 1, Time, Bytes).

tick(Time, Interval) ->
    NewTime = Time+Interval,
    meck:expect(folsom_utils, now_epoch, fun() ->
                                                 NewTime end),
    NewTime.

-endif.
