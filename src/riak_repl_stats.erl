%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_stats).
-author('Andy Gross <andy@basho.com>').
-behaviour(gen_server).
-include("riak_repl.hrl").
-export([init/1, 
         handle_call/3, 
         handle_cast/2, 
         handle_info/2,
         terminate/2, 
         code_change/3]).
-export([start_link/0,
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
         get_stats/0]).

-define(APP_NAME, "riak_repl_").

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

register_stats() ->
    [register_stat(Name, Type) || {Name, Type} <- stats()],
    folsom_metrics:notify_existing_metric(riak_repl_last_report, now(), gauge).

client_bytes_sent(Bytes) ->
    increment_counter(riak_repl_client_bytes_sent, Bytes).

client_bytes_recv(Bytes) ->
    increment_counter(riak_repl_client_bytes_recv, Bytes).

client_connects() ->
    increment_counter(riak_repl_client_connects).
    
client_connect_errors() ->
    increment_counter(riak_repl_client_connect_errors).

client_redirect() ->
    increment_counter(riak_repl_client_redirect).

server_bytes_sent(Bytes) ->
    increment_counter(riak_repl_server_bytes_sent, Bytes).

server_bytes_recv(Bytes) ->
    increment_counter(riak_repl_server_bytes_recv, Bytes).

server_connects() ->
    increment_counter(riak_repl_server_connects).

server_connect_errors() ->
    increment_counter(riak_repl_server_connect_errors).

server_fullsyncs() ->
    increment_counter(riak_repl_server_fullsyncs).

objects_dropped_no_clients() ->
    increment_counter(riak_repl_objects_dropped_no_clients).

objects_dropped_no_leader() ->
    increment_counter(riak_repl_objects_dropped_no_leader).

objects_sent() ->
    increment_counter(riak_repl_objects_sent).

objects_forwarded() ->
    increment_counter(riak_repl_objects_forwarded).

elections_elected() ->
    increment_counter(riak_repl_elections_elected).

elections_leader_changed() ->
    increment_counter(riak_repl_elections_leader_changed).

get_stats() ->
    lists:flatten([backwards_compat(Stat, Type) ||
        {Stat, Type} <- stats()]).

init([]) -> 
    schedule_report_bw(),
    {ok, ok}.

register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name);
register_stat(Name, history) ->
    BwHistoryLen =  get_bw_history_len(),
    folsom_metrics:new_history(Name, BwHistoryLen);
register_stat(Name, gauge) ->
    folsom_metrics:new_gauge(Name).

stats() ->
    [{riak_repl_server_bytes_sent, counter},
     {riak_repl_server_bytes_recv, counter},
     {riak_repl_server_connects, counter},
     {riak_repl_server_connect_errors, counter},
     {riak_repl_server_fullsyncs, counter},
     {riak_repl_client_bytes_sent, counter},
     {riak_repl_client_bytes_recv, counter},
     {riak_repl_client_connects, counter},
     {riak_repl_client_connect_errors, counter},
     {riak_repl_client_redirect, counter},
     {riak_repl_objects_dropped_no_clients, counter},
     {riak_repl_objects_dropped_no_leader, counter},
     {riak_repl_objects_sent, counter},
     {riak_repl_objects_forwarded, counter},
     {riak_repl_elections_elected, counter},
     {riak_repl_elections_leader_change, counter},
     {riak_repl_client_rx_kbps, history},
     {riak_repl_client_tx_kbps, history},
     {riak_repl_server_rx_kbps, history},
     {riak_repl_server_tx_kbps, history},
     {riak_repl_last_report, gauge},
     {riak_repl_last_client_bytes_sent, gauge},
     {riak_repl_last_client_bytes_recv, gauge},
     {riak_repl_last_server_bytes_sent, gauge},
     {riak_repl_last_server_bytes_recv, gauge}].

increment_counter(Name) ->
    increment_counter(Name, 1).

increment_counter(Name, IncrBy) when is_atom(Name) andalso is_integer(IncrBy) ->
    folsom_metrics:notify_existing_metric(Name, {inc, IncrBy}, counter).

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(report_bw, State) ->
    ThisClientBytesSent=lookup_stat(riak_repl_client_bytes_sent),
    ThisClientBytesRecv=lookup_stat(riak_repl_client_bytes_recv),
    ThisServerBytesSent=lookup_stat(riak_repl_server_bytes_sent),
    ThisServerBytesRecv=lookup_stat(riak_repl_server_bytes_recv),

    Now = now(),
    DeltaSecs = now_diff(Now, lookup_stat(riak_repl_last_report)),
    ClientTx = bytes_to_kbits_per_sec(ThisClientBytesSent, lookup_stat(riak_repl_last_client_bytes_sent), DeltaSecs),
    ClientRx = bytes_to_kbits_per_sec(ThisClientBytesRecv, lookup_stat(riak_repl_last_client_bytes_recv), DeltaSecs),
    ServerTx = bytes_to_kbits_per_sec(ThisServerBytesSent, lookup_stat(riak_repl_last_server_bytes_sent), DeltaSecs),
    ServerRx = bytes_to_kbits_per_sec(ThisServerBytesRecv, lookup_stat(riak_repl_last_server_bytes_recv), DeltaSecs),

    [folsom_metrics:notify_existing_metric(Metric, Reading, history)
     || {Metric, Reading} <- [{riak_repl_client_tx_kbps, ClientTx},
                              {riak_repl_client_rx_kbps, ClientRx},
                              {riak_repl_server_tx_kbps, ServerTx},
                              {riak_repl_server_rx_kbps, ServerRx}]],

    [folsom_metrics:notify_existing_metric(Metric, Reading, gauge)
     || {Metric, Reading} <- [{riak_repl_last_client_bytes_sent, ThisClientBytesSent},
                              {riak_repl_last_client_bytes_recv, ThisClientBytesRecv},
                              {riak_repl_last_server_bytes_sent, ThisServerBytesSent},
                              {riak_repl_last_server_bytes_recv, ThisServerBytesRecv}]],
    
    schedule_report_bw(),
    {noreply, State};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule_report_bw() ->
    BwHistoryInterval = app_helper:get_env(riak_repl, bw_history_interval, 60000),
    timer:send_after(BwHistoryInterval, report_bw).

%% Convert two values in bytes to a kbits/sec
bytes_to_kbits_per_sec(This, Last, Delta) ->
    trunc((This - Last) / (128 * Delta)).  %% x8/1024 = x/128

lookup_stat(Name) ->
    folsom_metrics:get_metric_value(Name).

now_diff({Lmega,Lsecs,_Lmicro}, {Emega,Esecs,_Emicro}) ->
    1000000*(Lmega-Emega)+(Lsecs-Esecs).

get_bw_history_len() ->
    app_helper:get_env(riak_repl, bw_history_len, 8).

backwards_compat(Name, history) ->
    Stats = folsom_metrics:get_history_values(Name, get_bw_history_len()),
    Readings = [[Reading || {event, Reading} <- Events] || {_Moment, Events} <- Stats],
    {un_namespace(Name), Readings};
backwards_compat(_Name, gauge) ->
    [];
backwards_compat(Name,  _Type) ->
    {un_namespace(Name), lookup_stat(Name)}.

un_namespace(Name) ->
    list_to_atom(lists:subtract(atom_to_list(Name), ?APP_NAME)).
