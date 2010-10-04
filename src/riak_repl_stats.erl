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
         server_bytes_sent/1,
         server_bytes_recv/1,
         server_connects/0,
         server_connect_errors/0,
         server_fullsyncs/0,
         add_counter/1,
         add_counter/2,
         increment_counter/1,
         increment_counter/2]).
-record(state, {t,
                last_report,
                last_client_bytes_sent=0,
                last_client_bytes_recv=0,
                last_server_bytes_sent=0,
                last_server_bytes_recv=0
               }).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

client_bytes_sent(Bytes) ->
    increment_counter(client_bytes_sent, Bytes).

client_bytes_recv(Bytes) ->
    increment_counter(client_bytes_recv, Bytes).

client_connects() ->
    increment_counter(client_connects).
    
client_connect_errors() ->
    increment_counter(client_connect_errors).

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

init([]) -> 
    T = ets:new(?MODULE, [public, named_table, set, {write_concurrency, true}]),
    [ets:insert(T, {Stat, 0}) || Stat <- [server_bytes_sent,
                                          server_bytes_recv,
                                          server_connects,
                                          server_connect_errors,
                                          server_fullsyncs,
                                          client_bytes_sent,
                                          client_bytes_recv,
                                          client_connects,
                                          client_connect_errors]],
    [ets:insert(T, {Stat, []}) || Stat <- [client_rx_kbps,
                                           client_tx_kbps,
                                           server_rx_kbps,
                                           server_tx_kbps]],
    schedule_report_bw(),
    {ok, #state{t=T,last_report=now()}}.

add_counter(Name) ->
    add_counter(Name, 0).

add_counter(Name, InitVal) when is_atom(Name) andalso is_integer(InitVal) ->
    gen_server:call(?MODULE, {add_counter, Name, InitVal}, infinity).

increment_counter(Name) ->
    increment_counter(Name, 1).

increment_counter(Name, IncrBy) when is_atom(Name) andalso is_integer(IncrBy) ->
    %gen_server:cast(?MODULE, {increment_counter, Name, IncrBy}).
    catch ets:update_counter(?MODULE, Name, IncrBy).

handle_call({add_counter, Name, InitVal}, _From, State=#state{t=T}) -> 
    ets:insert(T, {Name, InitVal}),
    {reply, ok, State}.
handle_cast({increment_counter, Name, IncrBy}, State=#state{t=T}) -> 
    catch ets:update_counter(T, Name, IncrBy),
    {noreply, State}.

handle_info(report_bw, State=#state{last_client_bytes_sent=LastClientBytesSent,
                                    last_client_bytes_recv=LastClientBytesRecv,
                                    last_server_bytes_sent=LastServerBytesSent,
                                    last_server_bytes_recv=LastServerBytesRecv}) ->
    ThisClientBytesSent=lookup_stat(client_bytes_sent),
    ThisClientBytesRecv=lookup_stat(client_bytes_recv),
    ThisServerBytesSent=lookup_stat(server_bytes_sent),
    ThisServerBytesRecv=lookup_stat(server_bytes_recv),

    Now = now(),
    DeltaSecs = now_diff(Now, State#state.last_report),
    ClientTx = bytes_to_kbits_per_sec(ThisClientBytesSent, LastClientBytesSent, DeltaSecs),
    ClientRx = bytes_to_kbits_per_sec(ThisClientBytesRecv, LastClientBytesRecv, DeltaSecs),
    ServerTx = bytes_to_kbits_per_sec(ThisServerBytesSent, LastServerBytesSent, DeltaSecs),
    ServerRx = bytes_to_kbits_per_sec(ThisServerBytesRecv, LastServerBytesRecv, DeltaSecs),

    BwHistoryLen = app_helper:get_env(riak_repl, bw_history_len, 8),

    update_list(client_tx_kbps, ClientTx, BwHistoryLen, State#state.t),
    update_list(client_rx_kbps, ClientRx, BwHistoryLen, State#state.t),
    update_list(server_tx_kbps, ServerTx, BwHistoryLen, State#state.t),
    update_list(server_rx_kbps, ServerRx, BwHistoryLen, State#state.t),
    
    schedule_report_bw(),
    {noreply, State#state{last_report = Now,
                          last_client_bytes_sent = ThisClientBytesSent,
                          last_client_bytes_recv = ThisClientBytesRecv,
                          last_server_bytes_sent = ThisServerBytesSent,
                          last_server_bytes_recv = ThisServerBytesRecv}};
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

schedule_report_bw() ->
    BwHistoryInterval = app_helper:get_env(riak_repl, bw_history_interval, 60000),
    timer:send_after(BwHistoryInterval, report_bw).
   

%% Convert two values in bytes to a kbits/sec
bytes_to_kbits_per_sec(This, Last, Delta) ->
    trunc((This - Last) / (128 * Delta)).  %% x8/1024 = x/128

update_list(Name, Entry, MaxLen, Tid) ->
    Current = lookup_stat(Name),
    Updated = [Entry | lists:sublist(Current, MaxLen-1)],
    ets:insert(Tid, {Name, Updated}).

lookup_stat(Name) ->
    [{Name,Val}]=ets:lookup(?MODULE, Name),
    Val.

now_diff({Lmega,Lsecs,_Lmicro}, {Emega,Esecs,_Emicro}) ->
    1000000*(Lmega-Emega)+(Lsecs-Esecs).
         
