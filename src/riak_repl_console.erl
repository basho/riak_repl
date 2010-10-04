%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_console).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([add_listener/1, del_listener/1]).
-export([add_site/1, del_site/1]).
-export([status/1, start_fullsync/1, stop_fullsync/1]).

add_listener([NodeName, IP, Port]) ->
    Ring = get_ring(),
    Listener = make_listener(NodeName, IP, Port),
    NewRing = riak_repl_ring:add_listener(Ring, Listener),
    ok = maybe_set_ring(Ring, NewRing).

del_listener([NodeName, IP, Port]) ->
    Ring = get_ring(),
    Listener = make_listener(NodeName, IP, Port),
    NewRing = riak_repl_ring:del_listener(Ring, Listener),
    ok = maybe_set_ring(Ring, NewRing).

add_site([IP, Port, SiteName]) ->
    Ring = get_ring(),
    Site = make_site(SiteName, IP, Port),
    NewRing = riak_repl_ring:add_site(Ring, Site),
    ok = maybe_set_ring(Ring, NewRing).

del_site([SiteName]) ->
    Ring = get_ring(),
    NewRing = riak_repl_ring:del_site(Ring, SiteName),
    ok = maybe_set_ring(Ring, NewRing).

status([]) ->
    Config = get_config(),
    Stats1 = lists:sort(ets:tab2list(riak_repl_stats)),
    LeaderStats = leader_stats(),
    ClientStats = client_stats(),
    ServerStats = server_stats(),
    format_counter_stats(Config++Stats1++LeaderStats++ClientStats++ServerStats).

start_fullsync([]) ->
    [riak_repl_tcp_server:start_fullsync(Pid) || Pid <- server_pids()],
    ok.

stop_fullsync([]) ->
    [riak_repl_tcp_server:stop_fullsync(Pid) || Pid <- server_pids()],
    ok.

%% helper functions

format_counter_stats([]) -> ok;
format_counter_stats([{K,V}|T]) when is_list(K) ->
    io:format("~s: ~p~n", [K,V]),
    format_counter_stats(T);
format_counter_stats([{K,V}|T]) ->
    io:format("~p: ~p~n", [K,V]),
    format_counter_stats(T).

make_listener(NodeName, IP, Port) ->
    #repl_listener{nodename=list_to_atom(NodeName),
                   listen_addr={IP, list_to_integer(Port)}}.

make_site(SiteName, IP, Port) ->
    #repl_site{name=SiteName, addrs=[{IP, list_to_integer(Port)}]}.

maybe_set_ring(_R, _R) -> ok;
maybe_set_ring(_R1, R2) ->
    RC = riak_repl_ring:get_repl_config(R2),
    F = fun(InRing, ReplConfig) ->
                {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
        end,
    RC = riak_repl_ring:get_repl_config(R2),
    riak_core_ring_manager:ring_trans(F, RC),
    ok.

get_ring() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:ensure_config(Ring).

get_config() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_repl_config(R) of
        undefined ->
            [];
        Repl ->
            case dict:find(sites, Repl) of
                error ->
                    [];
                {ok, Sites} ->
                    lists:flatten([format_site(S) || S <- Sites])
            end ++
            case dict:find(listeners, Repl) of
                error ->
                    [];
                {ok, Listeners} ->
                    lists:flatten([format_listener(L) || L <- Listeners])
            end
    end.

format_site(S) ->
    [{S#repl_site.name ++ "_ips", format_ips(S#repl_site.addrs)},
     {S#repl_site.name ++ "_last_sync", calendar:now_to_local_time(S#repl_site.last_sync)}].
        
format_ips(IPs) ->
    string:join([format_ip(IP) || IP <- IPs], ", ").

format_ip({Addr,Port}) ->
    lists:flatten(io_lib:format("~s:~p", [Addr, Port])).

format_listener(L) ->
    [{"listener_" ++ atom_to_list(L#repl_listener.nodename),
      format_ip(L#repl_listener.listen_addr)}].

leader_stats() ->
    try
        LeaderNode = riak_repl_leader:leader_node(),
        LeaderPid = rpc:call(LeaderNode, erlang, whereis, [riak_repl_leader]),
        LeaderStats = rpc:call(LeaderNode, erlang, process_info, [LeaderPid, [message_queue_len,
                                                                              total_heap_size,
                                                                              heap_size,
                                                                              stack_size,
                                                                              reductions,
                                                                              garbage_collection]]),
        [{leader, LeaderNode}] ++ [{"leader_" ++  atom_to_list(K), V} || {K,V} <- LeaderStats]
    catch
        _:_ ->
            []
    end.

client_stats() ->
    Pids = [P || {_,P,_,_} <- supervisor:which_children(riak_repl_client_sup), P /= undefined],
    [{client_stats, [{P, erlang:process_info(P, message_queue_len)} || P <- Pids]}].

server_stats() ->
    [{server_stats, [{P, erlang:process_info(P, message_queue_len)} || 
                        P <- server_pids()]}].
    
server_pids() ->
    [P || {_,P,_,_} <- supervisor:which_children(riak_repl_server_sup), P /= undefined].
