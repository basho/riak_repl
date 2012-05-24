%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_console).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([add_listener/1, del_listener/1, add_nat_listener/1]).
-export([add_site/1, del_site/1]).
-export([status/1, start_fullsync/1, cancel_fullsync/1,
         pause_fullsync/1, resume_fullsync/1]).

add_listener(Params) ->
    Ring = get_ring(),
    {ok, NewRing} = add_listener_internal(Ring,Params),
    ok = maybe_set_ring(Ring, NewRing).

add_nat_listener(Params) ->
    Ring = get_ring(),
    {ok, NewRing} = add_nat_listener_internal(Ring, Params),
    ok = maybe_set_ring(Ring, NewRing).

add_listener_internal(Ring, [NodeName, IP, Port]) ->
    Listener = make_listener(NodeName, IP, Port),
    case lists:member(Listener#repl_listener.nodename, riak_core_ring:all_members(Ring)) of
        true ->
            case catch rpc:call(Listener#repl_listener.nodename,
                                riak_repl_util, valid_host_ip, [IP]) of
                true ->
                    NewRing = riak_repl_ring:add_listener(Ring, Listener),
                    {ok,NewRing}; 
                false ->
                    io:format("~p is not a valid IP address for ~p\n",
                              [IP, Listener#repl_listener.nodename]),
                    {error,Ring};
                Error ->
                    io:format("Node ~p must be available to add listener: ~p\n",
                              [Listener#repl_listener.nodename, Error]),
                    {error,Ring}
            end;
        false ->
            io:format("~p is not a member of the cluster\n", [Listener#repl_listener.nodename]),
            {error, Ring}
    end.

add_nat_listener_internal(Ring, [NodeName, IP, Port, PublicIP, PublicPort]) ->
    case add_listener_internal(Ring, [NodeName, IP, Port]) of
        {ok,NewRing} ->
            case inet_parse:address(PublicIP) of
                {ok, _} ->
                    NatListener = make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort),
                    NewRing2 = riak_repl_ring:add_nat_listener(NewRing, NatListener),
                    {ok, NewRing2};
                {error, IPParseError} ->
                    io:format("Invalid NAT IP address: ~p~n", [IPParseError]),
                    {error, Ring};
                {_,Error} ->
                    io:format("Error adding NAT Listener: ~s~n",[Error]),
                    {error, Ring}
            end;
        {error,_} ->
            io:format("Error adding nat address. ~n"),
            {error, Ring}
    end.

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
    status2(true);
status(quiet) ->
    status2(false).

status2(Verbose) ->
    Config = get_config(),
    Stats1 = lists:sort(ets:tab2list(riak_repl_stats)),
    LeaderStats = leader_stats(),
    ClientStats = client_stats(),
    ServerStats = server_stats(),
    All = Config++Stats1++LeaderStats++ClientStats++ServerStats,
    if Verbose ->
            format_counter_stats(All);
       true ->
            All
    end.

start_fullsync([]) ->
    [riak_repl_tcp_server:start_fullsync(Pid) || Pid <- server_pids()],
    ok.

cancel_fullsync([]) ->
    [riak_repl_tcp_server:cancel_fullsync(Pid) || Pid <- server_pids()],
    ok.

pause_fullsync([]) ->
    [riak_repl_tcp_server:pause_fullsync(Pid) || Pid <- server_pids()],
    ok.

resume_fullsync([]) ->
    [riak_repl_tcp_server:resume_fullsync(Pid) || Pid <- server_pids()],
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

make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort) ->
    #nat_listener{nodename=list_to_atom(NodeName),
                listen_addr={IP, list_to_integer(Port)},
                nat_addr={PublicIP, list_to_integer(PublicPort)}}.


make_site(SiteName, IP, Port) ->
    #repl_site{name=SiteName, addrs=[{IP, list_to_integer(Port)}]}.

maybe_set_ring(_R, _R) -> ok;
maybe_set_ring(_R1, R2) ->
    RC = riak_repl_ring:get_repl_config(R2),
    F = fun(InRing, ReplConfig) ->
                {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
        end,
    RC = riak_repl_ring:get_repl_config(R2),
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, RC),
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
            end ++ 
            case dict:find(natlisteners, Repl) of
                error ->
                    [];
                {ok, NatListeners} ->
                    lists:flatten([format_nat_listener(L) || L <- NatListeners])
            end
    end.

format_site(S) ->
    [{S#repl_site.name ++ "_ips", format_ips(S#repl_site.addrs)}].
        
format_ips(IPs) ->
    string:join([format_ip(IP) || IP <- IPs], ", ").

format_ip({Addr,Port}) ->
    lists:flatten(io_lib:format("~s:~p", [Addr, Port])).

format_listener(L) ->
    [{"listener_" ++ atom_to_list(L#repl_listener.nodename),
      format_ip(L#repl_listener.listen_addr)}].

format_nat_listener(L) ->
    [{"natlistener_" ++ atom_to_list(L#nat_listener.nodename),
      format_ip(L#nat_listener.listen_addr) ++ "->" ++ 
      format_ip(L#nat_listener.nat_addr)}].

leader_stats() ->
    LeaderNode = riak_repl_leader:leader_node(),
    LocalStats = 
        try
            LocalProcInfo = erlang:process_info(whereis(riak_repl_leader_gs),
                                                [message_queue_len, heap_size]),
            [{"local_leader_" ++  atom_to_list(K), V} || {K,V} <- LocalProcInfo]
        catch _:_ ->
                []
        end,
    RemoteStats =
        try
            LeaderPid = rpc:call(LeaderNode, erlang, whereis,
                [riak_repl_leader_gs]),
            LeaderStats = rpc:call(LeaderNode, erlang, process_info,
                                   [LeaderPid, [message_queue_len,
                                                total_heap_size,
                                                heap_size,
                                                stack_size,
                                                reductions,
                                                garbage_collection]]),
            [{"leader_" ++  atom_to_list(K), V} || {K,V} <- LeaderStats]
        catch
            _:_ ->
                []
        end,
    [{leader, LeaderNode}] ++ RemoteStats ++ LocalStats.

client_stats() ->
    Pids = [P || {_,P,_,_} <- supervisor:which_children(riak_repl_client_sup), P /= undefined],
    [{client_stats, [client_stats(P) || P <- Pids]}].

server_stats() ->
    [{server_stats, [server_stats(P) || P <- server_pids()]}].

client_stats(Pid) ->
    State = try
                riak_repl_tcp_client:status(Pid, 1000)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.
    

server_stats(Pid) ->
    %% try and work out what state the TCP server is in.  In the middle
    %% of merkle generation etc this could take a long time, so punt with a
    %% too_busy message rather than hold up status
    State = try
                riak_repl_tcp_server:status(Pid, 1000)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.
                        
server_pids() ->
    [P || {_,P,_,_} <- supervisor:which_children(riak_repl_server_sup), P /= undefined].
