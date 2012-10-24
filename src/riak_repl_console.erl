%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl_console).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([add_listener/1, del_listener/1, add_nat_listener/1]).
-export([add_site/1, del_site/1]).
-export([status/1, start_fullsync/1, cancel_fullsync/1,
         pause_fullsync/1, resume_fullsync/1]).
-export([client_stats_rpc/0, server_stats_rpc/0]).

-export([clustername/1, connections/1, clusters/1, connect/1, disconnect/1, realtime/1, fullsync/1]).

-export([get_config/0,
         leader_stats/0,
         client_stats/0,
         server_stats/0]).
-export([set_modes/1, get_modes/0]).

add_listener(Params) ->
    Ring = get_ring(),
    case add_listener_internal(Ring,Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

add_nat_listener(Params) ->
    Ring = get_ring(),
    case add_nat_listener_internal(Ring, Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

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
                    error;
                Error ->
                    io:format("Node ~p must be available to add listener: ~p\n",
                              [Listener#repl_listener.nodename, Error]),
                    error
            end;
        false ->
            io:format("~p is not a member of the cluster\n", [Listener#repl_listener.nodename]),
            error
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
                    error
            end;
        {error,_} ->
            io:format("Error adding nat address. ~n"),
            error
    end.

del_listener([NodeName, IP, Port]) ->
    Ring = get_ring(),
    Listener = make_listener(NodeName, IP, Port),
    NewRing0 = riak_repl_ring:del_listener(Ring, Listener),
    NewRing = riak_repl_ring:del_nat_listener(NewRing0, Listener),
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

set_modes(Modes) ->
    Ring = get_ring(),
    NewRing = riak_repl_ring:set_modes(Ring, Modes),
    ok = maybe_set_ring(Ring, NewRing).

get_modes() ->
    Ring = get_ring(),
    riak_repl_ring:get_modes(Ring).


status([]) ->
    status2(true);
status(quiet) ->
    status2(false).

status2(Verbose) ->
    Config = get_config(),
    Stats1 = lists:sort(riak_repl_stats:get_stats()),
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


%%
%% Repl2 commands
%%

%% TODO: cluster naming belongs in riak_core_ring, not in riak_core_connection, but
%% not until we move all of the connection stuff to core.
clustername([]) ->
    MyName = riak_core_connection:symbolic_clustername(),
    io:format("~s~n", [MyName]);
clustername([ClusterName]) ->
    riak_core_ring_manager:ring_trans(fun riak_core_connection:set_symbolic_clustername/2,
        ClusterName).

clusters([]) ->
    {ok, Clusters} = riak_core_cluster_mgr:get_known_clusters(),
    %% TODO: include our own cluster in the listing
    %% MyAddr = ???
    %% MyMembers = gen_server:call(?CLUSTER_MANAGER_SERVER, {get_my_members, MyAddr}),
    %% io:format("~-20s ~-15s ~p~n", [string_of_ipaddr(MyAddr), " ", MyMembers]),
    lists:foreach(
      fun(ClusterName) ->
              {ok,Members} = riak_core_cluster_mgr:get_ipaddrs_of_cluster(ClusterName),
              IPs = [string_of_ipaddr(Addr) || Addr <- Members],
              io:format("~s: ~p~n", [ClusterName, IPs])
      end,
      Clusters).

string_of_ipaddr({IP, Port}) ->
    lists:flatten(io_lib:format("~s:~p", [IP, Port])).

choose_best_addr({cluster_by_addr, {IP,Port}}, _ClientAddr) ->
    string_of_ipaddr({IP,Port});
choose_best_addr({cluster_by_name, _}, ClientAddr) ->
    string_of_ipaddr(ClientAddr).

string_of_remote({cluster_by_addr, {IP,Port}}) ->
    string_of_ipaddr({IP,Port});
string_of_remote({cluster_by_name, ClusterName}) ->
    ClusterName.

%% Print info about this sink
%% Remote :: {ip,port} | ClusterName
showSink({Remote,Pid}) ->
    SinkName = string_of_remote(Remote),
    PidStr = io_lib:format("~p", [Pid]),
    %% try to get status from Pid of cluster control channel.
    %% if we haven't connected successfully yet, it will time out, which we will fail
    %% fast for since it's a local process, not a remote one.
    Pid ! {self(), status},
    receive
        {Pid, connecting, SRemote} ->
            io:format("~-20s ~-20s ~-15s (connecting to ~p)~n",
                      [SinkName, "", PidStr, string_of_remote(SRemote)]);
        {Pid, status, {ClientAddr, _Transport, Name, Members}} ->
            IPs = [string_of_ipaddr(Addr) || Addr <- Members],
            CAddr = choose_best_addr(Remote, ClientAddr),
            io:format("~-20s ~-20s ~-15s ~p (via ~s)~n",
                      [SinkName, Name, PidStr,IPs, CAddr])
    after 2 ->
            io:format("~-20s ~-20s ~-15s (status timed out)~n",
                      [SinkName, "", PidStr])
    end.

connections([]) ->
    %% get cluster manager's outbound connections to other "remote" clusters,
    %% which for now, are all the "sinks".
    {ok, Conns} = riak_core_cluster_mgr:get_connections(),
    io:format("~-20s ~-20s ~-15s [Members]~n", ["Sink", "Cluster Name", "<Ctrl-Pid>"]),
    io:format("~-20s ~-20s ~-15s ---------~n", ["----", "------------", "----------"]),
    [showSink(Conn) || Conn <- Conns].

connect([Address]) ->
    NWords = string:words(Address, $:),
    case NWords of
        2 ->
            IP = string:sub_word(Address, 1, $:),
            PortStr = string:sub_word(Address, 2, $:),
            connect([IP, PortStr]);
        _ ->
            {error, {badarg, Address}}
    end;
connect([IP, PortStr]) ->
    {Port,_Rest} = string:to_integer(PortStr),
    riak_core_cluster_mgr:add_remote_cluster({IP, Port}).

%% remove a remote connection by clustername or by IP/Port address:
%% clustername
%% | ip:port
%% | ip port
disconnect([Address]) ->
    NWords = string:words(Address, $:),
    case NWords of
        1 ->
            riak_core_cluster_mgr:remove_remote_cluster(Address);
        2 ->
            IP = string:sub_word(Address, 1, $:),
            PortStr = string:sub_word(Address, 2, $:),
            disconnect([IP, PortStr]);
        _ ->
            {error, {badarg, Address}}
    end;
disconnect([IP, PortStr]) ->
    {Port,_Rest} = string:to_integer(PortStr),
    riak_core_cluster_mgr:remove_remote_cluster({IP, Port}).

realtime([Cmd, Remote]) ->
    case Cmd of
        "enable" ->
            riak_repl2_rt:enable(Remote);
        "disable" ->
            riak_repl2_rt:disable(Remote);
        "start" ->
            riak_repl2_rt:start(Remote);
        "stop" ->
            riak_repl2_rt:stop(Remote)
    end;
realtime([Cmd]) ->
    Remotes = riak_repl2_rt:enabled(),
    case Cmd of
        "start" ->
            [riak_repl2_rt:start(Remote) || Remote <- Remotes];
        "stop" ->
            [riak_repl2_rt:stop(Remote) || Remote <- Remotes]
    end.

fullsync([Cmd, Remote]) ->
    io:format("TODO: implement fullsync ~s ~s~n", [Cmd, Remote]);
fullsync([Cmd]) ->
    io:format("TODO: implement fullsync ~s~n", [Cmd]).


%% helper functions

format_counter_stats([]) -> ok;
format_counter_stats([{K,V}|T]) when is_list(K) ->
    io:format("~s: ~p~n", [K,V]),
    format_counter_stats(T);
format_counter_stats([{K,V}|T]) when K == client_rx_kbps;
                                     K == client_tx_kbps;
                                     K == server_rx_kbps;
                                     K == server_tx_kbps ->
    io:format("~s: ~w~n", [K,V]),
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
    {Stats, _BadNodes} = rpc:multicall(riak_core_node_watcher:nodes(riak_kv), riak_repl_console, client_stats_rpc, []),
    [{client_stats, lists:flatten(lists:filter(fun({badrpc, _}) ->
                false;
            (_) -> true
        end, Stats))}].

client_stats_rpc() ->
    RT2 = [rt2_sink_stats(P) || P <- riak_repl2_rt:get_sink_pids()],
    Pids = [P || {_,P,_,_} <- supervisor:which_children(riak_repl_client_sup), P /= undefined],
    [client_stats(P) || P <- Pids] ++ RT2.

server_stats() ->
    RT2 = [rt2_source_stats(P) || {_R,P} <- riak_repl2_rtsource_conn_sup:enabled()],
    LeaderNode = riak_repl_leader:leader_node(),
    case LeaderNode of
        undefined ->
            [{server_stats, RT2}];
        _ ->
            [{server_stats, rpc:call(LeaderNode, ?MODULE, server_stats_rpc, [])++RT2}]
    end.

server_stats_rpc() ->
    [server_stats(P) || P <- server_pids()].
    
client_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl_tcp_client:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

server_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl_tcp_server:status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

rt2_source_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl2_rtsource_conn:legacy_status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

rt2_sink_stats(Pid) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    State = try
                riak_repl2_rtsink_conn:legacy_status(Pid, Timeout)
            catch
                _:_ ->
                    too_busy
            end,
    {Pid, erlang:process_info(Pid, message_queue_len), State}.

server_pids() ->
    %%%%%%%%
    %% NOTE:
    %% This is needed because Ranch doesn't directly expose child PID's.
    %% However, digging into the Ranch supervision tree can cause problems in the
    %% future if Ranch is upgraded. Ideally, this code should be moved into
    %% Ranch. Please check for Ranch updates!
    %%%%%%%%
    [Pid2 ||
        {{ranch_listener_sup, _}, Pid, _Type, _Modules} <- supervisor:which_children(ranch_sup), is_pid(Pid),
        {ranch_conns_sup,Pid1,_,_} <- supervisor:which_children(Pid),
        {_,Pid2,_,_} <- supervisor:which_children(Pid1)].
