%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_listener_sup).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([start_listener/1, ensure_listeners/1, close_all_connections/0]).

start_listener(Listener = #repl_listener{listen_addr={IP, Port}}) ->
    case riak_repl_util:valid_host_ip(IP) of
        true ->
            lager:info("Starting replication listener on ~s:~p",
                [IP, Port]),
            %supervisor:start_child(?MODULE, [IP, Port]);
            {ok, RawAddress} = inet_parse:address(IP),
            ranch:start_listener(Listener, 10, ranch_tcp,
                [{ip, RawAddress}, {port, Port}], riak_repl_tcp_server, []);
        _ ->
            lager:error("Cannot start replication listener "
                "on ~s:~p - invalid address.",
                [IP, Port])
    end.

ensure_listeners(Ring) ->
    ReplConfig = 
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            riak_repl_ring:initial_config();
        RC -> RC
    end,
    CurrentListeners = [L ||
        {{_, L}, Pid, _Type, _Modules} <- supervisor:which_children(ranch_sup),
        is_pid(Pid)],
    ConfiguredListeners = [Listener || Listener <- dict:fetch(listeners, ReplConfig),
        Listener#repl_listener.nodename == node()],
    ToStop = sets:to_list(
               sets:subtract(
                 sets:from_list(CurrentListeners), 
                 sets:from_list(ConfiguredListeners))),
    ToStart = sets:to_list(
               sets:subtract(
                 sets:from_list(ConfiguredListeners), 
                 sets:from_list(CurrentListeners))),
    [start_listener(Listener) || Listener <- ToStart],
    lists:foreach(fun(Listener) ->
                {IP, Port} = Listener#repl_listener.listen_addr,
                lager:info("Stopping replicaion listener on ~s:~p",
                    [IP, Port]),
                ranch:stop_listener(Listener)
        end, ToStop),
    ok.

close_all_connections() ->
    [exit(P, kill) || {_, P, _, _} <-
        supervisor:which_children(riak_repl_server_sup)]. 
