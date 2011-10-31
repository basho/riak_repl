%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_listener_sup).
-author('Andy Gross <andy@basho.com>').
-behaviour(supervisor).
-include("riak_repl.hrl").
-export([start_link/0, init/1, stop/1]).
-export([start_listener/1, ensure_listeners/1]).

start_listener(#repl_listener{listen_addr={IP, Port}}) ->
    case riak_repl_util:valid_host_ip(IP) of
        true ->
            lager:info("Starting replication listener on ~s:~p",
                [IP, Port]),
            supervisor:start_child(?MODULE, [IP, Port]);
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
    CurrentConfig = [begin Res = riak_repl_listener:config(Pid), {Pid, Res} end||
        {_Id, Pid, _Type, _Modules} <- supervisor:which_children(?MODULE),
        is_pid(Pid)],
    CurrentListeners = lists:map(fun({_Pid, Listener}) -> Listener end,
        CurrentConfig),
    ConfiguredListeners = dict:fetch(listeners, ReplConfig),
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
                case lists:keyfind(Listener, 2, CurrentConfig) of
                    {Pid, Listener} ->
                        riak_repl_listener:stop(Pid);
                    _ ->
                        ok
                end
        end, ToStop),
    ok.

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplConfig = 
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            riak_repl_ring:initial_config();
        RC -> RC
    end,
    [start_listener(Listener) || Listener <- dict:fetch(listeners, ReplConfig)],
    {ok, Pid}.


stop(_S) -> ok.

%% @private
init([]) ->
    {ok, 
     {{simple_one_for_one, 100, 10}, 
      [{undefined,
        {riak_repl_listener, start_link, []},
        transient, brutal_kill, worker, [riak_repl_listener]}]}}.
