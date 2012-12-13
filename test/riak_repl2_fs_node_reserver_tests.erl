%% excersize the riak_repl2_fs_node_reserver

-module(riak_repl2_fs_node_reserver_tests).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {
    %running_sinks = 0,
    max_sinks = 2,
    reservations = [],
    partitions = []
}).

eqc_test_() ->
    {setup, fun() ->
        application:load(riak_core),
        {ok, RingEvtPid} = riak_core_ring_events:start_link(),
        unlink(RingEvtPid),
        {ok, MgrPid} = riak_core_ring_manager:start_link(test),
        unlink(MgrPid),
        {MgrPid, RingEvtPid}
    end,
    fun({MgrPid, RingEvtPid}) ->
        stop_fake_sink_sup(),
        wait_for_pid_exit(riak_repl2_fssink_sup),
        wait_for_pid_exit(MgrPid),
        wait_for_pid_exit(RingEvtPid),
        ok
    end,
    fun(_) -> [
        {"snip1", ?_assertEqual([], lists_snip(1, "a"))},
        {"snip2", ?_assertEqual("a", lists_snip(2, "ab"))},
        {"snip3", ?_assertEqual("b", lists_snip(1, "ab"))},
        {"snip4", ?_assertEqual("bc", lists_snip(1, "abc"))},
        {"snip5", ?_assertEqual("ac", lists_snip(2, "abc"))},
        {"snip6", ?_assertEqual("ab", lists_snip(3, "abc"))},
        {"snip7", ?_assertEqual("abde", lists_snip(3, "abcde"))},

        {"proptest", timeout, 60000, fun() ->
            ?assertEqual([], module(?MODULE))
        end}
    ] end}.

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
            {H, State, Res} = run_commands(?MODULE,Cmds),
                ?WHENFAIL(begin
                    io:format("=== TEST FAILED ===~n~n"
                        "=== History ===~n"
                        "~p~n~n"
                        "=== State ===~n"
                        "~p~n~n"
                        "=== Result ===~n"
                        "~p~n~n",
                        [H, State, Res])
                end, Res == ok)
        end).

%% ====================================================================
%% eqc_statem generators
%% ====================================================================

initial_state() ->
    restart_fake_sink_sup(),
    restart_node_reserver(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    Parts = [P || {P, _} <- Owners],
    #state{partitions = Parts}.

command(S) ->
    oneof([
        {call, ?MODULE, claim_reservation, [g_reservation(S), S]},
        {call, ?MODULE, reserve_partition, [g_partition(S)]},
        {call, ?MODULE, unreserve_partition, [g_reservation(S), S]},
        {call, ?MODULE, timeout_reservation, [g_reservation(S), S]},
        {call, ?MODULE, exit_sink, []}
    ]).

precondition(_S, {call, _, reserve_partition, _}) ->
    true;

precondition(_S, {call, _, exit_sink, []}) ->
    Kids = riak_repl2_fssink_sup:started(),
    length(Kids) > 0;

precondition(S, {call, _, _, [Nth, _]}) ->
    length(S#state.reservations) > 0 andalso Nth =< length(S#state.reservations) .

g_reservation(#state{reservations = []}) ->
    badarg;
g_reservation(#state{reservations = [_R]}) ->
    1;
g_reservation(#state{reservations = R}) ->
    Max = length(R),
    choose(1, Max).

g_partition(#state{partitions = Parts}) ->
    oneof(Parts).

next_state(S, _Res, {call, _, reserve_partition, [Part]}) ->
    #state{reservations = Reserves, max_sinks = Max} = S,
    Active = length(riak_repl2_fssink_sup:started()),
    Reserved = length(Reserves),
    Total = Reserved + Active,
    Reserves2 = if
        Total < Max ->
            [Part | S#state.reservations];
        true ->
            S#state.reservations
    end,
    S#state{reservations = Reserves2};

next_state(S, _Res, {call, _, _, [Nth, _]}) ->
    Reserves = lists_snip(Nth, S#state.reservations),
    S#state{reservations = Reserves};

next_state(S, _Res, _) ->
    S.

maybe_include_partition(Part, ok, Reserves) ->
    [Part | Reserves];
maybe_include_partition(_Part, busy, Reserved) ->
    Reserved.

%% ====================================================================
%% eqc_statem tests
%% ====================================================================

claim_reservation(Nth, S) ->
    Part = lists:nth(Nth, S#state.reservations),
    riak_repl2_fs_node_reserver:claim_reservation(Part),
    start_fake_sink().

reserve_partition(Part) ->
    riak_repl2_fs_node_reserver:reserve(Part).

unreserve_partition(Nth, S) ->
    Part = lists:nth(Nth, S#state.reservations),
    riak_repl2_fs_node_reserver:unreserve(Part),
    gen_server:call(riak_repl2_fs_node_reserver, state).

timeout_reservation(Nth, S) ->
    Part = lists:nth(Nth, S#state.reservations),
    riak_repl2_fs_node_reserver ! {reservation_expired, Part}.

exit_sink() ->
    [DeathRow | _] = riak_repl2_fssink_sup:started(),
    wait_for_pid_exit(DeathRow).

%% ====================================================================
%% Did it work?
%% ====================================================================

postcondition(S, {call, _, reserve_partition, _}, Res) ->
    #state{reservations = Reserves, max_sinks = Max} = S,
    Active = length(riak_repl2_fssink_sup:started()),
    Reserved = length(Reserves),
    Total = Reserved + Active,
    case Res of
        ok when Total < Max ->
            true;
        busy when Total =< Max ->
            true;
        _ ->
            false
    end;

postcondition(_, _, _) ->
    true.

%% ====================================================================
%% Internal functions
%% ====================================================================

restart_fake_sink_sup() ->
    stop_fake_sink_sup(),
    start_fake_sink_sup().

restart_node_reserver() ->
    case whereis(riak_repl2_fs_node_reserver) of
        Pid when is_pid(Pid) ->
            wait_for_pid_exit(Pid);
        _ ->
            ok
    end,
    start_node_reserver().

start_fake_sink_sup() ->
    meck:new(riak_repl2_fssink_sup, [passthrough]),
    meck:expect(riak_repl2_fssink_sup, init, fun(_) ->
        {ok, {{simple_one_for_one, 3, 5}, [
            {id, {?MODULE, start_zombie, []}, transient, brutal_kill, worker, [riak_repl2_fssink_sup]}
        ]}}
    end),
    {ok, Pid} = riak_repl2_fssink_sup:start_link(),
    unlink(Pid).

stop_fake_sink_sup() ->
    case whereis(riak_repl2_fssink_sup) of
        Pid when is_pid(Pid) ->
            wait_for_pid_exit(Pid);
        _ ->
            ok
    end,
    catch meck:unload(riak_repl2_fssink_sup).

start_node_reserver() ->
    {ok, Pid} = riak_repl2_fs_node_reserver:start_link(),
    unlink(Pid).

start_fake_sink() ->
    supervisor:start_child(riak_repl2_fssink_sup, []).

lists_snip(1, [_Elem]) ->
    [];
lists_snip(1, [_Elem, Tail]) ->
    [Tail];
lists_snip(2, [Head, _Elem]) ->
    [Head];
lists_snip(Nth, List) ->
    {Head, [_|Tail]} = lists:split(Nth - 1, List),
    Head ++ Tail.

wait_for_pid_exit(Atom) when is_atom(Atom) ->
    case whereis(Atom) of
        Pid when is_pid(Pid) ->
            wait_for_pid_exit(Pid);
        _ ->
            ok
    end;
wait_for_pid_exit(Pid) when is_pid(Pid) ->
    exit(Pid, kill),
    MonRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MonRef, process, Pid, _} ->
            ok
    end.
