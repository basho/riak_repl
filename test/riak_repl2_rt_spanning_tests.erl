-module(riak_repl2_rt_spanning_tests).

-ifdef(EQC).
-ifdef(TEST).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

prop_test_() ->
    {timeout, 60000, fun() ->
        ?assert(eqc:quickcheck(?MODULE:prop_statem()))
    end}.

prop_statem() ->
    ?FORALL(Cmds, commands(?MODULE),
        aggregate(command_names(Cmds), begin
            {H, S, Res} = run_commands(?MODULE, Cmds),
            Out = pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok),
            riak_repl2_rt_spanning:stop(),
            Out
        end)).

command(_State) ->
    oneof([
        {call, ?MODULE, add_replication, [g_cluster(), g_cluster()]},
        {call, ?MODULE, drop_replication, [g_cluster(), g_cluster()]},
        {call, ?MODULE, drop_cluster, [g_cluster()]},
        {call, ?MODULE, replications, []},
        {call, ?MODULE, clusters, []}
    ]).

g_cluster() ->
    oneof(["one", "two", "three", "four", "five", "six", "seven", "eight",
        "nine", "ten"]).

initial_state() ->
    case whereis(riak_repl2_rt_spanning) of
        undefined ->
            ok;
        LivingPid ->
            Mon = erlang:monitor(process, LivingPid),
            riak_repl2_rt_spanning:stop(),
            receive
                {'DOWN', Mon, process, LivingPid, _} ->
                    ok
            end
    end,
    {ok, Pid} = riak_repl2_rt_spanning:start_link(),
    unlink(Pid),
    [].

next_state(State, _Res, {call, ?MODULE, add_replication, [Source, Sink]}) ->
    Sinks = case orddict:find(Source, State) of
        error -> [];
        {ok, Val} -> Val
    end,
    Sinks2 = ordsets:add_element(Sink, Sinks),
    State2 = orddict:store(Source, Sinks2, State),
    case orddict:find(Sink, State2) of
        error ->
            orddict:store(Sink, [], State2);
        {ok, _HasSink} ->
            State2
    end;

next_state(State, _Res, {call, ?MODULE, drop_replication, [Source, Sink]}) ->
    case orddict:find(Source, State) of
        error ->
            State;
        {ok, Sinks} ->
            Sinks2 = ordsets:del_element(Sink, Sinks),
            orddict:store(Source, Sinks2, State)
    end;

next_state(State, _Res, {call, ?MODULE, drop_cluster, [Cluster]}) ->
    State2 = orddict:erase(Cluster, State),
    lists:map(fun({Src, Sinks}) ->
        {Src, ordsets:del_element(Cluster, Sinks)}
    end, State2);

next_state(State, _Res, _Call) ->
    State.

postcondition(State, {call, ?MODULE, replications, []}, State) ->
    true;

postcondition(State, {call, ?MODULE, replications, []}, Res) ->
    ?debugFmt("replications dict not expected~n"
        "    Expected: ~p~n"
        "    got: ~p", [State, Res]),
    false;

postcondition(State, {call, ?MODULE, clusters, []}, Res) ->
    case orddict:fetch_keys(State) of
        Res ->
            true;
        Other ->
            ?debugFmt("clusters list not expected~n"
                "    Expected: ~p~n"
                "    Got: ~p", [Other, Res]),
            false
    end;

postcondition(_State, _Call, _Res) ->
    true.

precondition(_State, {call, ?MODULE, add_replication, [Source, Source]}) ->
    false;
precondition(_State, {call, ?MODULE, drop_replication, [Source, Source]}) ->
    false;
precondition(_State, _Call) ->
    true.

add_replication(Source, Sink) ->
    riak_repl2_rt_spanning:add_replication(Source, Sink).

drop_replication(Source, Sink) ->
    riak_repl2_rt_spanning:drop_replication(Source, Sink).

drop_cluster(ClusterName) ->
    riak_repl2_rt_spanning:drop_cluster(ClusterName).

replications() ->
    riak_repl2_rt_spanning:replications().

clusters() ->
    riak_repl2_rt_spanning:clusters().

%% === internal ==========

get_rec(Source, State) ->
    case orddict:find(Source, State) of
        error ->
            {Source, []};
        {ok, Val} ->
            Val
    end.

add_sink(Sink, {Source, Sinks}) ->
    Sinks2 = ordsets:add_element(Sink, Sinks),
    {Source, Sinks2}.

del_sink(Sink, {Source, Sinks}) ->
    Sinks2 = ordsets:del_element(Sink, Sinks),
    {Source, Sinks2}.

-endif.
-endif.