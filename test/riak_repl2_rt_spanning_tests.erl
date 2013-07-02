-module(riak_repl2_rt_spanning_tests).

-ifdef(EQC).
-ifdef(TEST).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

functionality_test_() ->
    {setup, fun() ->
        ok
    end,
    fun(ok) ->
        case whereis(riak_repl2_rt_spanning) of
            undefined ->
                ok;
            Pid ->
                exit(Pid, kill)
        end
    end,
    fun(ok) -> [

        {"start up", fun() ->
            Got = riak_repl2_rt_spanning:start_link(),
            ?assertMatch({ok, _Pid}, Got),
            ?assert(is_pid(element(2, Got))),
            unlink(element(2, Got))
        end},

        {"get list of known clusters", fun() ->
            ?assertEqual([], riak_repl2_rt_spanning:clusters())
        end},

        {"list replications", fun() ->
            Got = riak_repl2_rt_spanning:replications(),
            ?assertEqual([], Got)
        end},

        {"add a replication", fun() ->
            riak_repl2_rt_spanning:add_replication("source", "sink"),
            ?assertEqual(["sink", "source"], riak_repl2_rt_spanning:clusters()),
            Repls = riak_repl2_rt_spanning:replications(),
            ?assertEqual([{"sink", []}, {"source", ["sink"]}], Repls)
        end},

        {"drop replication", fun() ->
            riak_repl2_rt_spanning:add_replication("source", "sink"),
            riak_repl2_rt_spanning:drop_replication("source", "sink"),
            ?assertEqual(lists:sort(["source", "sink"]), lists:sort(riak_repl2_rt_spanning:clusters())),
            Repls = riak_repl2_rt_spanning:replications(),
            ?assertEqual([{"sink", []},{"source",[]}], Repls)
        end},

        {"drop cluster drops replications", fun() ->
            riak_repl2_rt_spanning:add_replication("source", "sink"),
            riak_repl2_rt_spanning:drop_cluster("sink"),
            ?assertEqual([{"source", []}], riak_repl2_rt_spanning:replications())
        end},

        {"tear down", fun() ->
            Pid = whereis(riak_repl2_rt_spanning),
            Mon = erlang:monitor(process, Pid),
            riak_repl2_rt_spanning:stop(),
            Got = receive
                {'DOWN', Mon, process, Pid, _Why} ->
                    true
            after 1000 ->
                {error, timeout}
            end,
            ?assert(Got)
        end}

    ] end}.

next_routes_test_() ->
    {foreach, fun() ->
        {ok, Pid} = riak_repl2_rt_spanning:start_link(),
        unlink(Pid),
        Pid
    end,
    fun(Pid) ->
        riak_repl2_rt_spanning:stop(),
        riak_repl_test_util:wait_for_pid(Pid)
    end,
    [

        fun(_) -> {"siimple one way cirlce", setup, fun() ->
            Clusters = ["1", "2", "3", "4", "5", "6"],
            lists:foldl(fun(_Cluster, Acc) ->
                case Acc of
                    [Last] ->
                        riak_repl2_rt_spanning:add_replication(Last, "1"),
                        [];
                    [Current, Next | Tail] ->
                        riak_repl2_rt_spanning:add_replication(Current, Next),
                        [Next | Tail]
                end
            end, Clusters, Clusters)
        end,
        fun(_) -> ok end,
        fun(_) -> [

            {"starting out", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "1"),
                ?assertEqual(["2"], Got)
            end},

            {"hitting four", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "4"),
                Expected = ["5"],
                ?assertEqual(Expected,Got)
            end},

            {"hitting six", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "6"),
                ?assertEqual([], Got)
            end},

            {"starting 4, hitting six", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("4", "6"),
                ?assertEqual(["1"], Got)
            end},

            {"walk routes", fun() ->
                Clusters = ["1", "2", "3", "4", "5", "6"],
                OrdExpected = ordsets:from_list(Clusters),
                lists:map(fun(C) ->
                    Got = walk_route(C),
                    ?debugFmt("Walking route for ~p got raw ~p", [C, Got]),
                    ?assertEqual(OrdExpected, lists:sort(Got))
                end, Clusters)
            end}

        ] end} end,

        fun(_) -> {"two way circle", setup, fun() ->
            Clusters = ["1", "2", "3", "4", "5", "6"],
            FoldFun = fun
                (Cluster, {First, [Cluster]}) ->
                    riak_repl2_rt_spanning:add_replication(Cluster, First),
                    {First, []};
                (Cluster, {First, [Cluster, Next | Tail]}) ->
                    riak_repl2_rt_spanning:add_replication(Cluster, Next),
                    {First, [Next | Tail]}
            end,
            lists:foldl(FoldFun, {hd(Clusters), Clusters}, Clusters),
            Rev = lists:reverse(Clusters),
            lists:foldl(FoldFun, {hd(Rev), Rev}, Rev)
        end,
        fun(_) -> ok end,
        fun(_) -> [

            {"hitting 2", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "2"),
                ?assertEqual(["3"], Got)
            end},

            {"hitting 3", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "3"),
                ?assertEqual(["4"], Got)
            end},

            {"hitting 5", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("1", "5"),
                ?assertEqual([], Got)
            end},

            {"hitting 6 from 4", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("4", "6"),
                ?assertEqual([], Got)
            end},

            {"hitting 2 from 4", fun() ->
                Got = riak_repl2_rt_spanning:choose_nexts("4", "2"),
                ?assertEqual(["1"], Got)
            end},

            {"walk routes", fun() ->
                Clusters = ["1", "2", "3", "4", "5", "6"],
                OrdExpected = ordsets:from_list(Clusters),
                lists:map(fun(C) ->
                    Got = walk_route(C),
                    ?debugFmt("Walking route for ~p got raw ~p", [C, Got]),
                    ?assertEqual(OrdExpected, lists:sort(Got))
                end, Clusters)
            end}
        ] end} end,

        {"highly connected mesh", setup, fun() ->
            %   1<->2<->3
            %   ^   ^   ^
            %  / \ / \ /
            % V   V   V
            % 6<->5<->4
            MeshData = [
                {"1", ["2", "5", "6"]},
                {"2", ["1", "3", "4", "5"]},
                {"3", ["2", "4"]},
                {"4", ["2", "3", "5"]},
                {"5", ["1", "2", "4", "6"]},
                {"6", ["1", "5"]}
            ],
            lists:map(fun({Source, Sinks}) ->
                lists:map(fun(Sink) ->
                    riak_repl2_rt_spanning:add_replication(Source, Sink)
                end, Sinks)
            end, MeshData)
        end,
        fun(_) -> ok end,
        fun(_) -> [

            {"walk routes", fun() ->
                Clusters = ["1", "2", "3", "4", "5", "6"],
                OrdExpected = ordsets:from_list(Clusters),
                lists:map(fun(C) ->
                    Got = walk_route(C),
                    ?assertEqual(OrdExpected, lists:sort(Got))
                end, Clusters)
            end}

        ] end},

        {"cascade set to never", setup, fun() ->
            % 1 <-> 2 <-> 3 (never) <-> 4 <-> 5
            MeshData = [
                {"1", ["2"]},
                {"2", ["1", "3"]},
                {"3", ["2", "4"]},
                {"4", ["3", "5"]},
                {"5", ["4"]}
            ],
            lists:map(fun({Source, Sinks}) ->
                lists:map(fun(Sink) ->
                    riak_repl2_rt_spanning:add_replication(Source, Sink)
                end, Sinks)
            end, MeshData)
        end,
        fun(_) -> ok end,
        fun(_) -> [

            {"set 3 to never cascade", fun() ->
                riak_repl2_rt_spanning:drop_all_cascades("3")
            end},

            {"ensure 1 can reach 3", fun() ->
                Got = riak_repl2_rt_spanning:path("1", "3"),
                ?assertEqual(["1", "2", "3"], Got)
            end},

            {"ensure 3 cannot reach 5", fun() ->
                % the graph is only useful for cascades, initial
                % replication needs to take care of itself.
                Got = riak_repl2_rt_spanning:path("3", "5"),
                ?assertEqual(false, Got)
            end},

            {"there's no path from 1 to 5", fun() ->
                Got = riak_repl2_rt_spanning:path("1", "5"),
                ?assertEqual(false, Got)
            end}

        ] end}

    ]}.

walk_route(Start) ->
    Nexts = riak_repl2_rt_spanning:choose_nexts(Start, Start),
    ?debugFmt("Der nexts: ~p", [Nexts]),
    [Start] ++ step(Start, Nexts).

step(Start, []) ->
    ?debugFmt("Empty stepping from ~p", [Start]),
    [];
step(Start, Nexts) ->
    ?debugFmt("doing stepping from ~p with nexts ~p", [Start, Nexts]),
    lists:foldl(fun(N, Acc) ->
        NextNexts = riak_repl2_rt_spanning:choose_nexts(Start, N),
        ?debugFmt("Der nexts in step: ~p", [NextNexts]),
        Acc ++ step(Start, NextNexts)
    end, Nexts, Nexts).

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
