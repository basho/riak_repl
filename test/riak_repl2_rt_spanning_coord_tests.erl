-module(riak_repl2_rt_spanning_coord_tests).

-include_lib("eunit/include/eunit.hrl").

functionality_test_() ->
    {setup, fun() ->
        meck:new([riak_repl2_rtsource_conn_sup, riak_repl2_rtsink_conn_sup]),
        meck:expect(riak_repl2_rtsource_conn_sup, enabled, fun() ->
            [{"src_a", src_a}, {"src_b", src_b}, {"src_c", src_c}]
        end),
        meck:expect(riak_repl2_rtsink_conn_sup, started, fun() ->
            [sink_a, sink_b, sink_c]
        end),
        % TODO remove no_strict when expected functions exist on these modules.
        meck:new([riak_repl2_rtsource_conn, riak_repl2_rtsink_conn], [non_strict]),
        [src_a, src_b, src_c, sink_a, sink_b, sink_c]
    end,
    fun(_) ->
        meck:unload([riak_repl2_rtsource_conn_sup, riak_repl2_rtsink_conn_sup,
            riak_repl2_rtsource_conn, riak_repl2_rtsink_conn]),
        riak_repl2_rt_spanning_model:stop()
    end,
    fun(Pids) -> [

        {foreach, fun() ->
            riak_repl2_rt_spanning_model:start_link()
        end,
        fun(_) ->
            Pid = whereis(riak_repl2_rt_spanning_model),
            unlink(Pid),
            riak_repl2_rt_spanning_model:stop(),
            riak_repl_test_util:wait_for_pid(Pid)
        end, [

            fun(_) -> {"forwarding", fun() ->
                set_conn_mecks(),
                riak_repl2_rt_spanning_coord:spanning_update("source", "sink", connect, []),
                Expected = lists:sort([{spanning_update, Pid} || Pid <- Pids]),
                Got = lists:sort(recv_conn_mecks()),
                ?assertEqual(Expected, Got),
                ?assertEqual(["source", "sink"], riak_repl2_rt_spanning_model:path("source", "sink"))
            end} end,

            fun(_) -> {"forwarding only to some sources", fun() ->
                set_conn_mecks(),
                riak_repl2_rt_spanning_coord:spanning_update("source", "sink", connect, ["src_a"]),
                Expected = lists:sort([{spanning_update, Pid} || Pid <- Pids, Pid =/= src_a]),
                Got = lists:sort(recv_conn_mecks()),
                ?assertEqual(Expected, Got),
                ?assertEqual(["source", "sink"], riak_repl2_rt_spanning_model:path("source", "sink"))
            end} end,

            fun(_) -> {"forwarding to all sinks", fun() ->
                set_conn_mecks(),
                riak_repl2_rt_spanning_coord:spanning_update("source", "sink", connect, ["sink_a"]),
                Expected = lists:sort([{spanning_update, Pid} || Pid <- Pids]),
                Got = lists:sort(recv_conn_mecks()),
                ?assertEqual(Expected, Got),
                ?assertEqual(["source", "sink"], riak_repl2_rt_spanning_model:path("source", "sink"))
            end} end,

            fun(_) -> {"define a disconnect", fun() ->
                set_conn_mecks(),
                riak_repl2_rt_spanning_model:add_cascade("source", "sink"),
                riak_repl2_rt_spanning_coord:spanning_update("source", "sink", disconnect, []),
                recv_conn_mecks(),
                ?assertNot(riak_repl2_rt_spanning_model:path("source", "sink"))
            end} end,

            fun(_) -> {"disconnect all sinks", fun() ->
                set_conn_mecks(),
                TestSinks = [integer_to_list(N) ++ "_test_sink" || N <- lists:seq(1, 3)],
                [riak_repl2_rt_spanning_model:add_cascade("source", TestSink) || TestSink <- TestSinks],
                riak_repl2_rt_spanning_coord:spanning_update("source", undefined, disconnect_all, []),
                recv_conn_mecks(),
                [?assertNot(riak_repl2_rt_spanning_model:path("source", TestSink)) || TestSink <- TestSinks]
            end} end

        ]}

    ] end}.

set_conn_mecks() ->
    Self = self(),
    meck:expect(riak_repl2_rtsink_conn, spanning_update, fun(SinkPid, _From, _To, _ConnectAct, _Routed) ->
        Self ! {spanning_update, SinkPid}
    end),
    meck:expect(riak_repl2_rtsource_conn, spanning_update, fun(SourcePid, _From, _To, _ConnectAct, _Routed) ->
        Self ! {spanning_update, SourcePid}
    end).

recv_conn_mecks() ->
    recv_conn_mecks(10).

recv_conn_mecks(Timeout) ->
    recv_conn_mecks(Timeout, []).

recv_conn_mecks(Timeout, Acc) ->
    receive
        {spanning_update, _} = Msg ->
            recv_conn_mecks(Timeout, [Msg | Acc])
    after Timeout ->
        lists:reverse(Acc)
    end.
