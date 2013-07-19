-module(riak_repl2_rt_spanning_coord_tests).

-include_lib("eunit/include/eunit.hrl").

functionality_test_() ->
    {foreach, fun() ->
        {ok, ModelPid} = riak_repl2_rt_spanning_model:start_link(),
        {ok, CoordPid} = riak_repl2_rt_spanning_coord:start_link(),
        meck:new(spanning_cb_mod, [non_strict]),
        {ModelPid, CoordPid}
    end,
    fun({Model, Coord}) ->
        meck:unload(spanning_cb_mod),
        unlink(Model),
        unlink(Coord),
        riak_repl2_rt_spanning_model:stop(),
        riak_repl2_rt_spanning_coord:stop(),
        [riak_repl_test_util:wait_for_pid(P) || P <- [Model, Coord]]
    end, [

        fun(_) -> {"begin_chain sets the model", [

            fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", connect),
                Expected = [{"sink", []}, {"source", ["sink"]}],
                Got = riak_repl2_rt_spanning_model:cascades(),
                ?assertEqual(Expected, Got)
            end,

            fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", disconnect),
                Expected = [{"sink", []}, {"source", []}],
                Got = riak_repl2_rt_spanning_model:cascades(),
                ?assertEqual(Expected, Got)
            end,

            fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", connect),
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink2", connect),
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink3", connect),
                Expected = orddict:from_list([{"source", ["sink", "sink2", "sink3"]}, {"sink", []}, {"sink2", []}, {"sink3", []}]),
                Got = riak_repl2_rt_spanning_model:cascades(),
                ?assertEqual(Expected, Got)
            end,

            fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", undefined, disconnect_all),
                Expected = orddict:from_list([{Name, []} || Name <- ["source", "sink", "sink2", "sink3"]]),
                Got = riak_repl2_rt_spanning_model:cascades(),
                ?assertEqual(Expected, Got)
            end

        ]} end,

        fun(_) -> {"chaining and subscriptions", [

            {"subscribe", fun() ->
                set_meck(),
                Got = riak_repl2_rt_spanning_coord:subscribe("id", "node_name", spanning_cb_mod, "sub_state"),
                Expected = {ok, {"node_name", "id"}},
                ?assertEqual(Expected, Got)
            end},

            {"begin chain", fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", connect),
                Got = recv_sub("sub_state"),
                Expected = {"source", "sink", connect, 1, ["node_name"]},
                ?assertEqual(Expected, Got)
            end},

            {"version increments", fun() ->
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", disconnect),
                Got = recv_sub("sub_state"),
                Expected = {"source", "sink", disconnect, 2, ["node_name"]},
                ?assertEqual(Expected, Got)
            end},

            {"continue chain simple", fun() ->
                riak_repl2_rt_spanning_coord:continue_chain("source", "sink", connect, 5, ["visited"]),
                Got = recv_sub("sub_state"),
                Expected = {"source", "sink", connect, 5, ["node_name", "visited"]},
                ?assertEqual(Expected, Got)
            end},

            {"continue chain already routed", fun() ->
                riak_repl2_rt_spanning_coord:continue_chain("source", "sink", disconnect, 10, ["node_name"]),
                Got = recv_sub("sub_state"),
                Expected = {error, timeout},
                ?assertEqual(Expected, Got)
            end},

            {"continue chain old vsn skipped", fun() ->
                riak_repl2_rt_spanning_coord:continue_chain("source", "sink", connect, 7, ["visited"]),
                Got = recv_sub("sub_state"),
                Expected = {error, timeout},
                ?assertEqual(Expected, Got)
            end},

            {"unsubcribe", fun() ->
                riak_repl2_rt_spanning_coord:unsubscribe({"node_name", "id"}),
                riak_repl2_rt_spanning_coord:begin_chain("source", "sink", disconnect),
                Got = recv_sub("sub_state"),
                ?assertEqual({error, timeout}, Got)
            end}

        ]} end
    ]}.

set_meck() ->
    Self = self(),
    meck:expect(spanning_cb_mod, spanning_update, fun(Source, Sink, Act, Vsn, Routed, Name) ->
        Self ! {spanning, Name, Source, Sink, Act, Routed, Vsn}
    end).

recv_sub(Name) ->
    recv_sub(Name, 1000).

recv_sub(Name, Timeout) ->
    receive
        {spanning, Name, Source, Sink, Act, Routed, Vsn} ->
            {Source, Sink, Act, Routed, Vsn}
    after Timeout ->
        {error, timeout}
    end.

