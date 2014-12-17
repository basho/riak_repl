-module(riak_repl2_fscoordinator_serv_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

status_no_leader_test_() ->
    {setup, fun() ->
        ok = meck:new(riak_repl2_leader, [non_strict]),
        ok = meck:expect(riak_repl2_leader, leader_node, fun() ->
            undefined
        end),
        ok = meck:new(riak_repl2_fscoordinator_serv_sup, [non_strict]),
        ok = meck:expect(riak_repl2_fscoordinator_serv_sup, started, fun() ->
            []
        end)
    end,
    fun(_) ->
        meck:unload(riak_repl2_fscoordinator_serv_sup),
        meck:unload(riak_repl2_leader)
    end,
    fun(_) -> [

        {"without a leader, status/0 returns an empty list", fun() ->
            Got = riak_repl2_fscoordinator_serv:status(),
            ?assertEqual([], Got)
        end}

    ] end}.

-endif.
