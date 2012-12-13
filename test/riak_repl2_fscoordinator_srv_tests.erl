-module(riak_repl2_fscoordinator_srv_tests).

%-include_lib("eqc/include/eqc.hrl").
%-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {
    serv
}).

whereis_test_() ->
    {setup, fun() ->
        #state{}
    end,
    fun(_) ->
        ok
    end,
    fun(State) -> [

        {"Target node is fully available", fun() ->
            meck:expect(riak_repl2_fs_node_reserver, reserve, fun(1) ->
                ok
            end),
            send({whereis, 1, {127, 0, 0, 1}, 9090}, State#state.serv),
            Response = wait_for_response(),
            ?assertEqual({location, 1, {goodnode@node, {127, 0, 0, 1}, 8090}}, Response)
        end},

        {"Target node is max'ed out", ?_assert(false)},

        {"Target node does not have node reserver running", ?_assert(false)},

        {"Target node is down", ?_assert(false)}

    ] end}.

send(_, _) -> ok.
wait_for_response() -> ok.