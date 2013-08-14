-module(riak_repl2_rtq_tests).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

rtq_trim_test() ->
    %% make sure the queue is 10mb
    application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
    {ok, Pid} = riak_repl2_rtq:start_test(),
    try
        gen_server:call(Pid, {register, rtq_test}),
        %% insert over 20mb in the queue
        MyBin = crypto:rand_bytes(1024*1024),
        [gen_server:cast(Pid, {push, 1, MyBin}) || _ <- lists:seq(0, 20)],

        %% we get all 10 bytes back, because in TEST mode the RTQ disregards
        %% ETS overhead
        Size = accumulate(Pid, 0, 10),
        ?assert(Size =< 10*1024*1024),
        %% the queue is now empty
        ?assert(gen_server:call(Pid, {is_empty, rtq_test}))
    after
        application:unset_env(riak_repl, rtq_max_bytes),
        exit(Pid, kill)
    end.

ask(Pid) ->
    Self = self(),
    gen_server:call(Pid, {pull_with_ack, rtq_test,
             fun ({Seq, NumItem, Bin, _Meta}) ->
                    Self ! {rtq_entry, {NumItem, Bin}},
                    gen_server:cast(Pid, {ack, rtq_test, Seq}),
                    ok
        end}).


accumulate(_, Acc, 0) ->
    Acc;
accumulate(Pid, Acc, C) ->
    ask(Pid),
    receive
        {rtq_entry, {N, B}} ->
            Size = byte_size(B),
            accumulate(Pid, Acc+Size, C-1)
    end.


overload_protection_test_() ->
    [
        {"able to start after a crash without ets errors", fun() ->
            {ok, Rtq1} = riak_repl2_rtq:start_link(),
            unlink(Rtq1),
            exit(Rtq1, kill),
            riak_repl_test_util:wait_for_pid(Rtq1),
            Got = riak_repl2_rtq:start_link(),
            ?assertMatch({ok, _Pid}, Got),
            riak_repl2_rtq:stop()
        end},

        {"start with overload and recover options", fun() ->
            Got = riak_repl2_rtq:start_link([{overload_threshold, 5000}, {overload_recover, 2500}]),
            ?assertMatch({ok, _Pid}, Got),
            riak_repl2_rtq:stop()
        end}
    ].
