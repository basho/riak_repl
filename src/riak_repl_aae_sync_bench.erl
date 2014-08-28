-module(riak_repl_aae_sync_bench).
-compile(export_all).

go() ->
    Delays = [0, 10, 100],
    %% Delays = [0, 10, 100, 200],
    [test(Delay, Pipeline) || Delay <- Delays,
                              Pipeline <- [false, true]].

test(Delay, Pipeline) ->
    sidejob_config:load_config(riak_repl_test_config, [{delay, Delay},
                                                       {pipeline, Pipeline}]),
    {Time, _} = timer:tc(fun() ->
                                 {ok, Pid} = riak_repl2_fssource:start_link(0, {"127.0.0.1", 10026}),
                                 wait_for_pid(Pid)
                         end),
    timer:sleep(500),
    {Delay, Pipeline, Time}.

wait_for_pid(Pid) ->
    Ref = monitor(process, Pid),
    receive
        {'DOWN', Ref, _, _, _} ->
            ok
    end.
