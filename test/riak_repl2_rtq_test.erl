-module(riak_repl2_rtq_test).
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

        %% we should get 9 elements back, when we ask for them, because of
        %% overhead
        Size = accumulate(Pid, 0, 9),
        ?assert(Size < 100*1024*1024),
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
            Size1 = erts_debug:flat_size({N, B}),
            Size2 = byte_size(B) - erts_debug:flat_size(B),
            accumulate(Pid, Acc+Size1+Size2, C-1)
    end.




