-module(riak_repl_test_util).

-export([reset_meck/1, reset_meck/2]).
-export([abstract_gen_tcp/0, abstract_stats/0, abstract_stateful/0]).
-export([kill_and_wait/1, wait_for_pid/1, wait_for_pid/2]).

abstract_gen_tcp() ->
    reset_meck(gen_tcp, [unstick, passthrough]),
    meck:expect(gen_tcp, setopts, fun(Socket, Opts) ->
        inet:setopts(Socket, Opts)
    end).

abstract_stats() ->
    reset_meck(riak_repl_stats),
    meck:expect(riak_repl_stats, rt_source_errors, fun() -> ok end),
    meck:expect(riak_repl_stats, objects_sent, fun() -> ok end).

abstract_stateful() ->
    reset_meck(stateful),
    meck:expect(stateful, set, fun(Key, Val) ->
        Fun = fun() -> Val end,
        meck:expect(stateful, Key, Fun)
    end),
    meck:expect(stateful, delete, fun(Key) ->
        meck:delete(stateful, Key, 0)
    end).

reset_meck(Mod) ->
    reset_meck(Mod, []).

reset_meck(Mod, Opts) ->
    try meck:unload(Mod) of
        ok -> ok
    catch
        error:{not_mocked, Mod} -> ok
    end,
    meck:new(Mod, Opts).

kill_and_wait(undefined) ->
    ok;

kill_and_wait(Atom) when is_atom(Atom) ->
    kill_and_wait(whereis(Atom));

kill_and_wait(Pid) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, stupify),
    wait_for_pid(Pid).

wait_for_pid(Pid) ->
    wait_for_pid(Pid, infinity).

wait_for_pid(Pid, Timeout) ->
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, process, Pid, _Why} ->
            ok
    after Timeout ->
        {error, timeout}
    end.
