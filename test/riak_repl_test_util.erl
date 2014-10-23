-module(riak_repl_test_util).

-export([reset_meck/1, reset_meck/2]).
-export([abstract_gen_tcp/0, abstract_stats/0, abstract_stateful/0]).
-export([kill_and_wait/1, kill_and_wait/2]).
-export([wait_for_pid/1, wait_for_pid/2]).
-export([maybe_unload_mecks/1]).
-export([start_test_ring/0, stop_test_ring/0]).
-export([maybe_start_lager/0, start_lager/0, stop_apps/1]).


start_test_ring() ->
    stop_test_ring(),
    riak_core_ring_events:start_link(),
    riak_core_ring_manager:start_link(test).

stop_test_ring() ->
    kill_and_wait(riak_core_ring_events, kill),
    kill_and_wait(riak_core_ring_manager, kill).

maybe_unload_mecks(Mecks) when is_list(Mecks) ->
    Unload = fun(Meck) ->
        try meck:unload(Meck) of
            ok -> ok
        catch
            error:{not_mocked, Meck} -> ok
        end
    end,
    [Unload(M) || M <- Mecks].

abstract_gen_tcp() ->
    reset_meck(gen_tcp, [unstick, passthrough, no_link, non_strict]),
    meck:expect(gen_tcp, setopts, fun(Socket, Opts) ->
        inet:setopts(Socket, Opts)
    end),
    meck:expect(gen_tcp, peername, fun(Socket) ->
        inet:peername(Socket)
    end),
    meck:expect(gen_tcp, sockname, fun(Socket) ->
        inet:sockname(Socket)
    end).

abstract_stats() ->
    reset_meck(riak_repl_stats, [no_link]),
    meck:expect(riak_repl_stats, rt_source_errors, fun() -> ok end),
    meck:expect(riak_repl_stats, objects_sent, fun() -> ok end).

abstract_stateful() ->
    reset_meck(stateful, [non_strict]),
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

kill_and_wait(Victem) ->
    kill_and_wait(Victem, stupify).

kill_and_wait(undefined, _Cause) ->
    ok;

kill_and_wait(Atom, Cause) when is_atom(Atom) ->
    kill_and_wait(whereis(Atom), Cause);

kill_and_wait(Pid, Cause) when is_pid(Pid) ->
    unlink(Pid),
    exit(Pid, Cause),
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

%% @doc Check the enviroment variable "ENABLE_LAGER" and return any
%% applications that were started if it was set. If it wasn't set, it
%% returns an empty list.
maybe_start_lager() ->
    maybe_start_lager(os:getenv("ENABLE_LAGER")).

maybe_start_lager(false) ->
    [];

maybe_start_lager(_) ->
    start_lager().

start_lager() ->
    %error_logger:tty(false),
    {ok, Started} = application:ensure_all_started(lager),
    lager:set_loglevel(lager_console_backend, debug),
    Started.

%% @doc Stop the applications listsed. The list is assumed to be in the
%% order they were started, like what is returned from
%% `application:ensure_all_started/1'. The apps are stopped in the reverse
%% order.
stop_apps(Started) ->
    lists:foreach(fun(App) ->
        application:stop(App)
    end, lists:reverse(Started)).

