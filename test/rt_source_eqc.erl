-module(rt_source_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {
    remotes_available = ["a", "b", "c", "d", "e"],
    sources = [] % {remote_name(), {source_pid(), sink_pid(), [object_to_ack()]}}
    }).

prop_test() ->
    {timeout, 30, fun() ->
        ?assert(eqc:quickcheck(prop_main()))
    end}.

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
        aggregate(command_names(Cmds), begin
            false
        end)).

%% ====================================================================
%% Generators (including commands)
%% ====================================================================

command(S) ->
    oneof(
        [{call, ?MODULE, connect_to_v1, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, connect_to_v2, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        % push an object that may already have been rt'ed
        [{call, ?MODULE, push_object, [g_unique_remotes()]}] ++
        [{call, ?MODULE, ack_object, [elements(S#state.sources)]} || S#state.sources /= []]
    ).

g_unique_remotes() ->
    ?LET(Remotes, list(elements(remote_name())), lists:usort(Remotes)).

% name of a remote
remote_name(#state{remotes_available = []}) ->
    erlang:error(no_name_available);
remote_name(#state{remotes_available = Remotes}) ->
    oneof(Remotes).

precondition(S, {call, disconnect, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, ack_object, _Args}) ->
    S#state.sources /= [];
precondition(_S, _Call) ->
    true.

%% ====================================================================
%% state generation
%% ====================================================================

initial_state() ->
    abstract_gen_tcp(),
    abstract_stats(),
    start_rt(),
    start_rtq(),
    start_tcp_mon(),
    start_fake_sink(),
    #state{}.

next_state(S, Res, {call, _, connect_to_v1, [Remote]}) ->
    next_state_connect(decode_res_connect_v1, Remote, Res, S);

next_state(S, Res, {call, _, connect_to_v2, [Remote]}) ->
    next_state_connect(decode_res_connect_v2, Remote, Res, S);

next_state(S, _Res, {call, _, disconnect, [Source]}) ->
    {Remote, _} = Source,
    Sources = lists:delete(Source, S#state.sources),
    S#state{sources = Sources, [Remote | S#state.remotes_available]};

next_state(S, Res, {call, _, push_object, [Remotes]}) ->
    Sources = update_unacked_objects(Remotes, Res, S#state.sources),
    S#state{sources = Sources};

next_state(S, _Res, {call, _, ack_object, [{Remote, _Source}]}) ->
    case lists:keytake(Remote, 1, S#state.sources) of
        false ->
            S;
        {value, {Remote, RealSource}, Sources} ->
            Updated = {Remote, {call, ?MODULE, ack_object, [RealSource]}},
            Sources2 = [Updated | Sources],
            S#state{sources = Sources2}
    end.

next_state_connect(DecodeFunc, Remote, Res, State) ->
    case lists:keyfind(Remote, 1, State#state.sources) of
        true ->
            State;
        false ->
            Entry = {Remote, {call, ?MODULE, DecodeFunc, [Remote, Res]}},
            Sources = [Entry | State#state.sources],
            Remotes = lists:delete(Remote, State#state.remotes_available),
            State#state{sources = Sources, remotes_available = Remotes}
    end.

update_unacked_objects([], _Res, Sources) ->
    Sources;

update_unacked_objects([Remote | Tail], Res, Sources) ->
    case lists:keytake(Remote, 1, Sources) of
        false ->
            update_unacked_objects(Tail, Res, Sources);
        {value, {Remote, Source}, Sources2} ->
            Source2 = {call, ?MODULE, push_object, [Source, Res]},
            Sources3 = [{Remote, Source2} | Sources2],
            update_unacked_objects(Tail, Res, Sources3)
    end.

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(_S, _C, _R) ->
    true.

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_to_v1(RemoteName) ->

    
        [{call, ?MODULE, connect_to_v1, [remote_name()]}] ++
        [{call, ?MODULE, connect_to_v2, [remote_name()]}] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        % push an object that may already have been rt'ed
        [{call, ?MODULE, push_object, [g_unique_remotes()]}] ++
        [{call, ?MODULE, ack_object, [elements(S#state.sources)]} || S#state.sources /= []]

%% ====================================================================
%% helpful utility functions
%% ====================================================================

abstract_gen_tcp() ->
    reset_meck(gen_tcp, [unstick, passthrough]),
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:expect(gen_tcp, setopts, fun(Socket, Opts) ->
        inet:setopts(Socket, Opts)
    end).

abstract_stats() ->
    reset_meck(riak_repl_stats),
    meck:expect(riak_repl_stats, rt_source_errors, fun() -> ok end),
    meck:expect(riak_repl_stats, objects_sent, fun() -> ok end).

start_rt() ->
    kill_and_wait(riak_repl2_rt),
    riak_repl2_rt:start_link().

start_rtq() ->
    kill_and_wait(riak_repl2_rtq),
    riak_repl2_rtq:start_link().

start_tcp_mon() ->
    kill_and_wait(riak_core_tcp_mon),
    riak_core_tcp_mon:start_link().

start_fake_sink() ->
    reset_meck(riak_core_service_mgr, [passthrough]),
    try meck:unload(fake_sink) of
        ok -> ok
    catch
        error:{not_mocked, _Mod} -> ok
    end,
    meck:new(fake_sink),

reset_meck(Mod) ->
    try meck:unload(Mod) of
        ok -> ok
    catch
        error:{not_mocked, Mod} -> ok
    end,
    meck:new(Mod).

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
    exit(Pid, kill),
    Mon = erlang:monitor(process, Pid),
    receive
        {'DOWN', Mon, process, Pid, _Why} ->
            ok
    end.
