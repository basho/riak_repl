-module(rt_source_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5007).
-define(all_remotes, ["a", "b", "c", "d", "e"]).

-record(state, {
    remotes_available = ?all_remotes,
    master_queue = [],
    sources = [] % [#src_state{}]
    }).

-record(src_state, {
    pids, % {SourcePid, SinkPid}
    version,
    unacked_objects = []
}).

prop_test_() ->
    {timeout, 30, fun() ->
        ?assert(eqc:quickcheck(?MODULE:prop_main()))
    end}.

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
        aggregate(command_names(Cmds), begin
            {H, S, Res} = run_commands(?MODULE, Cmds),
            process_flag(trap_exit, false),
            pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok)
        end)).

%% ====================================================================
%% Generators (including commands)
%% ====================================================================

command(S) ->
    oneof(
        [{call, ?MODULE, connect_to_v1, [remote_name(S), S#state.master_queue]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, connect_to_v2, [remote_name(S), S#state.master_queue]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        % push an object that may already have been rt'ed
        % todo while pushing before remotes up is supported, disabling that
        % condition for ease of testing (for now)
        [{call, ?MODULE, push_object, [g_unique_remotes(), binary(), S]} || S#state.sources /= []] ++
        [?LET({_Remote, SrcState} = Source, elements(S#state.sources), {call, ?MODULE, ack_objects, [g_up_to_length(SrcState#src_state.unacked_objects), Source]}) || S#state.sources /= []]
    ).

g_up_to_length([]) ->
    0;
g_up_to_length(List) ->
    Max = length(List),
    choose(0, Max).

g_unique_remotes() ->
    ?LET(Remotes, list(g_remote_name()), lists:usort(Remotes)).

g_remote_name() ->
    oneof(?all_remotes).

% name of a remote
remote_name(#state{remotes_available = []}) ->
    erlang:error(no_name_available);
remote_name(#state{remotes_available = Remotes}) ->
    oneof(Remotes).

precondition(#state{sources = []}, {call, _, disconnect, _Args}) ->
    false;
precondition(#state{sources = Sources}, {call, _, disconnect, [{Remote, _}]}) ->
    is_tuple(lists:keyfind(Remote, 1, Sources));
precondition(S, {call, _, disconnect, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, ack_objects, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, push_object, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, Connect, [Remote, _]}) when Connect =:= connect_to_v1; Connect =:= connect_to_v2 ->
    lists:member(Remote, S#state.remotes_available);
precondition(_S, _Call) ->
    true.

%% ====================================================================
%% state generation
%% ====================================================================

initial_state() ->
    process_flag(trap_exit, true),
    abstract_gen_tcp(),
    abstract_stats(),
    abstract_stateful(),
    abstract_connection_mgr(),
    {ok, _RTPid} = start_rt(),
    {ok, _RTQPid} = start_rtq(),
    {ok, _TCPMonPid} = start_tcp_mon(),
    {ok, _FakeSinkPid} = start_fake_sink(),
    #state{}.

next_state(S, Res, {call, _, connect_to_v1, [Remote, MQ]}) ->
    SrcState = #src_state{pids = Res, version = 1},
    next_state_connect(Remote, SrcState, S);

next_state(S, Res, {call, _, connect_to_v2, [Remote, MQ]}) ->
    SrcState = #src_state{pids = Res, version = 2},
    next_state_connect(Remote, SrcState, S);

next_state(S, _Res, {call, _, disconnect, [{Remote, _}]}) ->
    Sources = lists:keydelete(Remote, 1, S#state.sources),
    S#state{sources = Sources, remotes_available = [Remote | S#state.remotes_available]};

next_state(S, Res, {call, _, push_object, [Remotes, Binary, _S]}) ->
    Sources = update_unacked_objects(Remotes, Res, S#state.sources),
    Master = S#state.master_queue,
    Master2 = [{Remotes, Binary, Res} | Master],
    S#state{sources = Sources, master_queue = Master2};

next_state(S, _Res, {call, _, ack_objects, [NumAcked, {Remote, _Source}]}) ->
    case lists:keytake(Remote, 1, S#state.sources) of
        false ->
            S;
        {value, {Remote, RealSource}, Sources} ->
            UnAcked = RealSource#src_state.unacked_objects,
            Updated = model_ack_objects(NumAcked, UnAcked),
            SrcState2 = RealSource#src_state{unacked_objects = Updated},
            Sources2 = [{Remote, SrcState2} | Sources],
            S#state{sources = Sources2}
    end.

next_state_connect(Remote, SrcState, State) ->
    case lists:keyfind(Remote, 1, State#state.sources) of
        false ->
            Remotes = lists:delete(Remote, State#state.remotes_available),
            NewQueue = [{call, ?MODULE, fix_unacked_from_master, [Remote, Queued, RoutedRemotes, State]} || {RoutedRemotes, _Binary, Queued} <- State#state.master_queue, not lists:member(Remote, RoutedRemotes)],
            SrcState2 = SrcState#src_state{unacked_objects = NewQueue},
            Sources = [{Remote, SrcState2} | State#state.sources],
            State#state{sources = Sources, remotes_available = Remotes};
        _Tuple ->
            State
    end.

fix_unacked_from_master(Remote, {N, Bin, Meta}, RoutedRemotes, #state{sources = Sources}) ->
    Active = [A || {A, _} <- Sources],
    Routed = lists:usort(RoutedRemotes ++ Active ++ [Remote]),
    Meta2 = orddict:store(routed_clusters, Routed, Meta),
    {N, Bin, Meta2}.

update_unacked_objects(Remotes, Res, Sources) ->
    update_unacked_objects(Remotes, Res, Sources, []).

update_unacked_objects(_Remotes, _Res, [], Acc) ->
    lists:reverse(Acc);

update_unacked_objects(Remotes, Res, [{Remote, Source} = KV | Tail], Acc) ->
    case lists:member(Remote, Remotes) of
        true ->
            update_unacked_objects(Remotes, Res, Tail, [KV | Acc]);
        false ->
            Entry = model_push_object(Res, Source),
            update_unacked_objects(Remotes, Res, Tail, [{Remote, Entry} | Acc])
    end.

model_push_object(Res, SrcState = #src_state{unacked_objects = ObjQueue}) ->
    SrcState#src_state{unacked_objects = [Res | ObjQueue]}.

model_ack_objects(_Num, []) ->
    [];
model_ack_objects(NumAcked, Unacked) when length(Unacked) >= NumAcked ->
    NewLength = length(Unacked) - NumAcked,
    {NewList, _} = lists:split(NewLength, Unacked),
    NewList.

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(_State, {call, _, connect_to_v1, _Args}, {error, _}) ->
    false;
postcondition(_State, {call, _, connect_to_v1, _Args}, {Source, Sink}) ->
    is_pid(Source) andalso is_pid(Sink);

postcondition(_State, {call, _, connect_to_v2, _Args}, {error, _}) ->
    false;
postcondition(_State, {call, _, connect_to_v2, _Args}, {Source, Sink}) ->
    is_pid(Source) andalso is_pid(Sink);

postcondition(_State, {call, _, disconnect, [_SourceState]}, Waits) ->
    lists:all(fun(ok) -> true; (_) -> false end, Waits);

postcondition(_State, {call, _, push_object, [Remotes, _Binary, State]}, Res) ->
    assert_sink_bugs(Res, Remotes, State#state.sources);

postcondition(_State, {call, _, ack_objects, [NumAck, {_Remote, Source}]}, AckedStack) ->
    #src_state{unacked_objects = UnAcked} = Source,
    {_, Acked} = lists:split(length(UnAcked) - NumAck, UnAcked),
    if
        length(Acked) =/= length(AckedStack) ->
            ?debugMsg("Acked length is not the same as AckedStack length"),
            false;
        true ->
            assert_sink_ackings(Source#src_state.version, Acked, AckedStack)
    end;

postcondition(_S, _C, _R) ->
    ?debugMsg("fall through postcondition"),
    false.

assert_sink_bugs(Object, Remotes, Sources) ->
    Active = [A || {A, _} <- Sources],
    assert_sink_bugs(Object, Remotes, Active, Sources, []).

assert_sink_bugs(_Object, _Remotes, _Active, [], Acc) ->
    lists:all(fun(true) -> true; (_) -> false end, Acc);

assert_sink_bugs(Object, Remotes, Active, [{Remote, SrcState} | Tail], Acc) ->
    Truthiness = assert_sink_bug(Object, Remotes, Remote, Active, SrcState),
    assert_sink_bugs(Object, Remotes, Active, Tail, [Truthiness | Acc]).

assert_sink_bug({_Num, ObjBin, Meta}, Remotes, Remote, _Active, SrcState) ->
    {_, Sink} = SrcState#src_state.pids,
    Version = SrcState#src_state.version,
    History = gen_server:call(Sink, history),
    ShouldSkip = lists:member(Remote, Remotes),
    if
        ShouldSkip andalso length(History) == length(SrcState#src_state.unacked_objects) ->
            true;
        true ->
            Frame = hd(History),
            case {Version, Frame} of
                {1, {objects, {_Seq, ObjBin}}} ->
                    true;
                {2, {objects_and_meta, {_Seq, ObjBin, Meta}}} ->
                    true;
                _ ->
                    ?debugFmt("assert sink bug failure!~n"
                        "    Remote: ~p~n"
                        "    Remotes: ~p~n"
                        "    ObjBin: ~p~n"
                        "    Meta: ~p~n"
                        "    ShouldSkip: ~p~n"
                        "    Version: ~p~n"
                        "    Frame: ~p", [Remote, Remotes, ObjBin, Meta, ShouldSkip, Version, Frame]),
                    false
            end
    end.

assert_sink_ackings(Version, Expecteds, Gots) ->
    assert_sink_ackings(Version, Expecteds, Gots, []).

assert_sink_ackings(_Version, [], [], Acc) ->
    lists:all(fun(true) -> true; (_) -> false end, Acc);

assert_sink_ackings(Version, [Expected | ETail], [Got | GotTail], Acc) ->
    AHead = assert_sink_acking(Version, Expected, Got),
    assert_sink_ackings(Version, ETail, GotTail, [AHead | Acc]).

assert_sink_acking(Version, {1, ObjBin, Meta}, Got) ->
    case {Version, Got} of
        {1, {objects, {_Seq, ObjBin}}} ->
            true;
        {2, {objects_and_meta, {_Seq, ObjBin, Meta}}} ->
            true;
        _ ->
            ?debugFmt("Sink ack failure!~n    Version: ~p~n    ObjBin: ~p~n    Meta: ~p~n    Got: ~p", [Version, ObjBin, Meta, Got]),
            false
    end.

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_to_v1(RemoteName, MasterQueue) ->
    stateful:set(version, {realtime, {1,0}, {1,0}}),
    stateful:set(remote, RemoteName),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName),
    receive
        {sink_started, SinkPid} ->
            wait_for_valid_sink_history(SinkPid, RemoteName, MasterQueue),
            ok = ensure_registered(RemoteName),
            {SourcePid, SinkPid}
    after 1000 ->
        {error, timeout}
    end.

connect_to_v2(RemoteName, MasterQueue) ->
    stateful:set(version, {realtime, {2,0}, {2,0}}),
    stateful:set(remote, RemoteName),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName),
    receive
        {sink_started, SinkPid} ->
            wait_for_valid_sink_history(SinkPid, RemoteName, MasterQueue),
            ok = ensure_registered(RemoteName),
            {SourcePid, SinkPid}
    after 1000 ->
        {error, timeout}
    end.

disconnect(ConnectState) ->
    {_Remote, SrcState} = ConnectState,
    #src_state{pids = {Source, Sink}} = SrcState,
    riak_repl2_rtsource_conn:stop(Source),
    [wait_for_pid(P, 3000) || P <- [Source, Sink]].

push_object(Remotes, BinObjects, State) ->
    Meta = [{routed_clusters, Remotes}],
    Active = [A || {A, _} <- State#state.sources],
    ExpectedRouted = lists:usort(Remotes ++ Active),
    ExpectedMeta = [{routed_clusters, ExpectedRouted}],
    %plant_bugs(Remotes, State#state.sources),
    riak_repl2_rtq:push(1, BinObjects, Meta),
    wait_for_pushes(State, Remotes),
    {1, BinObjects, ExpectedMeta}.

ack_objects(NumToAck, {_Remote, SrcState}) ->
    {_, Sink} = SrcState#src_state.pids,
    {ok, Acked} = gen_server:call(Sink, {ack, NumToAck}),
    Acked.

%% ====================================================================
%% helpful utility functions
%% ====================================================================

ensure_registered(RemoteName) ->
    ensure_registered(RemoteName, 10).

ensure_registered(_RemoteName, N) when N < 1 ->
    {error, registration_timeout};
ensure_registered(RemoteName, N) ->
    Status = riak_repl2_rtq:status(),
    Consumers = proplists:get_value(consumers, Status),
    case proplists:get_value(RemoteName, Consumers) of
        undefined ->
            timer:sleep(100),
            ensure_registered(RemoteName, N - 1);
        _ ->
            ok
    end.

wait_for_valid_sink_history(Pid, Remote, MasterQueue) ->
    NewQueue = [Queued || {RoutedRemotes, _Binary, Queued} <- MasterQueue, not lists:member(Remote, RoutedRemotes)],
    if
        length(NewQueue) > 0 ->
            gen_server:call(Pid, {block_until, length(NewQueue)}, 30000);
        true ->
            ok
    end.

wait_for_pushes(State, Remotes) ->
    [wait_for_push(SrcState, Remotes) || SrcState <- State#state.sources].

wait_for_push({Remote, SrcState}, Remotes) ->
    case lists:member(Remote, Remotes) of
        true -> ok;
        _ ->
            WaitLength = length(SrcState#src_state.unacked_objects) + 1,
            {_, Sink} = SrcState#src_state.pids,
            gen_server:call(Sink, {block_until, WaitLength}, 3000)
    end.

plant_bugs(_Remotes, []) ->
    ok;
plant_bugs(Remotes, [{Remote, SrcState} | Tail]) ->
    case lists:member(Remote, Remotes) of
        true ->
            plant_bugs(Remotes, Tail);
        false ->
            {_, Sink} = SrcState#src_state.pids,
            ok = gen_server:call(Sink, bug),
            plant_bugs(Remotes, Tail)
    end.

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

abstract_connection_mgr() ->
    reset_meck(riak_core_connection_mgr, [passthrough]),
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec) ->
        proc_lib:spawn_link(fun() ->
            Version = stateful:version(),
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
            {ok, Socket} = gen_tcp:connect("localhost", ?SINK_PORT, [binary | TcpOpts]),
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT}, Version, Pid, [])
        end),
        {ok, make_ref()}
    end).

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
    WhoToTell = self(),
    meck:expect(riak_core_service_mgr, register_service, fun(HostSpec, _Strategy) ->
        kill_and_wait(fake_sink),
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArgs}} = HostSpec,
        sink_listener(TcpOpts, WhoToTell)
    end),
    riak_repl2_rtsink_conn:register_service().

sink_listener(TcpOpts, WhoToTell) ->
    TcpOpts2 = [binary, {reuseaddr, true} | TcpOpts],
    Pid = proc_lib:spawn_link(fun() ->
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, TcpOpts2),
        proc_lib:spawn(?MODULE, sink_acceptor, [Listen, WhoToTell]),
        receive
            _ -> ok
        end
    end),
    register(fake_sink, Pid),
    {ok, Pid}.

sink_acceptor(Listen, WhoToTell) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    Version = stateful:version(),
    Pid = proc_lib:spawn_link(?MODULE, fake_sink, [Socket, Version, undefined, []]),
    ok = gen_tcp:controlling_process(Socket, Pid),
    Pid ! start,
    WhoToTell ! {sink_started, Pid},
    sink_acceptor(Listen, WhoToTell).

fake_sink(Socket, Version, Bug, History) ->
    receive
        start ->
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Version, Bug, History);
        stop ->
            ok;
        {'$gen_call', From, history} ->
            gen_server:reply(From, History),
            fake_sink(Socket, Version, Bug, History);
        {'$gen_call', From, {ack, Num}} when Num =< length(History) ->
            {NewHistory, Return} = lists:split(length(History) - Num, History),
            gen_server:reply(From, {ok, Return}),
            fake_sink(Socket, Version, Bug, NewHistory);
        {'$gen_call', {NewBug, _Tag} = From, bug} ->
            gen_server:reply(From, ok),
            fake_sink(Socket, Version, {once_bug, NewBug}, History);
        {'$gen_call', From, {block_until, HistoryLength}} when length(History) >= HistoryLength ->
            gen_server:reply(From, ok),
            fake_sink(Socket, Version, Bug, History);
        {'$gen_call', From, {block_until, HistoryLength}} ->
            fake_sink(Socket, Version, {block_until, From, HistoryLength}, History);
        {'$gen_call', From, Msg} ->
            ?debugFmt("~n    Your bad call: ~p;~n    My History: ~p~n    My Bug: ~p~n    My Version~p", [Msg, History, Bug, Version]),
            gen_server:reply(From, {error, badcall}),
            fake_sink(Socket, Version, Bug, History);
        {tcp, Socket, Bin} ->
            History2 = fake_sink_nom_frames(Bin, History),
            NewBug = case Bug of
                {once_bug, Target} ->
                    Self = self(),
                    [Frame | _] = History2,
                    Target ! {got_data, Self, Frame},
                    undefined;
                {block_until, From, Length} when length(History2) >= Length ->
                    gen_server:reply(From, ok),
                    undefined;
                _ ->
                    Bug
            end,
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Version, NewBug, History2);
        {tcp_error, Socket, Err} ->
            exit(Err);
        {tcp_closed, Socket} ->
            ok
    end.

fake_sink_nom_frames({ok, undefined, <<>>}, History) ->
    History;
fake_sink_nom_frames({ok, undefined, Rest}, History) ->
    ?debugFmt("Fame issues: binary left over: ~p", [Rest]),
    History;
fake_sink_nom_frames({ok, Frame, Rest}, History) ->
    fake_sink_nom_frames(Rest, [Frame | History]);
fake_sink_nom_frames(Bin, History) ->
    fake_sink_nom_frames(riak_repl2_rtframe:decode(Bin), History).

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
