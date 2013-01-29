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
        % TODO while pushing before remotes up is supported, disabling that
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

precondition(S, {call, _, disconnect, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, ack_objects, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, push_object, _Args}) ->
    S#state.sources /= [];
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

next_state(S, Res, {call, _, connect_to_v1, [Remote, _S]}) ->
    SrcState = #src_state{pids = Res, version = 1},
    next_state_connect(Remote, SrcState, S);

next_state(S, Res, {call, _, connect_to_v2, [Remote, _S]}) ->
    SrcState = #src_state{pids = Res, version = 2},
    next_state_connect(Remote, SrcState, S);

next_state(S, _Res, {call, _, disconnect, [Source]}) ->
    {Remote, _} = Source,
    Sources = lists:delete(Source, S#state.sources),
    S#state{sources = Sources, remotes_available = [Remote | S#state.remotes_available]};

next_state(S, Res, {call, _, push_object, [Remotes, Binary, _S]}) ->
    Sources = update_unacked_objects(Remotes, Res, S#state.sources),
    Master = S#state.master_queue,
    S#state{sources = Sources, master_queue = [{Remotes, Binary, Res} | Master]};

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
            NewQueue = [Queued || {RoutedRemotes, _Binary, Queued} <- State#state.master_queue, not lists:member(Remote, RoutedRemotes)],
            SrcState2 = SrcState#src_state{unacked_objects = NewQueue},
            Sources = [{Remote, SrcState2} | State#state.sources],
            State#state{sources = Sources, remotes_available = Remotes};
        _Tuple ->
            State
    end.

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
    SrcState#src_state{unacked_objects = ObjQueue ++ [Res]}.

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
    {Acked, _} = lists:split(NumAck, UnAcked),
    if
        length(Acked) =/= length(AckedStack) ->
            false;
        true ->
            assert_sink_ackings(Source#src_state.version, Acked, AckedStack)
    end;

postcondition(_S, _C, _R) ->
    false.

assert_sink_bugs(Object, Remotes, Sources) ->
    assert_sink_bugs(Object, Remotes, Sources, []).

assert_sink_bugs(_Object, _Remotes, [], Acc) ->
    lists:all(fun(true) -> true; (_) -> false end, Acc);

assert_sink_bugs(Object, Remotes, [{Remote, SrcState} | Tail], Acc) ->
    Truthiness = assert_sink_bug(Object, Remotes, Remote, SrcState),
    assert_sink_bugs(Object, Remotes, Tail, [Truthiness | Acc]).

assert_sink_bug({_Num, ObjBin, Meta}, Remotes, Remote, SrcState) ->
    {_, Sink} = SrcState#src_state.pids,
    BugGot = receive
        {got_data, Sink, Frame} ->
            Frame
    after 3000 ->
        {error, timeout}
    end,
    ShouldSkip = lists:member(Remote, Remotes),
    Version = SrcState#src_state.version,
    case {ShouldSkip, Version, BugGot} of
        {true, _, {error, timeout}} ->
            true;
        {false, 1, {objects, {_Seq, ObjBin}}} ->
            true;
        % TODO this clause below is to be removed as soon as cascading rt is 
        % ready to be implemented.
        {false, 2, {objects, {_Seq, ObjBin}}} ->
            true;
        {false, 2, {objects_and_meta, {_Seq, ObjBin, Meta}}} ->
            true;
        _ ->
            false
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
        % TODO clause below needs to be removed as soon as cascading rt is ready
        % for implementation
        {2, {objects, {_Seq, ObjBin}}} ->
            true;
        {2, {objects_and_meta, {_Seq, ObjBin, Meta}}} ->
            true;
        _ ->
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
    %BinObjects = term_to_binary([Binary]),
    Meta = [{routed_clusters, Remotes}],
    plant_bugs(Remotes, State#state.sources),
    riak_repl2_rtq:push(1, BinObjects, Meta),
    {1, BinObjects, Meta}.

ack_objects(NumToAck, {_Remote, SrcState}) ->
    {_, Sink} = SrcState#src_state.pids,
    {ok, Acked} = gen_server:call(Sink, {ack, NumToAck}),
    Acked.

%% ====================================================================
%% helpful utility functions
%% ====================================================================

wait_for_valid_sink_history(Pid, Remote, MasterQueue) ->
    NewQueue = [Queued || {RoutedRemotes, _Binary, Queued} <- MasterQueue, not lists:member(Remote, RoutedRemotes)],
    BugLength = length(NewQueue),
    gen_server:call(Pid, {block_until, BugLength}).

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
        {'$gen_call', From, _Msg} ->
            gen_server:reply(From, {error, badcall}),
            fake_sink(Socket, Version, Bug, History);
        {tcp, Socket, Bin} ->
            {ok, Frame, _Rest} = riak_repl2_rtframe:decode(Bin),
            NewBug = case Bug of
                {once_bug, Target} ->
                    Self = self(),
                    Target ! {got_data, Self, Frame},
                    undefined;
                {block_until, From, Length} when length(History) + 1 >= Length ->
                    gen_server:reply(From, ok),
                    undefined;
                _ ->
                    Bug
            end,
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Version, NewBug, [Frame | History]);
        {tcp_error, Socket, Err} ->
            exit(Err);
        {tcp_closed, Socket} ->
            ok
    end.

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
