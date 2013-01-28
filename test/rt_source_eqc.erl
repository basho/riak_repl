-module(rt_source_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5007).

-record(state, {
    remotes_available = ["a", "b", "c", "d", "e"],
    sources = [] % {remote_name(), {source_pid(), sink_pid(), [object_to_ack()]}}
    }).

-record(src_state, {
    src_pid,
    sink_pid,
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
        [{call, ?MODULE, connect_to_v1, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, connect_to_v2, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        % push an object that may already have been rt'ed
        [{call, ?MODULE, push_object, [g_unique_remotes(), binary()]}] ++
        [{call, ?MODULE, ack_object, [elements(S#state.sources)]} || S#state.sources /= []]
    ).

g_unique_remotes() ->
    ?LET(Remotes, list(g_remote_name()), lists:usort(Remotes)).

g_remote_name() ->
    oneof(["a", "b", "c", "d", "e"]).

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

next_state(S, Res, {call, _, connect_to_v1, [Remote]}) ->
    next_state_connect(decode_res_connect_v1, Remote, Res, S);

next_state(S, Res, {call, _, connect_to_v2, [Remote]}) ->
    next_state_connect(decode_res_connect_v2, Remote, Res, S);

next_state(S, _Res, {call, _, disconnect, [Source]}) ->
    {Remote, _} = Source,
    Sources = lists:delete(Source, S#state.sources),
    S#state{sources = Sources, remotes_available = [Remote | S#state.remotes_available]};

next_state(S, Res, {call, _, push_object, [Remotes, _Binary]}) ->
    Sources = update_unacked_objects(Remotes, Res, S#state.sources),
    S#state{sources = Sources};

next_state(S, _Res, {call, _, ack_object, [{Remote, _Source}]}) ->
    case lists:keytake(Remote, 1, S#state.sources) of
        false ->
            S;
        {value, {Remote, RealSource}, Sources} ->
            Updated = {Remote, {call, ?MODULE, model_ack_object, [RealSource]}},
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

decode_res_connect_v1(_Remote, Res) ->
    Res.

decode_res_connect_v2(_Remote, Res) ->
    Res.

update_unacked_objects(Remotes, Res, Sources) ->
    update_unacked_objects(Remotes, Res, Sources, []).

update_unacked_objects(_Remotes, _Res, [], Acc) ->
    lists:reverse(Acc);

update_unacked_objects(Remotes, Res, [{Remote, Source} = KV | Tail], Acc) ->
    case lists:member(Remote, Remotes) of
        true ->
            update_unacked_objects(Remotes, Res, Tail, [KV | Acc]);
        false ->
            Entry = {call, ?MODULE, model_push_object, [Res, Source]},
            update_unacked_objects(Remotes, Res, Tail, [{Remote, Entry} | Acc])
    end.

model_push_object(Res, SrcState = #src_state{unacked_objects = ObjQueue}) ->
    SrcState#src_state{unacked_objects = ObjQueue ++ [Res]}.

model_ack_object(#src_state{unacked_objects = []} = SourceState) ->
    SourceState;
model_ack_object(#src_state{unacked_objects = [_Acked | Rest]} = SrcState) ->
    SrcState#src_state{unacked_objects = Rest}.

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(_State, {call, _, connect_to_v1, [_RemoteName]}, {error, _}) ->
    false;
postcondition(_State, {call, _, connect_to_v1, [_RemoteName]}, Res) ->
    #src_state{src_pid = Source, sink_pid = Sink} = Res,
    is_pid(Source) andalso is_pid(Sink);

postcondition(_State, {call, _, connect_to_v2, [_RemoteName]}, {error, _}) ->
    false;
postcondition(_State, {call, _, connect_to_v2, [_RemoteName]}, Res) ->
    #src_state{src_pid = Source, sink_pid = Sink} = Res,
    is_pid(Source) andalso is_pid(Sink);

postcondition(_State, {call, _, disconnect, [_SourceState]}, Waits) ->
    lists:all(fun(ok) -> true; (_) -> false end, Waits);

postcondition(State, {call, _, push_object, [Remotes, _Binary]}, Res) ->
    % need to give the fake sinks time to get the messages.
    timer:sleep(100),
    assert_sink_recvs(State#state.sources, Remotes, Res);

postcondition(_State, {call, _, ack_object, [_Source]}, _Res) ->
    % TODO make a true postcondition.
    true;

postcondition(_S, _C, _R) ->
    false.

assert_sink_recvs(Sources, Remotes, Expected) ->
    lists:all(fun(Elem) ->
        assert_sink_recv(Elem, Remotes, Expected)
    end, Sources).

assert_sink_recv({Remote, SrcState}, Remotes, Created) ->
    Sink = SrcState#src_state.sink_pid,
    LastGot = gen_server:call(Sink, last_data),
    case lists:member(Remote, Remotes) of
        true ->
            LastGot == undefined;
        false ->
            {_NumObj, ObjBin, Meta} = Created,
            case {SrcState#src_state.version, LastGot} of
                {1, {objects, {_Seq, ObjBin}}} ->
                    true;
                {2, {objects_and_meta, {_Seq, ObjBin, Meta}}} ->
                    true;
                _ ->
                    false
            end
    end.

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_to_v1(RemoteName) ->
    stateful:set(version, {realtime, {1,0}, {1,0}}),
    stateful:set(remote, RemoteName),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName),
    receive
        {sink_started, SinkPid} ->
            #src_state{src_pid = SourcePid, sink_pid = SinkPid, version = 1}
    after 1000 ->
        {error, timeout}
    end.

connect_to_v2(RemoteName) ->
    stateful:set(version, {realtime, {2,0}, {2,0}}),
    stateful:set(remote, RemoteName),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link(RemoteName),
    receive
        {sink_started, SinkPid} ->
            #src_state{src_pid = SourcePid, sink_pid = SinkPid, version = 2}
    after 1000 ->
        {error, timeout}
    end.

disconnect(ConnectState) ->
    {_Remote, SrcState} = ConnectState,
    #src_state{src_pid = Source, sink_pid = Sink} = SrcState,
    riak_repl2_rtsource_conn:stop(Source),
    [wait_for_pid(P, 3000) || P <- [Source, Sink]].

push_object(Remotes, Binary) ->
    BinObjects = term_to_binary([Binary]),
    Meta = [{routed_clusters, Remotes}],
    riak_repl2_rtq:push(1, BinObjects, Meta),
    {1, BinObjects, Meta}.

ack_object({_Remote, #src_state{unacked_objects = []}}) ->
    [];
ack_object(SourceState) ->
    {_Remote, SrcState} = SourceState,
    #src_state{unacked_objects = Objects, sink_pid = Sink} = SrcState,
    Sink ! ack_object,
    [_Acked | Objects2] = Objects,
    Objects2.

%% ====================================================================
%% helpful utility functions
%% ====================================================================

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

fake_sink(Socket, Version, LastData, History) ->
    receive
        start ->
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Version, LastData, History);
        stop ->
            ok;
        {'$gen_call', From, last_data} ->
            gen_server:reply(From, LastData),
            fake_sink(Socket, Version, undefined, History);
        {'$gen_call', From, _Msg} ->
            gen_server:reply(From, {error, badcall}),
            fake_sink(Socket, Version, LastData, History);
        {tcp, Socket, Bin} ->
            {ok, Frame, _Rest} = riak_repl2_rtframe:decode(Bin),
            inet:setopts(Socket, [{active, once}]),
            fake_sink(Socket, Version, Frame, [Frame | History]);
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
