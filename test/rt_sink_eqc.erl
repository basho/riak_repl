-module(rt_sink_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5008).
-define(all_remotes, ["a", "b", "c", "d", "e"]).

-record(state, {
    remotes_available = ?all_remotes,
    sources = [],
    rtq = []
    }).

-record(src_state, {
    pids,
    version,
    done_fun_queue = []
    }).

-record(fake_source, {
    socket,
    clustername,
    version,
    seq = 1,
    skips = 0,
    tcp_bug = undefined
    }).

-record(push_result, {
    rtq_res,
    donefun,
    binary,
    already_routed
}).

prop_test_() ->
    {timeout, 60000, fun() ->
        ?assert(eqc:quickcheck(?MODULE:prop_main()))
    end}.

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
        aggregate(command_names(Cmds), begin
            {H, S, Res} = run_commands(?MODULE, Cmds),
            pretty_commands(?MODULE, Cmds, {H,S,Res}, Res == ok)
        end)).

%% ====================================================================
%% Generators (including commands)
%% ====================================================================

command(S) ->
    frequency(
        [{1, {call, ?MODULE, connect_from_v1, [remote_name(S)]}} || S#state.remotes_available /= []] ++
        [{1, {call, ?MODULE, connect_from_v2, [remote_name(S)]}} || S#state.remotes_available /= []] ++
        [{2, {call, ?MODULE, disconnect, [elements(S#state.sources)]}} || S#state.sources /= []] ++
        [{5, {call, ?MODULE, push_object, [elements(S#state.sources), obj_binary(), g_unique(["sink_cluster" | ?all_remotes])]}} || S#state.sources /= []] ++
        [{3, {call, ?MODULE, call_donefun, ?LET({Remote, Source}, elements(S#state.sources),[{Remote, Source}, g_donefun(Source#src_state.done_fun_queue)])}} || S#state.sources /= []]
        %[{3, {call, ?MODULE, call_donefun, [elements(S#state.sources)]}} || S#state.sources /= []]
        ).

obj_binary() ->
    ?LET(Binary, binary(), term_to_binary([Binary])).

g_donefun([]) ->
    empty;
g_donefun(DoneFuns) ->
    Max = length(DoneFuns),
    choose(1, Max).

g_unique(Possibilites) ->
    ?LET(List, list(elements(Possibilites)), lists:usort(List)).

remote_name(#state{remotes_available = []}) ->
    erlang:error(no_name_available);
remote_name(#state{remotes_available = Remotes}) ->
    oneof(Remotes).

precondition(_S, {call, _, call_donefun, [_Src, empty]}) ->
    false;
precondition(_S, {call, _, call_donefun, [{_Remote, Src}, Nth]}) ->
    case lists:nth(Nth, Src#src_state.done_fun_queue) of
        called ->
            false;
        _ ->
            true
    end;
precondition(#state{sources = []}, {call, _, disconnect, _Args}) ->
    false;
precondition(#state{sources = Sources}, {call, _, disconnect, [{Remote, _}]}) ->
    is_tuple(lists:keyfind(Remote, 1, Sources));
precondition(S, {call, _, push_object, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, Connect, [Remote]}) when Connect == connect_from_v1; Connect == connect_from_v2 ->
    lists:member(Remote, S#state.remotes_available);
precondition(S, {call, _, Connect, [#src_state{done_fun_queue = []}, _]}) ->
    false;
precondition(_S, _Call) ->
    true.

%% ====================================================================
%% state generation
%% ====================================================================

initial_state() ->
    % we will be resetting many mecks, which link to this, but we don't
    % want to kill our test process.
    %?debugMsg("initializing state"),
    process_flag(trap_exit, true),
    % read out messages that may be left over from a previous run
    read_all_rt_bugs(),
    read_all_fake_rtq_bugs(),
    % murder and restore all processes/mecks
    riak_repl_test_util:kill_and_wait(riak_repl2_rtsink_conn_sup, kill),
    riak_repl2_rtsink_conn_sup:start_link(),
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_core_tcp_mon:start_link(),
    riak_repl_test_util:abstract_stateful(),
    abstract_ring_manager(),
    abstract_ranch(),
    abstract_rtsink_helper(),
    riak_repl_test_util:abstract_gen_tcp(),
    bug_rt(),
    start_service_manager(),
    riak_repl2_rtsink_conn:register_service(),
    abstract_fake_source(),
    start_fake_rtq(),
    #state{}.

next_state(S, Res, {call, _, connect_from_v1, [Remote]}) ->
    %?debugMsg("connect v1 next state"),
    SrcState = #src_state{pids = Res, version = 1},
    next_state_connect(Remote, SrcState, S);

next_state(S, Res, {call, _, connect_from_v2, [Remote]}) ->
    %?debugMsg("connect v2 next state"),
    SrcState = #src_state{pids = Res, version = 2},
    next_state_connect(Remote, SrcState, S);

next_state(S, _Res, {call, _, disconnect, [{Remote, _}]}) ->
    %?debugMsg("disconnect next state"),
    Sources2 = lists:keydelete(Remote, 1, S#state.sources),
    Avails = [Remote | S#state.remotes_available],
    S#state{sources = Sources2, remotes_available = Avails};

next_state(S, Res, {call, _, push_object, [{Remote, SrcState}, Binary, PreviouslyRouted]}) ->
    case SrcState#src_state.version of
        1 ->
            %DoneQueue2 = [{{call, ?MODULE, extract_done_fun, [Res]}, Binary, PreviouslyRouted} | SrcState#src_state.done_fun_queue],
            DoneQueue2 = SrcState#src_state.done_fun_queue ++ [{call, ?MODULE, extract_queue_object, [Res]}],
            %?debugFmt("Done queue 2: ~p", [DoneQueue2]),
            SrcState2 = SrcState#src_state{done_fun_queue = DoneQueue2},
            Sources2 = lists:keystore(Remote, 1, S#state.sources, {Remote, SrcState2}),
            S#state{sources = Sources2};
        2 ->
            case lists:member(Remote, PreviouslyRouted) of
                true ->
                    %?debugMsg("no change after push"),
                    S;
                false ->
                    DoneQueue2 = SrcState#src_state.done_fun_queue ++ [{call, ?MODULE, extract_queue_object, [Res]}],
                    %?debugFmt("Done queue 2: ~p", [DoneQueue2]),
                    SrcState2 = SrcState#src_state{done_fun_queue = DoneQueue2},
                    Sources2 = lists:keystore(Remote, 1, S#state.sources, {Remote, SrcState2}),
                    S#state{sources = Sources2}
            end
    end;

next_state(S, _Res, {call, _, call_donefun, [{_Remote, #src_state{done_fun_queue = []}}, empty]}) ->
    S;
next_state(S, _Res, {call, _, call_donefun, [{Remote, SrcState}, DoneFunN]}) ->
    case lists:nth(DoneFunN, SrcState#src_state.done_fun_queue) of
        called ->
            S;
        _ ->
            {Head, [_Nix | Tail]} = lists:split(DoneFunN - 1, SrcState#src_state.done_fun_queue),
            DoneFun2 = Head ++ [called] ++ Tail,
            SrcState2 = SrcState#src_state{done_fun_queue = DoneFun2},
            Sources2 = lists:keystore(Remote, 1, S#state.sources, {Remote, SrcState2}),
            S#state{sources = Sources2}
    end.
%next_state(S, _Res, {call, _, call_donefun, [{Remote, SrcState}, DoneFunN]}) ->
%
%    Length = length(SrcState#src_state.done_fun_queue),
%    {DoneQueue2, _Done} = lists:split(Length - 1, SrcState#src_state.done_fun_queue),
%    SrcState2 = SrcState#src_state{done_fun_queue = DoneQueue2},
%    Sources2 = lists:keystore(Remote, 1, S#state.sources, {Remote, SrcState2}),
%    S#state{sources = Sources2}.

next_state_connect(Remote, SrcState, State) ->
    Sources = State#state.sources,
    Sources2 = lists:keystore(Remote, 1, Sources, {Remote, SrcState}),
    Avails = lists:delete(Remote, State#state.remotes_available),
    State#state{sources = Sources2, remotes_available = Avails}.

extract_queue_object(PushResult) ->
    #push_result{donefun = {ok, DoneFun}, already_routed = AlreadyRouted, binary = Binary} = PushResult,
    {DoneFun, Binary, AlreadyRouted}.

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(_S, {call, _, connect_from_v1, [_Remote]}, {error, _Reses}) ->
    %?debugMsg("postcontition failure"),
    false;
postcondition(_S, {call, _, connect_from_v1, [_Remote]}, {Source, Sink}) ->
    Out = is_pid(Source) andalso is_pid(Sink),
    case Out of
        true ->
            ok;
        _ ->
            %?debugMsg("postcontition failure"),
            ok
    end,
    Out;
postcondition(_S, {call, _, connect_from_v2, [_Remote]}, {error, _Reses}) ->
    %?debugMsg("postcontition failure"),
    false;
postcondition(_S, {call, _, connect_from_v2, [_Remote]}, {Source, Sink}) ->
    Out = is_pid(Source) andalso is_pid(Sink),
    case Out of
        true ->
            ok;
        _ ->
            %?debugMsg("postcontition failure"),
            ok
    end,
    Out;

postcondition(_S, {call, _, disconnect, [_Remote]}, {Source, Sink}) ->
    Out = not ( is_process_alive(Source) orelse is_process_alive(Sink) ),
    case Out of
        true ->
            ok;
        _ ->
            %?debugMsg("postcontition failure"),
            ok
    end,
    Out;

postcondition(_S, {call, _, push_object, [{_Remote, #src_state{version = 1} = SrcState}, _Binary, _AlreadyRouted]}, Reses) ->
    #push_result{rtq_res = RTQRes, donefun = HelperRes} = Reses,
    case RTQRes of
        {error, timeout} ->
            case HelperRes of
                {ok, _Other} ->
                    {Source, Sink} = SrcState#src_state.pids,
                    is_process_alive(Source) andalso is_process_alive(Sink);
                _HelperWhat ->
                    %?debugMsg("postcontition failure"),
                    false
            end;
        _QWhat ->
            %?debugMsg("postcontition failure"),
            false
    end;
postcondition(S, {call, _, push_object, [{Remote, #src_state{version = 2} = SrcState}, Binary, AlreadyRouted]}, Res) ->
    RTQRes = Res#push_result.rtq_res,
    HelperRes = Res#push_result.donefun,
    Routed = lists:member(Remote, Res#push_result.already_routed),
    case {Routed, RTQRes, HelperRes} of
        {true, {error, timeout}, {error, timeout}} ->
            true;
        {false, {error, timeout}, {ok, _DoneFun}} ->
            true;
        _ ->
            %?debugMsg("postcontition failure"),
            %?debugFmt("it's a bughunt!~n"
%                "    S: ~p~n"
%                "    Remote: ~p~n"
%                "    SrcState: ~p~n"
%                "    Binary: ~p~n"
%                "    AlreadyRouted: ~p~n"
%                "    Res: ~p", [S, Remote, SrcState, Binary, AlreadyRouted, Res]),
            false
    end;
postcondition(_S, {call, _, push_object, _Args}, {error, _What}) ->
    %?debugMsg("postcontition failure"),
    false;

postcondition(_S, {call, _, call_donefun, [{_Remote, #src_state{done_fun_queue = []}}, _NthDoneFun]}, {ok, empty}) ->
    true;
postcondition(_S, {call, _, call_donefun, [{Remote, SrcState}, NthDoneFun]}, Res) ->
    DoneFun = lists:nth(NthDoneFun, SrcState#src_state.done_fun_queue),
    case DoneFun of
        called ->
            true;
        {_DoneFun, Binary, AlreadyRouted} ->
            PreviousDones = lists:sublist(SrcState#src_state.done_fun_queue, NthDoneFun - 1),
            AckExpected = lists:all(fun(called) -> true; (_) -> false end, PreviousDones),
            Version = SrcState#src_state.version,
            case {Version, Res} of
                {2, {{ok, {push, _NumItems, Binary, Meta}}, {ok, TcpBin}}} when AckExpected ->
                    Routed = orddict:fetch(routed_clusters, Meta),
                    Got = ordsets:from_list(Routed),
                    Expected = ordsets:from_list([Remote | AlreadyRouted]),
                    Frame = riak_repl2_rtframe:decode(TcpBin),
                    case {Got, Frame} of
                        {Expected, {ok, {ack, _SomeSeq}, <<>>}} ->
                            true;
                        _ ->
                            %?debugMsg("postcontition failure"),
                            false
                    end;
                {2, {{ok, {push, _NumItems, Binary, Meta}}, {error, timeout}}} ->
                    Routed = orddict:fetch(routed_clusters, Meta),
                    Got = ordsets:from_list(Routed),
                    Expected = ordsets:from_list([Remote | AlreadyRouted]),
                    Got =:= Expected;
                {1, {{error, timeout}, {ok, TCPBin}}} when AckExpected ->
                    case riak_repl2_rtframe:decode(TCPBin) of
                        {ok, {ack, _SomeSeq}, <<>>} ->
                            true;
                        FrameDecode ->
                            %?debugFmt("Frame decoded: ~p", [FrameDecode]),
                            %?debugMsg("postcontition failure"),
                            false
                    end;
                {1, {{error, timeout},{error, timeout}}} ->
                    true;
                _ ->
                    ?debugFmt("No valid returns on call_donefun~n"
                        "    AckExpected: ~p~n"
                        "    DoneFun: ~p~n", [AckExpected, DoneFun]),
                    false
            end
    end;

%postcondition(_S, {call, _, call_donefun, [{Remote, SrcState}]}, Res) ->
%    {_DoneFun, Binary, AlreadyRouted} = lists:last(SrcState#src_state.done_fun_queue),
%    Version = SrcState#src_state.version,
%    case {Version, Res} of
%        {2, {{ok, {push, _NumItems, Binary, Meta}}, {ok, TcpBin}}} ->
%            Routed2 = orddict:fetch(routed_clusters, Meta),
%            Got = ordsets:from_list(Routed2),
%            Expected = ordsets:from_list([Remote | AlreadyRouted]),
%            Frame = riak_repl2_rtframe:decode(TcpBin),
%            case {Got, Frame} of
%                {Expected, {ok, {ack, _SomeSeq}, <<>>}} ->
%                    true;
%                _ ->
%                    %?debugMsg("postcontition failure"),
%                    false
%            end;
%        {1, {{error, timeout}, {ok, TCPBin}}} ->
%            case riak_repl2_rtframe:decode(TCPBin) of
%                {ok, {ack, _SomeSeq}, <<>>} ->
%                    true;
%                FrameDecode ->
%                    %?debugFmt("Frame decoded: ~p", [FrameDecode]),
%                    %?debugMsg("postcontition failure"),
%                    false
%            end;
%        _ ->
%            %?debugMsg("postcontition failure"),
%            false
%    end;

postcondition(_S, _C, _R) ->
    ?debugMsg("Fall through post condition"),
    true.

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_from_v1(Remote) ->
    %?debugMsg("connect v1"),
    SourceRes = connect_source({1,0}, Remote),
    SinkRes = read_rt_bug(),
    case {SourceRes, SinkRes} of
        {{ok, Source}, {ok, Sink}} ->
            {Source, Sink};
        _ ->
            {error, {SourceRes, SinkRes}}
    end.

connect_from_v2(Remote) ->
    %?debugMsg("connect v2 test"),
    SourceRes = connect_source({2,0}, Remote),
    SinkRes = read_rt_bug(),
    case {SourceRes, SinkRes} of
        {{ok, Source}, {ok, Sink}} ->
            {Source, Sink};
        _ ->
            {error, {SourceRes, SinkRes}}
    end.

disconnect({_Remote, State}) ->
    {Source, Sink} = State#src_state.pids,
    riak_repl_test_util:kill_and_wait(Source),
    riak_repl_test_util:wait_for_pid(Sink),
    {Source, Sink}.

push_object({_Remote, #src_state{version = 2} = SrcState}, Binary, AlreadyRouted) ->
    {Source, _Sink} = SrcState#src_state.pids,
    ok = fake_source_push_obj(Source, Binary, AlreadyRouted),
    RTQRes = read_fake_rtq_bug(),
    HelperRes = read_rtsink_helper_donefun(),
    #push_result{rtq_res = RTQRes, donefun = HelperRes, binary = Binary, already_routed = AlreadyRouted};

push_object({_Remote, SrcState}, Binary, AlreadyRouted) ->
    {Source, _Sink} = SrcState#src_state.pids,
    ok = fake_source_push_obj(Source, Binary, AlreadyRouted),
    RTQRes = read_fake_rtq_bug(),
    HelperRes = read_rtsink_helper_donefun(),
    #push_result{rtq_res = RTQRes, donefun = HelperRes, binary = Binary, already_routed = AlreadyRouted}.

call_donefun({_Remote, #src_state{done_fun_queue = []}}, empty) ->
    {ok, empty};
call_donefun({_Remote, SrcState}, NthDoneFun) ->
    {DoneFun, _Bin, _AlreadyRouted} = lists:nth(NthDoneFun, SrcState#src_state.done_fun_queue),
    DoneFun(),
    {Source, _Sink} = SrcState#src_state.pids,
    AckRes = fake_source_tcp_bug(Source),
    RTQRes = read_fake_rtq_bug(),
    {RTQRes,AckRes}.

%% ====================================================================
%% helpful utility functions
%% ====================================================================

abstract_ranch() ->
    %?debugMsg("abstracting ranch"),
    riak_repl_test_util:reset_meck(ranch),
    meck:expect(ranch, accept_ack, fun(_Listener) -> ok end),
    meck:expect(ranch, start_listener, fun(_IpPort, _MaxListeners, _RanchTCP, RanchOpts, _Module, _SubProtos) ->
        riak_repl_test_util:kill_and_wait(sink_listener),
        %?debugMsg("ranch start listener"),
        IP = proplists:get_value(ip, RanchOpts),
        Port = proplists:get_value(port, RanchOpts),
        Pid = proc_lib:spawn_link(?MODULE, sink_listener, [IP, Port, RanchOpts]),
        register(sink_listener, Pid),
        {ok, Pid}
    end).

abstract_ring_manager() ->
    riak_repl_test_util:reset_meck(riak_core_ring_manager, [passthrough]),
    meck:expect(riak_core_ring_manager, get_my_ring, fun() ->
        {ok, fake_ring}
    end),
    riak_repl_test_util:reset_meck(riak_core_ring, [passthrough]),
    meck:expect(riak_core_ring, get_meta, fun
        (symbolic_clustername, fake_ring) ->
            {ok, "sink_cluster"};
        (_Key, fake_ring) ->
            undefined;
        (Key, Ring) ->
            meck:passthrough([Key, Ring])
    end).

abstract_rtsink_helper() ->
    riak_repl_test_util:reset_meck(riak_repl2_rtsink_helper),
    ReturnTo = self(),
    meck:expect(riak_repl2_rtsink_helper, start_link, fun(_Parent) ->
        {ok, sink_helper}
    end),
    meck:expect(riak_repl2_rtsink_helper, stop, fun(sink_helper) ->
        ok
    end),
    meck:expect(riak_repl2_rtsink_helper, write_objects, fun(sink_helper, _BinObjs, DoneFun) ->
        ReturnTo ! {rtsink_helper, done_fun, DoneFun},
        ok
    end).

read_rtsink_helper_donefun() ->
    read_rtsink_helper_donefun(3000).

read_rtsink_helper_donefun(Timeout) ->
    receive
        {rtsink_helper, done_fun, DoneFun} ->
            {ok, DoneFun}
    after Timeout ->
        {error, timeout}
    end.

sink_listener(Ip, Port, Opts) ->
    %?debugFmt("prelisten arg dump: ~p, ~p, ~p", [Ip, Port, Opts]),
    {ok, Sock} = gen_tcp:listen(Port, [{ip, Ip}, binary, {reuseaddr, true}, {active, false} | Opts]),
    %?debugFmt("Listening on ~p:~p", [Ip, Port]),
    sink_listener(Sock).

sink_listener(ListenSock) ->
    %?debugMsg("preaccept"),
    {ok, Socket} = gen_tcp:accept(ListenSock),
    %?debugMsg("post accept"),
    {ok, Pid} = riak_core_service_mgr:start_link(sink_listener, Socket, gen_tcp, []),
    gen_tcp:controlling_process(Socket, Pid),
    %?debugMsg("post start link"),
    sink_listener(ListenSock).

bug_rt() ->
    %?debugMsg("installing bug on register sink"),
    riak_repl_test_util:reset_meck(riak_repl2_rt, [passthrough]),
    WhoToTell = self(),
    meck:expect(riak_repl2_rt, register_sink, fun(SinkPid) ->
        %?debugFmt("Sending bug to ~p", [WhoToTell]),
        WhoToTell ! {sink_pid, SinkPid},
        meck:passthrough([SinkPid])
    end),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl2_rt:start_link().

read_rt_bug() ->
    read_rt_bug(3000).

read_rt_bug(Timeout) ->
    %?debugFmt("reading rt bug until ~p", [Timeout]),
    receive
        {sink_pid, Pid} ->
            {ok, Pid}
    after Timeout ->
        {error, timeout}
    end.

read_all_rt_bugs() ->
    LastRead = read_rt_bug(0),
    read_all_rt_bugs(LastRead).

read_all_rt_bugs({error, timeout}) ->
    ok;
read_all_rt_bugs(_PreviousRead) ->
    read_all_rt_bugs(read_rt_bug(0)).

start_service_manager() ->
    %?debugMsg("Starting service manager"),
    riak_repl_test_util:kill_and_wait(riak_core_service_manager),
    riak_core_service_mgr:start_link({"127.0.0.1", ?SINK_PORT}).

abstract_fake_source() ->
    %?debugMsg("abstacting fake source"),
    riak_repl_test_util:reset_meck(fake_source),
    meck:expect(fake_source, connected, fun(Socket, Transport, Endpoint, Proto, Pid, Props) ->
        %?debugFmt("fake source connect fun call!~n"
%            "    Socket: ~p~n"
%            "    Transport: ~p~n"
%            "    Endpoint: ~p~n"
%            "    Proto: ~p~n"
%            "    Pid: ~p~n"
%            "    Props: ~p", [Socket, Transport, Endpoint, Proto, Pid, Props]),
        Transport:controlling_process(Socket, Pid),
        gen_server:call(Pid, {connected, Socket, Endpoint, Proto})
    end).

start_fake_rtq() ->
    WhoToTell = self(),
    riak_repl_test_util:kill_and_wait(fake_rtq),
    riak_repl_test_util:reset_meck(riak_repl2_rtq, [passthrough]),
    Pid = proc_lib:spawn_link(?MODULE, fake_rtq, [WhoToTell]),
    register(fake_rtq, Pid),
    meck:expect(riak_repl2_rtq, push, fun(NumItems, Bin, Meta) ->
        gen_server:cast(fake_rtq, {push, NumItems, Bin, Meta})
    end),
    {ok, Pid}.

fake_rtq(Bug) ->
    %?debugFmt("fake rtq started with bug ~p", [Bug]),
    fake_rtq(Bug, []).

fake_rtq(Bug, Queue) ->
    receive
        {'$gen_cast', {push, NumItems, Bin, Meta} = Got} ->
            %?debugFmt("Der fake rtq casted: ~p", [Got]),
            Bug ! Got,
            Queue2 = [{NumItems, Bin, Meta} | Queue],
            fake_rtq(Bug, Queue2);
        Else ->
            %?debugFmt("I don't even...~p", [Else]),
            ok
    end.

read_fake_rtq_bug() ->
    read_fake_rtq_bug(5000).

read_fake_rtq_bug(Timeout) ->
    receive
        {push, _NumItems, _Bin, _Metas} = Got ->
            {ok, Got}
    after Timeout ->
        {error, timeout}
    end.

read_all_fake_rtq_bugs() ->
    read_all_fake_rtq_bugs(read_fake_rtq_bug(0)).

read_all_fake_rtq_bugs({error, timeout}) ->
    ok;
read_all_fake_rtq_bugs(_) ->
    read_all_fake_rtq_bugs(read_fake_rtq_bug(0)).

connect_source(Version, Remote) ->
    %?debugFmt("starting fake source version ~p", [Version]),
    Pid = proc_lib:spawn_link(?MODULE, fake_source_loop, [undefined, Version, Remote]),
    TcpOptions = [{keepalive, true}, {nodelay, true}, {packet, 0},
        {active, false}],
    ClientSpec = {{realtime, [Version]}, {TcpOptions, fake_source, Pid}},
    IpPort = {{127,0,0,1}, ?SINK_PORT},
    %?debugFmt("attempting to connect~n"
%        "    IpPort: ~p~n"
%        "    ClientSpec: ~p", [IpPort, ClientSpec]),
    ConnRes = riak_core_connection:sync_connect(IpPort, ClientSpec),
    %?debugFmt("Connection result: ~p", [ConnRes]),
    {ok, Pid}.

fake_source_push_obj(Source, Binary, AlreadyRouted) ->
    gen_server:call(Source, {push, Binary, AlreadyRouted}).

fake_source_tcp_bug(Source) ->
    try gen_server:call(Source, bug) of
        Res -> Res
    catch
        exit:{timeout, _} ->
            {error, timeout}
    end.

fake_source_loop(undefined, Version, Cluster) ->
    %?debugMsg("fake loop, no socket"),
    State = #fake_source{version = Version, clustername = Cluster},
    fake_source_loop(State).

fake_source_loop(#fake_source{socket = undefined} = State) ->
    #fake_source{version = Version} = State,
    receive
        {'$gen_call', From, {connected, Socket, _EndPoint, {realtime, Version, Version}}} ->
            %?debugFmt("Connection achieved with valid version: ~p", [Version]),
            inet:setopts(Socket, [{active, once}]),
            gen_server:reply(From, ok),
            fake_source_loop(State#fake_source{socket = Socket});
        {'$gen_call', From, Aroo} ->
            %?debugFmt("Cannot handle request: ~p", [Aroo]),
            gen_server:reply(From, {error, badcall}),
            exit({badcall, Aroo});
        What ->
            %?debugFmt("lolwut? ~p", [What]),
            fake_source_loop(State)
    end;
fake_source_loop(State) ->
    receive
        {'$gen_call', From, {push, Binary, _AlreadyRouted}} when State#fake_source.version == {1,0} ->
            #fake_source{socket = Socket, seq = Seq} = State,
            Payload = riak_repl2_rtframe:encode(objects, {Seq, Binary}),
            gen_tcp:send(Socket, Payload),
            gen_server:reply(From, ok),
            fake_source_loop(State#fake_source{seq = Seq + 1});
        {'$gen_call', From, {push, Binary, AlreadyRouted}} when State#fake_source.version == {2,0} ->
            #fake_source{seq = Seq, clustername = ClusterName} = State,
            Skips2 = case lists:member(ClusterName, AlreadyRouted) of
                true ->
                    State#fake_source.skips + 1;
                false ->
                    Socket = State#fake_source.socket,
                    Meta = orddict:from_list([{skip_count, State#fake_source.skips}, {routed_clusters, [ClusterName | AlreadyRouted]}]),
                    Payload = riak_repl2_rtframe:encode(objects_and_meta, {Seq, Binary, Meta}),
                    gen_tcp:send(Socket, Payload),
                    0
            end,
            gen_server:reply(From, ok),
            fake_source_loop(State#fake_source{seq = Seq + 1, skips = Skips2});
        {'$gen_call', From, bug} ->
            NewBug = case State#fake_source.tcp_bug of
                {data, Bin} ->
                    %?debugMsg("Quicky bug reply"),
                    gen_server:reply(From, {ok, Bin}),
                    undefined;
                _ ->
                    %?debugMsg("Stashing bug for later"),
                    {bug, From}
            end,
            fake_source_loop(State#fake_source{tcp_bug = NewBug});
        {'$gen_call', From, Aroo} ->
            %?debugFmt("Cannot handle request: ~p", [Aroo]),
            gen_server:reply(From, {error, badcall}),
            exit({badcall, Aroo});
        {tcp, Socket, Bin} ->
            %?debugFmt("tcp nom: ~p", [Bin]),
            #fake_source{socket = Socket} = State,
            inet:setopts(Socket, [{active, once}]),
            NewBug = case State#fake_source.tcp_bug of
                {bug, From} ->
                    %?debugMsg("replying to tcp bug"),
                    gen_server:reply(From, {ok, Bin}),
                    undefined;
                _ ->
                    %?debugMsg("Stashing for later tcp bug (maybe)"),
                    {data, Bin}
            end,
            fake_source_loop(State#fake_source{tcp_bug = NewBug});
        {tcp_closed, Socket} when Socket == State#fake_source.socket ->
            %?debugMsg("exit as the socket closed"),
            ok;
        What ->
            %?debugFmt("lolwut? ~p", [What]),
            fake_source_loop(State)
    end.