-module(rt_sink_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5008).
-define(all_remotes, ["a", "b", "c", "d", "e"]).

-record(state, {
    remotes_available = ?all_remotes,
    sources = []
    }).

-record(src_state, {
    pids,
    version
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
    oneof(
        [{call, ?MODULE, connect_from_v1, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, connect_from_v2, [remote_name(S)]} || S#state.remotes_available /= []] ++
        [{call, ?MODULE, disconnect, [elements(S#state.sources)]} || S#state.sources /= []] ++
        [{call, ?MODULE, push_object, [elements(S#state.sources)]} || S#state.sources /= []] ++
        [{call, ?MODULE, ack_objects, [elements(S#state.sources)]} || S#state.sources /= []]
        ).

remote_name(#state{remotes_available = []}) ->
    erlang:error(no_name_available);
remote_name(#state{remotes_available = Remotes}) ->
    oneof(Remotes).

precondition(#state{sources = []}, {call, _, disconnect, _Args}) ->
    false;
precondition(#state{sources = Sources}, {call, _, disconnect, [{Remote, _}]}) ->
    is_tuple(lists:keyfind(Remote, 1, Sources));
precondition(S, {call, _, push_object, _Args}) ->
    S#state.sources /= [];
precondition(S, {call, _, Connect, [Remote]}) when Connect == connect_from_v1; Connect == connect_from_v2 ->
    lists:member(Remote, S#state.remotes_available);
precondition(_S, _Call) ->
    true.

%% ====================================================================
%% state generation
%% ====================================================================

initial_state() ->
    % we will be resetting many mecks, which link to this, but we don't
    % want to kill our test process.
    ?debugMsg("initializing state"),
    process_flag(trap_exit, true),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtsink_conn_sup, kill),
    riak_repl2_rtsink_conn_sup:start_link(),
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_core_tcp_mon:start_link(),
    riak_repl_test_util:abstract_stateful(),
    abstract_ring_manager(),
    abstract_ranch(),
    riak_repl_test_util:abstract_gen_tcp(),
    bug_rt(),
    start_service_manager(),
    riak_repl2_rtsink_conn:register_service(),
    abstract_fake_source(),
    #state{}.

next_state(S, Res, {call, _, connect_from_v1, [Remote]}) ->
    ?debugMsg("connect v1 next state"),
    SrcState = #src_state{pids = Res, version = 1},
    next_state_connect(Remote, SrcState, S);

next_state(S, Res, {call, _, connect_from_v2, [Remote]}) ->
    ?debugMsg("connect v2 next state"),
    SrcState = #src_state{pids = Res, version = 2},
    next_state_connect(Remote, SrcState, S);

next_state(S, _Res, {call, _, disconnect, [{Remote, _}]}) ->
    ?debugMsg("disconnect next state"),
    Sources2 = lists:keydelete(Remote, 1, S#state.sources),
    Avails = [Remote | S#state.remotes_available],
    S#state{sources = Sources2, remotes_available = Avails};

next_state(S, _Res, {call, _, push_object, _Args}) ->
    ?debugMsg("next state push object"),
    S;

next_state(S, _Res, {call, _, ack_objects, _Args}) ->
    ?debugMsg("next state ack objects"),
    S.

next_state_connect(Remote, SrcState, State) ->
    Sources = State#state.sources,
    Sources2 = lists:keystore(Remote, 1, Sources, {Remote, SrcState}),
    Avails = lists:delete(Remote, State#state.remotes_available),
    State#state{sources = Sources2, remotes_available = Avails}.

%% ====================================================================
%% postcondition
%% ====================================================================

postcondition(S, C, R) ->
    ?debugMsg("post condition"),
    true.

%% ====================================================================
%% test callbacks
%% ====================================================================

connect_from_v1(Remote) ->
    ?debugMsg("connect v1"),
    stateful:set(symbolic_clustername, Remote),
    {ok, Source} = connect_source({1,0}),
    {ok, Sink} = read_rt_bug(),
    {Source, Sink}.

connect_from_v2(_Remotes) ->
    ?debugMsg("connect v2 test"),
    ok.

disconnect(_Remote) ->
    ok.

push_object(_Remote) ->
    ok.

ack_objects(_) ->
    ok.

%% ====================================================================
%% helpful utility functions
%% ====================================================================

abstract_ranch() ->
    ?debugMsg("abstracting ranch"),
    riak_repl_test_util:reset_meck(ranch),
    meck:expect(ranch, accept_ack, fun(_Listener) -> ok end),
    meck:expect(ranch, start_listener, fun(_IpPort, _MaxListeners, _RanchTCP, RanchOpts, _Module, _SubProtos) ->
        riak_repl_test_util:kill_and_wait(sink_listener),
        ?debugMsg("ranch start listener"),
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
            {ok, stateful:symbolic_clustername()};
        (_Key, fake_ring) ->
            undefined;
        (Key, Ring) ->
            meck:passthrough([Key, Ring])
    end).

sink_listener(Ip, Port, Opts) ->
    ?debugFmt("prelisten arg dump: ~p, ~p, ~p", [Ip, Port, Opts]),
    {ok, Sock} = gen_tcp:listen(Port, [{ip, Ip}, binary, {reuseaddr, true}, {active, false} | Opts]),
    ?debugFmt("Listening on ~p:~p", [Ip, Port]),
    sink_listener(Sock).

sink_listener(ListenSock) ->
    ?debugMsg("preaccept"),
    {ok, Socket} = gen_tcp:accept(ListenSock),
    ?debugMsg("post accept"),
    riak_core_service_mgr:start_link(sink_listener, Socket, gen_tcp, []),
    ?debugMsg("post start link"),
    sink_listener(ListenSock).

bug_rt() ->
    ?debugMsg("installing bug on register sink"),
    riak_repl_test_util:reset_meck(riak_repl2_rt, [passthrough]),
    WhoToTell = self(),
    meck:expect(riak_repl2_rt, register_sink, fun(SinkPid) ->
        ?debugFmt("Sending bug to ~p", [WhoToTell]),
        WhoToTell ! {sink_pid, SinkPid},
        meck:passthrough([SinkPid])
    end).

read_rt_bug() ->
    read_rt_bug(3000).

read_rt_bug(Timeout) ->
    ?debugFmt("reading rt bug until ~p", [Timeout]),
    receive
        {sink_pid, Pid} ->
            {ok, Pid}
    after Timeout ->
        {error, timeout}
    end.

start_service_manager() ->
    ?debugMsg("Starting service manager"),
    riak_repl_test_util:kill_and_wait(riak_core_service_manager),
    riak_core_service_mgr:start_link({"127.0.0.1", ?SINK_PORT}).

abstract_fake_source() ->
    ?debugMsg("abstacting fake source"),
    riak_repl_test_util:reset_meck(fake_source),
    meck:expect(fake_source, connected, fun(Socket, Transport, Endpoint, Proto, Pid, Props) ->
        ?debugFmt("fake source connect fun call!~n"
            "    Socket: ~p~n"
            "    Transport: ~p~n"
            "    Endpoint: ~p~n"
            "    Proto: ~p~n"
            "    Pid: ~p~n"
            "    Props: ~p", [Socket, Transport, Endpoint, Proto, Pid, Props]),
        Transport:controlling_process(Socket, Pid),
        Transport:setopts(Socket, [{active, once}]),
        gen_server:call(Pid, {connected, Socket, Endpoint, Proto})
    end).

connect_source(Version) ->
    ?debugFmt("starting fake source version ~p", [Version]),
    Pid = proc_lib:spawn_link(?MODULE, fake_source_loop, [undefined, Version]),
    TcpOptions = [{keepalive, true}, {nodelay, true}, {packet, 0},
        {active, false}],
    ClientSpec = {{realtime, [Version]}, {TcpOptions, fake_source, Pid}},
    IpPort = {{127,0,0,1}, ?SINK_PORT},
    ?debugFmt("attempting to connect~n"
        "    IpPort: ~p~n"
        "    ClientSpec: ~p", [IpPort, ClientSpec]),
    ConnRes = riak_core_connection:sync_connect(IpPort, ClientSpec),
    ?debugFmt("Connection result: ~p", [ConnRes]),
    {ok, Pid}.

fake_source_loop(undefined, Version) ->
    ?debugMsg("fake loop, no socket"),
    receive
        {'$gen_call', From, {connected, Socket, _EndPoint, {realtime, Version, Version}}} ->
            ?debugFmt("Connection achieved with valid version: ~p", [Version]),
            gen_server:reply(From, ok),
            fake_source_loop(Socket, Version);
        {'$gen_call', From, Aroo} ->
            ?debugFmt("Cannot handle request: ~p", [Aroo]),
            gen_server:reply(From, {error, badcall}),
            exit({badcall, Aroo});
        What ->
            ?debugFmt("lolwut? ~p", [What]),
            fake_source_loop(undefined, Version)
    end;
fake_source_loop(Socket, Version) ->
    receive
        {'$gen_call', From, Aroo} ->
            ?debugFmt("Cannot handle request: ~p", [Aroo]),
            gen_server:reply(From, {error, badcall}),
            exit({badcall, Aroo});
        {tcp, Socket, Bin} ->
            inet:setopts(Socket, [{active, once}]),
            fake_source_loop(Socket, Version)
    end.