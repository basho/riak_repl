%%
%% Eunit tests for source/sink communictation between protocol versions.
%%

-module(riak_repl2_rt_source_sink_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5007).
-define(SOURCE_PORT, 4007).
-define(PROTO_V1_SOURCE_V1_SINK, {realtime, {1, 0}, {1, 0}}).

-record(v1_source_v1_sink, {
    sink, source, tcp_mon, rt
}).

v1_source_v1_sink_test_() ->
    {setup, fun() ->
        abstract_gen_tcp(),
        {ok, RT} = riak_repl2_rt:start_link(),
        {ok, _} = riak_repl2_rtq:start_link(),
        {ok, TCPMon} = riak_core_tcp_mon:start_link(),
        {ok, ListenPid} = start_sink(),
        {ok, {Source, Sink}} = start_source(),
        #v1_source_v1_sink{sink = Sink, source = Source, tcp_mon = TCPMon, rt = RT}
    end,
    fun(State) ->
        #v1_source_v1_sink{sink = Sink, source = Source, tcp_mon = TCPMon,
            rt = RT} = State,
        unlink(TCPMon),
        exit(TCPMon, kill),
        unlink(RT),
        exit(RT, kill),
        meck:unload(riak_core_service_mgr),
        meck:unload(riak_core_connection_mgr),
        riak_repl2_rtsource_conn:stop(Source),
        riak_repl2_rtsink_conn:stop(Sink),
        riak_repl2_rtq:shutdown(),
        meck:unload(gen_tcp)
    end,
    fun(State) -> [

        {"Everything started okay", fun() ->
            #v1_source_v1_sink{sink = Sink, source = Source} = State,
            ?assert(is_pid(Sink)),
            ?assert(is_pid(Source)),
            ?assert(is_process_alive(Sink)),
            ?assert(is_process_alive(Source))
        end}

    ] end}.

abstract_gen_tcp() ->
    meck:new(gen_tcp, [unstick, passthrough]),
    meck:expect(gen_tcp, setopts, fun(Socket, Opts) ->
        inet:setopts(Socket, Opts)
    end).

abstract_rt() ->
    meck:new(riak_repl2_rt),
    meck:expect(riak_repl2_rt, register_sink, fun(_SinkPid) ->
        ok
    end).

start_sink() ->
    TellMe = self(),
    meck:new(riak_core_service_mgr, [passthrough]),
    meck:expect(riak_core_service_mgr, register_service, fun(HostSpec, _Strategy) ->
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArg}} = HostSpec,
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, TcpOpts),
        TellMe ! sink_listening,
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, Pid} = riak_repl2_rtsink_conn:start_link(?PROTO_V1_SOURCE_V1_SINK, "source_cluster"),
        %unlink(Pid),
        ok = gen_tcp:controlling_process(Socket, Pid),
        ok = riak_repl2_rtsink_conn:set_socket(Pid, Socket, gen_tcp),
        TellMe ! {sink_started, Pid}
    end),
    Pid = proc_lib:spawn_link(?MODULE, listen_sink, []),
    receive
        sink_listening ->
            {ok, Pid}
    after 10000 ->
            {error, timeout}
    end.

listen_sink() ->
    riak_repl2_rtsink_conn:register_service().

start_source() ->
    meck:new(riak_core_connection_mgr, [passthrough]),
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec) ->
        spawn_link(fun() ->
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
            {ok, Socket} = gen_tcp:connect("localhost", ?SINK_PORT, TcpOpts),
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT}, ?PROTO_V1_SOURCE_V1_SINK, Pid, [])
        end),
        {ok, make_ref()}
    end),
    {ok, SourcePid} = riak_repl2_rtsource_conn:start_link("sink_cluster"),
    %unlink(SourcePid),
    receive
        {sink_started, SinkPid} ->
            {ok, {SourcePid, SinkPid}}
    after 1000 ->
        {error, timeout}
    end.
