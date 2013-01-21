%%
%% Eunit tests for source/sink communictation between protocol versions.
%%

-module(riak_repl2_rt_source_sink_tests).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SINK_PORT, 5007).
-define(SOURCE_PORT, 4007).
-define(VER1, {1,0}).
-define(PROTOCOL(NegotiatedVer), {realtime, NegotiatedVer, NegotiatedVer}).
-define(PROTOCOL_V1, ?PROTOCOL(?VER1)).
-define(PROTO_V1_SOURCE_V1_SINK, ?PROTOCOL(?VER1)).

-record(v1_source_v1_sink, {
    sink, source, tcp_mon, rt
}).

-record(connection_tests, {
    tcp_mon, rt
}).

connection_test_() ->
    {foreach, fun() ->
        abstract_gen_tcp(),
        {ok, RT} = riak_repl2_rt:start_link(),
        {ok, _} = riak_repl2_rtq:start_link(),
        {ok, TCPMon} = riak_core_tcp_mon:start_link(),
        ?debugFmt("rt: ~p; tcp_mon: ~p", [RT, TCPMon]),
        #connection_tests{tcp_mon = TCPMon, rt = RT}
    end,
    fun(State) ->
        #connection_tests{tcp_mon = TCPMon, rt = RT} = State,
        unlink(TCPMon),
        exit(TCPMon, kill),
        unlink(RT),
        exit(RT, kill),
        Rtq = whereis(riak_repl2_rtq),
        unlink(Rtq),
        exit(Rtq, kill),
        wait_for_pid(TCPMon),
        wait_for_pid(RT),
        wait_for_pid(Rtq),
        meck:unload(gen_tcp)
    end, [

        fun(State) -> {"v1 to v1 communication", setup,
            fun() ->
                {ok, _ListenPid} = start_sink(?VER1),
                {ok, {Source, Sink}} = start_source(?VER1),
                meck:new(poolboy, [passthrough]),
                meck:expect(poolboy, checkout, fun(_ServName, _SomeBool, _Timeout) ->
                        spawn(fun() -> ok end)
                end),
                {State, Source, Sink}
            end,
            fun({_State, Source, Sink}) ->
                meck:unload(poolboy),
                connection_test_teardown_pids(Source, Sink)
            end,
            fun({_State, Source, Sink}) -> [

                {"everything started okay", fun() ->
                    assert_living_pids([Source, Sink])
                end},

                {"sending objects", fun() ->
                    Self = self(),
                    meck:new(riak_repl_fullsync_worker),
                    meck:expect(riak_repl_fullsync_worker, do_binputs, fun(_Worker, <<"der object">>, DoneFun, riak_repl2_rtsink_pool) ->
                        Self ! continue,
                        Self ! {state, DoneFun},
                        ok
                    end),
                    riak_repl2_rtq:push(1, <<"der object">>),
                    MeckOk = wait_for_continue(),
                    ?assertEqual(ok, MeckOk),
                    meck:unload(riak_repl_fullsync_worker)
                end},

                {"assert done", fun() ->
                    {ok, DoneFun} = extract_state_msg(),
                    %?assert(is_function(DoneFun)),
                    DoneFun(),
                    ?assert(riak_repl2_rtq:all_queues_empty())
                end}

            ] end}
        end

    ]}.

assert_living_pids([]) ->
    true;
assert_living_pids([Pid | Tail]) ->
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),
    assert_living_pids(Tail).

%% The start for both source and sink start up the mecks, which link to
%% the calling process. This means the meck needs to be unloaded before that
%% process exits, or there will be a big dump in the console for no reason.
connection_test_teardown_pids(Source, Sink) ->
    meck:unload(riak_core_service_mgr),
    meck:unload(riak_core_connection_mgr),
    riak_repl2_rtsource_conn:stop(Source),
    riak_repl2_rtsink_conn:stop(Sink),
    wait_for_pid(Source),
    wait_for_pid(Sink).

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
    start_sink(?VER1).

start_sink(Version) ->
    TellMe = self(),
    meck:new(riak_core_service_mgr, [passthrough]),
    meck:expect(riak_core_service_mgr, register_service, fun(HostSpec, _Strategy) ->
        {_Proto, {TcpOpts, _Module, _StartCB, _CBArg}} = HostSpec,
        {ok, Listen} = gen_tcp:listen(?SINK_PORT, [binary | TcpOpts]),
        TellMe ! sink_listening,
        {ok, Socket} = gen_tcp:accept(Listen),
        {ok, Pid} = riak_repl2_rtsink_conn:start_link(?PROTOCOL(Version), "source_cluster"),
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
    start_source(?VER1).

start_source(NegotiatedVer) ->
    meck:new(riak_core_connection_mgr, [passthrough]),
    meck:expect(riak_core_connection_mgr, connect, fun(_ServiceAndRemote, ClientSpec) ->
        spawn_link(fun() ->
            {_Proto, {TcpOpts, Module, Pid}} = ClientSpec,
            {ok, Socket} = gen_tcp:connect("localhost", ?SINK_PORT, [binary | TcpOpts]),
            ok = Module:connected(Socket, gen_tcp, {"localhost", ?SINK_PORT}, ?PROTOCOL(?VER1), Pid, [])
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

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit, Pid, erlang:process_info(Pid)}
    end. 

wait_for_continue() ->
    wait_for_continue(1000).

wait_for_continue(Timeout) ->
    receive
        continue ->
            ok
    after Timeout ->
        {error, timeout}
    end.

% yes, this is nasty hack to get side effects between tests.
extract_state_msg() ->
    receive
        {state, Data} ->
            {ok, Data}
    after 0 ->
        {error, nostate}
    end.

