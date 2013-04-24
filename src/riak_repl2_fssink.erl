-module(riak_repl2_fssink).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/4, register_service/0, start_service/5, legacy_status/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        cluster,
        fullsync_worker,
        work_dir,
        ver              % highest common wire protocol in common with fs source
    }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).

%% Register with service manager
register_service() ->
    %% use 1,1 proto for new binary object
    ProtoPrefs = {fullsync,[{1,1}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, false},
                  {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:register_service(HostSpec, {round_robin, undefined}).

%% Callback from service manager
start_service(Socket, Transport, Proto, _Args, Props) ->
    {ok, Pid} = riak_repl2_fssink_sup:start_child(Socket, Transport,
        Proto, Props),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.

legacy_status(Pid, Timeout) ->
    gen_server:call(Pid, legacy_status, Timeout).

%% gen server

init([Socket, Transport, OKProto, Props]) ->
    %% TODO: remove annoying 'ok' from service mgr proto
    {ok, Proto} = OKProto,
    Ver = riak_repl_util:deduce_wire_version_from_proto(Proto),
    SocketTag = riak_repl_util:generate_socket_tag("fs_sink", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, sink,
                                       SocketTag}, Transport),

    Cluster = proplists:get_value(clustername, Props),
    lager:info("fullsync connection"),
    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
    %% strategy is hardcoded
    {ok, FullsyncWorker} = riak_repl_keylist_client:start_link(Cluster,
        Transport, Socket, WorkDir),
    {ok, #state{cluster=Cluster, transport=Transport, socket=Socket,
            fullsync_worker=FullsyncWorker, work_dir=WorkDir, ver=Ver}}.

handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket}) ->
    Res = case is_pid(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    SocketStats = riak_core_tcp_mon:socket_status(Socket),
    Desc =
        [
            {node, node()},
            {site, State#state.cluster},
            {strategy, fullsync},
            {fullsync_worker, riak_repl_util:safe_pid_to_list(State#state.fullsync_worker)},
            {socket, riak_core_tcp_mon:format_socket_stats(SocketStats, [])}
        ],
    {reply, Desc ++ Res, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Closed, Socket}, State=#state{socket=Socket})
        when Closed == tcp_closed; Closed == ssl_closed ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({Error, _Socket, Reason}, State)
        when Error == tcp_error; Error == ssl_error ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    case decode_obj_msg(Data) of
        {fs_diff_obj, RObj} ->
            riak_repl_util:do_repl_put(RObj);
        Other ->
            gen_fsm:send_event(State#state.fullsync_worker, Other)
    end,
    {noreply, State};
handle_info(init_ack, State=#state{socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

decode_obj_msg(Data) ->
    Msg = binary_to_term(Data),
    case Msg of
        {fs_diff_obj, BObj} when is_binary(BObj) ->
            RObj = riak_repl_util:from_wire(BObj),
            {fs_diff_obj, RObj};
        {fs_diff_obj, _RObj} ->
            Msg;
        Other ->
            Other
    end.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    case is_pid(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    %% clean up work dir
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
