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
        work_dir
    }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).

%% Register with service manager
register_service() ->
    ProtoPrefs = {fullsync,[{1,0}]},
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

init([Socket, Transport, _Proto, Props]) ->
    SocketTag = riak_repl_util:generate_socket_tag("fs_sink", Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, sink, SocketTag}),

    Cluster = proplists:get_value(clustername, Props),
    lager:info("fullsync connection"),
    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
    %% strategy is hardcoded
    {ok, FullsyncWorker} = riak_repl_keylist_client:start_link(Cluster,
        Transport, Socket, WorkDir),
    {ok, #state{cluster=Cluster, transport=Transport, socket=Socket,
            fullsync_worker=FullsyncWorker, work_dir=WorkDir}}.

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

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.cluster]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.cluster, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    case Msg of
        {fs_diff_obj, Obj} ->
            riak_repl_util:do_repl_put(Obj);
        _ ->
            gen_fsm:send_event(State#state.fullsync_worker, Msg)
    end,
    {noreply, State};
handle_info(init_ack, State=#state{socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

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


