-module(riak_repl2_fssink).
-include("riak_repl.hrl").

-behaviour(gen_server).
%% API
-export([start_link/4, register_service/0, start_service/5, legacy_status/2, fullsync_complete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        transport,
        socket,
        cluster,
        fullsync_worker,
        work_dir = undefined,
        strategy :: keylist | aae,
        ver              % highest common wire protocol in common with fs source
    }).

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, [Socket, Transport, Proto, Props], []).

fullsync_complete(Pid) ->
    %% cast to avoid deadlock in terminate
    gen_server:cast(Pid, fullsync_complete).

%% Register with service manager
register_service() ->
    %% 1,0 and up supports keylist strategy
    %% 1,1 and up supports binary object
    %% 2,0 and up supports AAE strategy
    ProtoPrefs = {fullsync,[{1,1}, {2,0}]},
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, false},
                  {nodelay, true}],
    HostSpec     = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
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
    lager:info("Negotiated ~p with ver ~p", [Proto, Ver]),
    SocketTag = riak_repl_util:generate_socket_tag("fs_sink", Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, sink,
                                       SocketTag}, Transport),

    Cluster = proplists:get_value(clustername, Props),
    lager:info("fullsync connection"),

    {_Proto,{CommonMajor,_CMinor},{CommonMajor,_HMinor}} = Proto,
    case CommonMajor of
        1 ->
            %% Keylist server strategy
            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, Cluster),
            {ok, FullsyncWorker} = riak_repl_keylist_client:start_link(Cluster, Transport,
                                                                       Socket, WorkDir),
            {ok, #state{cluster=Cluster, transport=Transport, socket=Socket,
                        fullsync_worker=FullsyncWorker, work_dir=WorkDir,
                        strategy=keylist, ver=Ver}};
        2 ->
            %% AAE strategy
            {ok, FullsyncWorker} = riak_repl_aae_sink:start_link(Cluster, Transport, Socket, self()),
            {ok, #state{transport=Transport, socket=Socket, cluster=Cluster,
                        fullsync_worker=FullsyncWorker, strategy=aae, ver=Ver}}
    end.

handle_call(legacy_status, _From, State=#state{fullsync_worker=FSW,
                                               socket=Socket}) ->
    Res = case is_process_alive(FSW) of
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

handle_cast(fullsync_complete, State) ->
    %% sent from AAE fullsync worker
    lager:info("Fullsync complete."),
    {stop, normal, State};
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
    %% aae strategy will not receive messages here
    Transport:setopts(Socket, [{active, once}]),
    case decode_obj_msg(Data) of
        {fs_diff_obj, RObj} ->
            riak_repl_util:do_repl_put(RObj);
        Other ->
            gen_fsm:send_event(State#state.fullsync_worker, Other)
    end,
    {noreply, State};
handle_info(init_ack, State=#state{socket=Socket,
                                   transport=Transport,
                                   fullsync_worker=FullsyncWorker,
                                   strategy=Strategy}) ->
    case Strategy of
        keylist ->
            Transport:setopts(Socket, [{active, once}]);
        aae ->
            ok = Transport:controlling_process(Socket, FullsyncWorker),
            riak_repl_aae_sink:init_sync(FullsyncWorker)
    end,
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
    lager:info("FS sink terminating."),
    case is_process_alive(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    %% clean up work dir
    case WorkDir of
        undefined ->
            ok;
        _Other ->
            Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
            os:cmd(Cmd)
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
