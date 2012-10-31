%% @doc Coordinates full sync replication parallelism.  Uses 3 riak_repl
%% application env's:  fullsync_on_connect, max_fssource_cluster, and
%% max_fssource_node.
%%
%% ## `{fullsync_on_connect, boolean()}'
%%
%% If true, as soon as a connection to the remote cluster is established,
%% fullsync starts.  If false, then an explicit start must be sent.
%% Defaults to true.
%%
%% ## `{max_fssource_cluster, pos_integer()}'
%%
%% How many sources can be started across all nodes in the local cluster.
%% Defaults to 5.
%%
%% ## `{max_fssource_node, pos_integer()}'
%%
%% How many sources can be started on a single node, provided starting one
%% wouldn't exceede the max_fssource_cluster setting. Defaults to 2.

-module(riak_repl2_fscoordinator).
-behaviour(gen_server).
-define(SERVER, ?MODULE).
-define(DEFAULT_SOURCE_PER_NODE, 2).
-define(DEFAULT_SOURCE_PER_CLUSTER, 5).
% how long to wait for a reply from remote cluster before moving on to
% next partition.
-define(WAITING_TIMEOUT, 5000).

-record(state, {
    leader_node :: 'undefined' | node(),
    leader_pid :: 'undefined' | node(),
    other_cluster,
    socket,
    transport,
    largest_n,
    owners = [],
    connection_ref,
    partition_queue = queue:new(),
    whereis_waiting = [],
    running_sources = [],
    pending_fullsync = false
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, start_fullsync/1, stop_fullsync/1,
    status/0, status/1, status/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% connection manager Function Exports
%% ------------------------------------------------------------------

-export([connected/6,connect_failed/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Cluster) ->
    gen_server:start_link(?MODULE, Cluster, []).

start_fullsync(Pid) ->
    gen_server:cast(Pid, start_fullsync).

stop_fullsync(Pid) ->
    gen_server:cast(Pid, stop_fullsync).

status() ->
    LeaderNode = riak_repl2_leader:leader_node(),
    case LeaderNode of
        undefined ->
            {[], []};
        _ ->
            case riak_repl2_fscoordinator_sup:started(LeaderNode) of
                [] ->
                    [];
                Repls ->
                    [{Remote, status(Pid)} || {Remote, Pid} <- Repls]
            end
    end.

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% ------------------------------------------------------------------
%% connection manager callbacks
%% ------------------------------------------------------------------

connected(Socket, Transport, Endpoint, Proto, Pid, _Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:cast(Pid, {connected, Socket, Transport, Endpoint, Proto}).

connect_failed(_ClientProto, Reason, SourcePid) ->
    gen_server:cast(SourcePid, {connect_failed, self(), Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Cluster) ->
    process_flag(trap_exit, true),
    TcpOptions = [
        {kepalive, true},
        {nodelay, true},
        {packet, 4},
        {active, false}
    ],
    ClientSpec = {{fs_coordinate, [{1,0}]}, {TcpOptions, ?MODULE, self()}},
    case riak_core_connection_mgr:connect({rt_repl, Cluster}, ClientSpec) of
        {ok, Ref} ->
            {ok, #state{other_cluster = Cluster, connection_ref = Ref}};
        {error, Error} ->
            lager:warning("Error connection to remote"),
            {stop, Error}
    end.

handle_call(status, _From, State) ->
    SourceStats = gather_source_stats(State#state.running_sources),
    SelfStats = [
        {cluster, State#state.other_cluster},
        {queued, queue:len(State#state.partition_queue)},
        {in_progress, length(State#state.running_sources)},
        {starting, length(State#state.whereis_waiting)},
        {running_stats, SourceStats}
    ],
    {reply, SelfStats, State};

handle_call(_Request, _From, State) ->
    lager:info("ignoring ~p", [_Request]),
    {reply, ok, State}.

handle_cast({connected, Socket, Transport, _Endpoint, _Proto}, State) ->
    lager:info("fullsync coordinator connected to ~p", [State#state.other_cluster]),
    Transport:setopts(Socket, [{active, once}]),
    State2 = State#state{ socket = Socket, transport = Transport},
    case app_helper:get_env(riak_repl, fullsync_on_connect, true) orelse
        State#state.pending_fullsync of
        true ->
            start_fullsync(self());
        false ->
            ok
    end,
    {noreply, State2};

handle_cast({connect_failed, _From, Why}, State) ->
    lager:info("fullsync remote connection to ~p failed due to ~p, retrying",
        [State#state.other_cluster, Why]),
    {noreply, State};

handle_cast(start_fullsync, #state{socket=undefined} = State) ->
    %% not connected yet...
    {noreply, State#state{pending_fullsync = true}};
handle_cast(start_fullsync,  State) ->
    case is_fullsync_in_progress(State) of
        true ->
            lager:warning("Fullsync already in progress; ignoring start"),
            {noreply, State};
        false ->
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            N = largest_n(Ring),
            Partitions = sort_partitions(Ring),
            FirstN = length(Partitions) div N,
            State2 = State#state{
                largest_n = N,
                owners = riak_core_ring:all_owners(Ring),
                partition_queue = queue:from_list(Partitions)
            },
            State3 = send_whereis_reqs(State2, FirstN),
            {noreply, State3}
    end;

handle_cast(stop_fullsync, State) ->
    % exit all running, cancel all timers, and reset the state.
    [erlang:cancel_timer(Tref) || {_, {_, Tref}} <- State#state.whereis_waiting],
    [begin
        unlink(Pid),
        riak_repl2_fssource:stop_fullsync(Pid)
    end || {Pid, _} <- State#state.running_sources],
    State2 = State#state{
        largest_n = undefined,
        owners = [],
        partition_queue = queue:new(),
        whereis_waiting = [],
        running_sources = []
    },
    {noreply, State2};

handle_cast(_Msg, State) ->
    lager:info("ignoring ~p", [_Msg]),
    {noreply, State}.

handle_info({'EXIT', Pid, Cause}, State) when Cause =:= normal; Cause =:= shutdown ->
    PartitionEntry = lists:keytake(Pid, 1, State#state.running_sources),
    case PartitionEntry of
        false ->
            {noreply, State};
        {value, {Pid, _Partition}, Running} ->
            % are we done?
            EmptyRunning =  Running == [],
            QEmpty = queue:is_empty(State#state.partition_queue),
            Waiting = State#state.whereis_waiting,
            case {EmptyRunning, QEmpty, Waiting} of
                {[], true, []} ->
                    % nothing outstanding, so we can exit.
                    {noreply, State#state{running_sources = Running}};
                _ ->
                    % there's something waiting for a response.
                    State2 = send_next_whereis_req(State#state{running_sources = Running}),
                    {noreply, State2}
            end
    end;

handle_info({'EXIT', Pid, _Cause}, State) ->
    lager:warning("fssource ~p exited abnormally", [Pid]),
    PartitionEntry = lists:keytake(Pid, 1, State#state.running_sources),
    case PartitionEntry of
        false ->
            {noreply, State};
        {value, {Pid, Partition}, Running} ->
            % TODO putting in the back of the queue a good idea?
            #state{partition_queue = PQueue} = State,
            PQueue2 = queue:in(Partition, PQueue),
            State2 = State#state{partition_queue = PQueue2, running_sources = Running},
            State3 = send_next_whereis_req(State2),
            {noreply, State3}
    end;

handle_info({Partition, whereis_timeout}, State) ->
    #state{whereis_waiting = Waiting} = State,
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            % late timeout.
            {noreply, State};
        {N, _Tref} ->
            Waiting2 = proplists:delete(Partition, Waiting),
            Partition1 = {Partition, N},
            Q = queue:in(Partition1, State#state.partition_queue),
            State2 = State#state{whereis_waiting = Waiting2, partition_queue = Q},
            State3 = send_next_whereis_req(State2),
            {noreply, State3}
    end;

handle_info({_Proto, Socket, Data}, #state{socket = Socket} = State) ->
    #state{transport = Transport} = State,
    Transport:setopts(Socket, [{active, once}]),
    Data1 = binary_to_term(Data),
    State2 = handle_socket_msg(Data1, State),
    {noreply, State2};

handle_info({Closed, Socket}, #state{socket = Socket} = State) when
    Closed =:= tcp_closed; Closed =:= ssl_closed ->
    lager:info("Connect closed"),
    % Yes I do want to die horribly; my supervisor should restart me.
    {stop, connection_closed, State};

handle_info({Erred, Socket, Reason}, #state{socket = Socket} = State) when
    Erred =:= tcp_error; Erred =:= ssl_error ->
    lager:error("Connection closed unexpectedly"),
    % Yes I do want to die horribly; my supervisor should restart me.
    {stop, connection_error, State};

handle_info(_Info, State) ->
    lager:info("ignoring ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_socket_msg({location, Partition, {_Node, Ip, Port}}, #state{whereis_waiting = Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {N, Tref} ->
            erlang:cancel_timer(Tref),
            Waiting2 = proplists:delete(Partition, Waiting),
            State2 = State#state{whereis_waiting = Waiting2},
            Partition2 = {Partition, N},
            State3 = start_fssource(Partition2, Ip, Port, State2),
            send_next_whereis_req(State3)
    end;
handle_socket_msg({location_down, Partition}, #state{whereis_waiting=Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {N, Tref} ->
            lager:info("Partition ~p is unavailable on cluster ~p",
                [Partition, State#state.other_cluster]),
            erlang:cancel_timer(Tref),
            Waiting2 = proplists:delete(Partition, Waiting),
            State2 = State#state{whereis_waiting = Waiting2},
            send_next_whereis_req(State2)
    end.

send_whereis_reqs(State, 0) ->
    State;
send_whereis_reqs(State, N) ->
    case send_next_whereis_req(State) of
        State ->
            State;
        State2 ->
            send_whereis_reqs(State2, N - 1)
    end.

send_next_whereis_req(State) ->
    #state{transport = Transport, socket = Socket, partition_queue = PQueue, whereis_waiting = Waiting} = State,
    case queue:out(PQueue) of
        {empty, Q} ->
            case length(Waiting) + length(State#state.running_sources) == 0 of
                true ->
                    lager:info("fullsync coordinator: fullsync complete"),
                    riak_repl_stats:server_fullsyncs();
                _ ->
                    ok
            end,
            State#state{partition_queue = Q};
        {{value, P}, Q} ->
            case below_max_sources(P, State) of
                false ->
                    State;
                true ->
                    {Pval, N} = P,
                    Tref = erlang:send_after(?WAITING_TIMEOUT, self(), {Pval, whereis_timeout}),
                    Waiting2 = [{Pval, {N, Tref}} | Waiting],
                    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
                    lager:info("sending whereis request for partition ~p", [P]),
                    Transport:send(Socket,
                        term_to_binary({whereis, element(1, P), PeerIP, PeerPort})),
                    State#state{partition_queue = Q, whereis_waiting =
                        Waiting2};
                defer ->
                    send_next_whereis_req(State#state{partition_queue =
                            queue:in(P, Q)});
                skip ->
                    send_next_whereis_req(State#state{partition_queue = Q})
            end
    end.

below_max_sources(Partition, State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    if
        ( length(State#state.running_sources) + length(State#state.whereis_waiting) ) < Max ->
            node_available(Partition, State);
        true ->
            false
    end.

node_available({Partition,_}, State) ->
    #state{owners = Owners} = State,
    LocalNode = proplists:get_value(Partition, Owners),
    Max = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
    try riak_repl2_fssource_sup:enabled(LocalNode) of
        RunningList ->
            PartsSameNode = [Part || {Part, PNode} <- Owners, PNode =:= LocalNode, Part],
            PartsWaiting = [Part || {Part, _} <- State#state.whereis_waiting, lists:member(Part, PartsSameNode)],
            lager:info("~p < ~p", [length(PartsWaiting) + length(RunningList), Max]),
            if
                ( length(PartsWaiting) + length(RunningList) ) < Max ->
                    case proplists:get_value(Partition, RunningList) of
                        undefined ->
                            true;
                        _ ->
                            defer
                    end;
                true ->
                    false
            end
    catch
        exit:{noproc, _} ->
            skip;
        exit:{{nodedown, _}, _} ->
            skip
    end.

start_fssource({Partition,_} = PartitionVal, Ip, Port, State) ->
    #state{owners = Owners} = State,
    LocalNode = proplists:get_value(Partition, Owners),
    lager:info("starting fssource for ~p on ~p to ~p", [Partition, LocalNode,
            Ip]),
    {ok, Pid} = riak_repl2_fssource_sup:enable(LocalNode, Partition, {Ip, Port}),
    link(Pid),
    Running = orddict:store(Pid, PartitionVal, State#state.running_sources),
    State#state{running_sources = Running}.

largest_n(Ring) ->
    Defaults = app_helper:get_env(riak_core, default_bucket_props, []),
    Buckets = riak_core_bucket:get_buckets(Ring),
    lists:foldl(fun(Bucket, Acc) ->
                max(riak_core_bucket:n_val(Bucket), Acc)
        end, riak_core_bucket:n_val(Defaults), Buckets).

sort_partitions(Ring) ->
    BigN = largest_n(Ring),
    RawPartitions = [P || {P, _Node} <- riak_core_ring:all_owners(Ring)],
    %% tag partitions with their index, for convienience in detecting preflist
    %% collisions later
    Partitions = lists:zip(RawPartitions,lists:seq(1,length(RawPartitions))),
    %% pick a random partition in the ring
    R = crypto:rand_uniform(0, length(Partitions)),
    %% pretend that the ring starts at offset R
    {A, B} = lists:split(R, Partitions),
    OffsetPartitions = B ++ A,
    %% now grab every Nth partition out of the ring until there are no more
    sort_partitions(OffsetPartitions, BigN, []).

sort_partitions([], _, Acc) ->
    lists:reverse(Acc);
sort_partitions(In, N, Acc) ->
    Split = min(length(In), N) - 1,
    {A, [P|B]} = lists:split(Split, In),
    sort_partitions(B++A, N, [P|Acc]).

gather_source_stats(PDict) ->
    gather_source_stats(PDict, []).

gather_source_stats([], Acc) ->
    lists:reverse(Acc);

gather_source_stats([{Pid, _} | Tail], Acc) ->
    try riak_repl2_fssource:legacy_status(Pid, infinity) of
        Stats ->
            gather_source_stats(Tail, [{Pid, Stats} | Acc])
    catch
        exit:_ ->
            gather_source_stats(Tail, [{Pid, []} | Acc])
    end.

is_fullsync_in_progress(State) ->
    QEmpty = queue:is_empty(State#state.partition_queue),
    Waiting = State#state.whereis_waiting,
    Running = State#state.running_sources,
    case {QEmpty, Waiting, Running} of
        {true, [], []} ->
            false;
        _ ->
            true
    end.
