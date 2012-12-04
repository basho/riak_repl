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
-include("riak_repl.hrl").

-behaviour(gen_server).
-define(SERVER, ?MODULE).

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
    busy_nodes = sets:new(),
    %whereis_busies = 0,
    %max_whereis_busies = 0,
    running_sources = [],
    successful_exits = 0,
    error_exits = 0,
    pending_fullsync = false
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, start_fullsync/1, stop_fullsync/1,
    status/0, status/1, status/2, is_running/1]).

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

is_running(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, is_running, infinity);
is_running(_Other) ->
    false.

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

handle_call(status, _From, State = #state{socket=Socket}) ->
    SourceStats = gather_source_stats(State#state.running_sources),
    SocketStats = riak_core_tcp_mon:format_socket_stats(
        riak_core_tcp_mon:socket_status(Socket), []),
    SelfStats = [
        {cluster, State#state.other_cluster},
        {queued, queue:len(State#state.partition_queue)},
        {in_progress, length(State#state.running_sources)},
        {starting, length(State#state.whereis_waiting)},
        {successful_exits, State#state.successful_exits},
        {error_exits, State#state.error_exits},
        {running_stats, SourceStats},
        {socket, SocketStats}
    ],
    {reply, SelfStats, State};

handle_call(is_running, _From, State) ->
    RunningSrcs = State#state.running_sources,
    % are we done?
    QEmpty = queue:is_empty(State#state.partition_queue),
    Waiting = State#state.whereis_waiting,
    case {RunningSrcs, QEmpty, Waiting} of
        {[], true, []} ->
            % nothing outstanding, so we can exit.
            {reply, false, State};
        _ ->
            % there's something waiting for a response.
            {reply, true, State}
    end;


handle_call(_Request, _From, State) ->
    lager:info("ignoring ~p", [_Request]),
    {reply, ok, State}.

handle_cast({connected, Socket, Transport, _Endpoint, _Proto}, State) ->
    lager:info("fullsync coordinator connected to ~p", [State#state.other_cluster]),
    SocketTag = riak_repl_util:generate_socket_tag("fs_coord", Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, coord,
                                       SocketTag}, Transport),

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
    lager:warning("fullsync remote connection to ~p failed due to ~p, retrying",
                  [State#state.other_cluster, Why]),
    {stop, normal, State};

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
            State2 = State#state{
                largest_n = N,
                owners = riak_core_ring:all_owners(Ring),
                partition_queue = queue:from_list(Partitions),
                successful_exits = 0,
                error_exits = 0
            },
            %State3 = send_next_whereis_req(State2),
            State3 = start_up_reqs(State2),
            {noreply, State3}
    end;

handle_cast(stop_fullsync, State) ->
    % exit all running, cancel all timers, and reset the state.
    [erlang:cancel_timer(Tref) || {_, {_, Tref}} <- State#state.whereis_waiting],
    [begin
        unlink(Pid),
        riak_repl2_fssource:stop_fullsync(Pid),
        riak_repl2_fssource_sup:disable(node(Pid), Part)
    end || {Pid, {Part, _PartN}} <- State#state.running_sources],
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
        {value, {Pid, Partition}, Running} ->
            % are we done?
            {_, _, Node} = Partition,
            NewBusies = sets:del_element(Node, State#state.busy_nodes),
            Sucesses = State#state.successful_exits + 1,
            State2 = State#state{successful_exits = Sucesses},
            EmptyRunning =  Running == [],
            QEmpty = queue:is_empty(State#state.partition_queue),
            Waiting = State#state.whereis_waiting,
            case {EmptyRunning, QEmpty, Waiting} of
                {[], true, []} ->
                    % nothing outstanding, so we can exit.
                    {noreply, State2#state{running_sources = Running, busy_nodes = NewBusies}};
                _ ->
                    % there's something waiting for a response.
                    %State3 = send_next_whereis_req(State2#state{running_sources = Running, busy_nodes = NewBusies}),
                    State3 = start_up_reqs(State2#state{running_sources = Running, busy_nodes = NewBusies}),
                    {noreply, State3}
            end
    end;

handle_info({'EXIT', Pid, _Cause}, State) ->
    lager:warning("fssource ~p exited abnormally", [Pid]),
    PartitionEntry = lists:keytake(Pid, 1, State#state.running_sources),
    case PartitionEntry of
        false ->
            {noreply, State};
        {value, {Pid, Partition}, Running} ->
            {_, _, Node} = Partition,
            NewBusies = sets:del_element(Node, State#state.busy_nodes),
            % TODO putting in the back of the queue a good idea?
            ErrorExits = State#state.error_exits + 1,
            #state{partition_queue = PQueue} = State,
            PQueue2 = queue:in(Partition, PQueue),
            State2 = State#state{partition_queue = PQueue2, busy_nodes = NewBusies,
                running_sources = Running, error_exits = ErrorExits},
            State3 = start_up_reqs(State2),
            {noreply, State3}
    end;

handle_info({Partition, whereis_timeout}, State) ->
    #state{whereis_waiting = Waiting} = State,
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            % late timeout.
            {noreply, State};
        {N, NodeData, _Tref} ->
            Waiting2 = proplists:delete(Partition, Waiting),
            Partition1 = {Partition, N, NodeData},
            Q = queue:in(Partition1, State#state.partition_queue),
            State2 = State#state{whereis_waiting = Waiting2, partition_queue = Q},
            State3 = start_up_reqs(State2),
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

handle_info({Erred, Socket, _Reason}, #state{socket = Socket} = State) when
    Erred =:= tcp_error; Erred =:= ssl_error ->
    lager:error("Connection closed unexpectedly"),
    % Yes I do want to die horribly; my supervisor should restart me.
    {stop, connection_error, State};

handle_info(send_next_whereis_req, #state{whereis_waiting = [], running_sources = []} = State) ->
    NewBusies = sets:new(),
    State2 = start_up_reqs(State#state{busy_nodes = NewBusies}),
    {noreply, State2};

handle_info(send_next_whereis_req, State) ->
    NewBusies = sets:new(),
    State2 = start_up_reqs(State#state{busy_nodes = NewBusies}),
    {noreply, State2};

%handle_info(retry_whereis, State) ->
%    %NewBusies = State#state.whereis_busies - 1,
%    PQueue = State#state.partition_queue,
%    State2 = send_next_whereis_req(State#state{whereis_busies = 0, max_whereis_busies = queue:len(PQueue)}),
%    {noreply, State2};

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

%decrement_nonneg(0) -> 0;
%decrement_nonneg(N) -> N-1.

%increment_if(true, N) -> N+1;
%increment_if(false, N) -> N.

handle_socket_msg({location, Partition, {Node, Ip, Port}}, #state{whereis_waiting = Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {N, _OldNode, Tref} ->
            erlang:cancel_timer(Tref),
            Waiting2 = proplists:delete(Partition, Waiting),
            %CurrentBusies = decrement_nonneg(State#state.whereis_busies),
            NewBusies = sets:del_element(Node, State#state.busy_nodes),
            State2 = State#state{whereis_waiting = Waiting2, busy_nodes = NewBusies},
            Partition2 = {Partition, N, Node},
            State3 = start_fssource(Partition2, Ip, Port, State2),
            start_up_reqs(State3)
    end;
handle_socket_msg({location_busy, Partition, Node}, #state{whereis_waiting = Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {N, _OldNode, Tref} ->
            lager:info("Partition ~p is too busy on cluster ~p at node ~p", [Partition, State#state.other_cluster, Node]),
            erlang:cancel_timer(Tref),

            Waiting2 = proplists:delete(Partition, Waiting),
            State2 = State#state{whereis_waiting = Waiting2},

            Partition2 = {Partition, N, Node},
            PQueue = State2#state.partition_queue,
            PQueue2 = queue:in(Partition2, PQueue),
            NewBusies = sets:add_element(Node, State#state.busy_nodes),
            State3 = State2#state{partition_queue = PQueue2, busy_nodes = NewBusies},
            start_up_reqs(State3)
            %MaxBusies = app_helper:get_env(riak_repl, max_fs_busies_tolerated, ?DEFAULT_MAX_FS_BUSIES_TOLERATED),
            %MaxBusies = State#state.max_whereis_busies,
            %NewBusies = increment_if((State#state.whereis_busies < MaxBusies), State#state.whereis_busies),
            %State3 = State2#state{partition_queue = PQueue2, whereis_busies = NewBusies},

%            case queue:peek(PQueue2) of
%                Partition2 ->
%                    % we where just told it was busy, so no point in asking
%                    % again until a fullsync for another partition is done
%                    State3;
%                _ when NewBusies < MaxBusies ->
%                    send_next_whereis_req(State3);
%                _ ->
%                    erlang:send_after(10000, self(), retry_whereis),
%                    lager:info("Too many location_busy threshold reached, waiting retry timer"),
%                    State3
%            end
    end;
handle_socket_msg({location_down, Partition, _Node}, #state{whereis_waiting=Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {_N, _OldNode, Tref} ->
            lager:info("Partition ~p is unavailable on cluster ~p",
                [Partition, State#state.other_cluster]),
            erlang:cancel_timer(Tref),
            Waiting2 = proplists:delete(Partition, Waiting),
            State2 = State#state{whereis_waiting = Waiting2},
            start_up_reqs(State2)
    end.

start_up_reqs(State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    Running = length(State#state.running_sources),
    Waiting = length(State#state.whereis_waiting),
    StartupCount = Max - Running - Waiting,
    start_up_reqs(State, StartupCount).

start_up_reqs(State, N) when N < 1 ->
    State;
start_up_reqs(State, N) ->
    State2 = send_next_whereis_req(State),
    start_up_reqs(State2, N - 1).

send_next_whereis_req(State) ->
    #state{transport = Transport, socket = Socket, whereis_waiting = Waiting} = State,
    %case queue:out(PQueue) of
    case nab_next(State) of
        {empty, Q} ->
            case length(Waiting) + length(State#state.running_sources) == 0 of
                true ->
                    lager:info("fullsync coordinator: fullsync complete"),
                    riak_repl_stats:server_fullsyncs();
                _ ->
                    ok
            end,
            case queue:is_empty(Q) of
                false ->
                    erlang:send_after(1000, self(), send_next_whereis_req);
                true ->
                    ok
            end,
            State#state{partition_queue = Q};
        {{value, P}, Q} ->
            case below_max_sources(P, State) of
                false ->
                    State;
                true ->
                    {Pval, N, RemoteNode} = P,
                    Tref = erlang:send_after(?WAITING_TIMEOUT, self(), {Pval, whereis_timeout}),
                    Waiting2 = [{Pval, {N, RemoteNode, Tref}} | Waiting],
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

nab_next(State) ->
    #state{partition_queue = PQueue, busy_nodes = Busies} = State,
    MaxLoops = queue:len(PQueue),
    nab_next(PQueue, Busies, MaxLoops).

nab_next(PQueue, _Business, 0) ->
    {empty, PQueue};
nab_next(PQueue, Busies, N) ->
    case queue:out(PQueue) of
        {empty, _PQueue2} = Out ->
            Out;
        {{value, {_, _, undefined}}, _PQueue2}  = Out ->
            Out;
        {{value, {_, _, Node} = PartitionInfo}, PQueue2} = MaybeOut ->
            case sets:is_element(Node, Busies) of
                true ->
                    PQueue3 = queue:in(PartitionInfo, PQueue2),
                    nab_next(PQueue3, Busies, N - 1);
                false ->
                    MaybeOut
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

node_available({Partition,_,_}, State) ->
    #state{owners = Owners} = State,
    LocalNode = proplists:get_value(Partition, Owners),
    Max = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
    try riak_repl2_fssource_sup:enabled(LocalNode) of
        RunningList ->
            PartsSameNode = [Part || {Part, PNode} <- Owners, PNode =:= LocalNode],
            PartsWaiting = [Part || {Part, _} <- State#state.whereis_waiting, lists:member(Part, PartsSameNode)],
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

start_fssource({Partition,_,_} = PartitionVal, Ip, Port, State) ->
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
    [{P,N,undefined} || {P,N} <- lists:reverse(Acc)];
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
            gather_source_stats(Tail, [{riak_repl_util:safe_pid_to_list(Pid), Stats} | Acc])
    catch
        exit:_ ->
            gather_source_stats(Tail, [{riak_repl_util:safe_pid_to_list(Pid), []} | Acc])
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
