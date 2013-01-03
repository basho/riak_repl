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
%% wouldn't exceede the max_fssource_cluster setting. Defaults to 1.

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

%% @doc Start a fullsync coordinator for managing a sycn to the remote `Cluster'.
-spec start_link(Cluster :: string()) -> {'ok', pid()}.
start_link(Cluster) ->
    gen_server:start_link(?MODULE, Cluster, []).

%% @doc Begin syncing.  If called while a fullsync is in progress, nothing
%% happens.
-spec start_fullsync(Pid :: pid()) -> 'ok'.
start_fullsync(Pid) ->
    gen_server:cast(Pid, start_fullsync).

%% @doc Stop syncing.  A start will begin the fullsync completely over.
-spec stop_fullsync(Pid :: pid()) -> 'ok'.
stop_fullsync(Pid) ->
    gen_server:cast(Pid, stop_fullsync).

%% @doc Get a status report as a proplist for each fullsync enabled. Usually
%% for use with a console.
-spec status() -> [tuple()].
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

%% @doc Get the status proplist for the given fullsync process. Same as
%% `status(Pid, infinity'.
%% @see status/2
-spec status(Pid :: pid()) -> [tuple()].
status(Pid) ->
    status(Pid, infinity).

%% @doc Get the stats proplist for the given fullsync process, or give up after
%% the timeout.  The atom `infinity' means never timeout.
-spec status(Pid :: pid(), Timeout :: timeout()) -> [tuple()].
status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% @doc Return true if the given fullsync coordiniator is in the middle of
%% syncing, otherwise false.
-spec is_running(Pid :: pid()) -> boolean().
is_running(Pid) when is_pid(Pid) ->
    gen_server:call(Pid, is_running, infinity);
is_running(_Other) ->
    false.

%% ------------------------------------------------------------------
%% connection manager callbacks
%% ------------------------------------------------------------------

%% @hidden
connected(Socket, Transport, Endpoint, Proto, Pid, _Props) ->
    Transport:controlling_process(Socket, Pid),
    gen_server:cast(Pid, {connected, Socket, Transport, Endpoint, Proto}).

%% @hidden
connect_failed(_ClientProto, Reason, SourcePid) ->
    gen_server:cast(SourcePid, {connect_failed, self(), Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @hidden
init(Cluster) ->
    process_flag(trap_exit, true),
    %% ensure that the ring has values for fullsync parameters or load them from the ring
    %% into our application environment.
    riak_repl_ring:reconcile_fs_params_with_ring([{max_fssource_node, ?DEFAULT_SOURCE_PER_NODE},
                                                  {max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER}]),
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

%% @hidden
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
        {busy_nodes, sets:size(State#state.busy_nodes)},
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


%% @hidden
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
            MaxSource = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
            MaxCluster = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
            lager:info("Starting fullsync (source) with max_fssource_node=~p and max_fssource_cluster=~p",
                       [MaxSource, MaxCluster]),
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


%% @hidden
handle_info({'EXIT', Pid, Cause}, State) when Cause =:= normal; Cause =:= shutdown ->
    PartitionEntry = lists:keytake(Pid, 1, State#state.running_sources),
    case PartitionEntry of
        false ->
            % late exit or otherwise non-existant
            {noreply, State};
        {value, {Pid, Partition}, Running} ->

            % likely a slot on the remote node opened up, so re-enable that
            % remote node for whereis requests.
            {_, _, Node} = Partition,
            NewBusies = sets:del_element(Node, State#state.busy_nodes),

            % stats
            Sucesses = State#state.successful_exits + 1,
            State2 = State#state{successful_exits = Sucesses},

            % are we done?
            EmptyRunning =  Running == [],
            QEmpty = queue:is_empty(State#state.partition_queue),
            Waiting = State#state.whereis_waiting,
            case {EmptyRunning, QEmpty, Waiting} of
                {true, true, []} ->
                    lager:info("fullsync complete"),
                    riak_repl_stats:server_fullsyncs(),
                    {noreply, State2#state{running_sources = Running, busy_nodes = NewBusies}};
                _ ->
                    % there's something waiting for a response.
                    State3 = start_up_reqs(State2#state{running_sources = Running, busy_nodes = NewBusies}),
                    {noreply, State3}
            end
    end;

handle_info({'EXIT', Pid, _Cause}, State) ->
    lager:warning("fssource ~p exited abnormally", [Pid]),
    PartitionEntry = lists:keytake(Pid, 1, State#state.running_sources),
    case PartitionEntry of
        false ->
            % late exit
            {noreply, State};
        {value, {Pid, Partition}, Running} ->

            % even a bad exit opens a slot on the remote node
            {_, _, Node} = Partition,
            NewBusies = sets:del_element(Node, State#state.busy_nodes),

            % stats
            ErrorExits = State#state.error_exits + 1,
            #state{partition_queue = PQueue} = State,

            % reset for retry later
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

handle_info(send_next_whereis_req, State) ->
    State2 = case is_fullsync_in_progress(State) of
        true ->
            % this is in response to a potential desync or stale cache of
            % remote nodes, so we'll ditch what we have and try again.
            NewBusies = sets:new(),
            start_up_reqs(State#state{busy_nodes = NewBusies});
        false ->
            State
    end,
    {noreply, State2};

handle_info(_Info, State) ->
    lager:info("ignoring ~p", [_Info]),
    {noreply, State}.


%% @hidden
terminate(_Reason, _State) ->
    ok.


%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

% handle the replies from the fscoordinator_serv, which lives on the sink side.
% we stash on our side what nodes gave a busy reply so we don't send too many
% pointless whereis requests.
handle_socket_msg({location, Partition, {Node, Ip, Port}}, #state{whereis_waiting = Waiting} = State) ->
    case proplists:get_value(Partition, Waiting) of
        undefined ->
            State;
        {N, _OldNode, Tref} ->
            erlang:cancel_timer(Tref),
            Waiting2 = proplists:delete(Partition, Waiting),
            % we don't know for sure it's no longer busy until we get a busy reply
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

% try our best to reach maximum capacity by sending as many whereis requests
% as we can under the condition that we don't overload our local nodes or 
% remote nodes.
start_up_reqs(State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    Running = length(State#state.running_sources),
    Waiting = length(State#state.whereis_waiting),
    StartupCount = Max - Running - Waiting,
    start_up_reqs(State, StartupCount).

start_up_reqs(State, N) when N < 1 ->
    State;
start_up_reqs(State, N) ->
    case send_next_whereis_req(State) of
        {ok, State2} ->
            start_up_reqs(State2, N - 1);
        {defer, State2} ->
            State2
    end.

% If a whereis was send, {ok, #state{}} is returned, else {defer, #state{}}
% this allows the start_up_reqs to stop early.
-spec send_next_whereis_req(State :: #state{}) -> {'defer', #state{}} | {'ok', #state{}}.
send_next_whereis_req(State) ->
    case below_max_sources(State) of
        false ->
            {defer, State};
        true ->
            {Partition, Queue} = determine_best_partition(State),
            case Partition of

                undefined when State#state.whereis_waiting == [], State#state.running_sources == [] ->
                    % something has gone wrong, usually a race condition where we
                    % handled a source exit but the source's supervisor process has
                    % not.  Another possiblity is another fullsync is in resource
                    % contention with use.  In either case, we just need to try
                    % again later.
                    lager:info("No partition available to start, no events outstanding, trying again later"),
                    erlang:send_after(?RETRY_WHEREIS_INTERVAL, self(), send_next_whereis_req),
                    {defer, State#state{partition_queue = Queue}};

                undefined ->
                    % something may have gone wrong, but we have outstanding 
                    % whereis requests or running sources, so we can wait for
                    % one of those to finish and try again
                    {defer, State#state{partition_queue = Queue}};

                {Pval, N, RemoteNode} = P ->
                    #state{transport = Transport, socket = Socket, whereis_waiting = Waiting} = State,
                    Tref = erlang:send_after(?WAITING_TIMEOUT, self(), {Pval, whereis_timeout}),
                    Waiting2 = [{Pval, {N, RemoteNode, Tref}} | Waiting],
                    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
                    lager:info("sending whereis request for partition ~p", [P]),
                    Transport:send(Socket,
                        term_to_binary({whereis, element(1, P), PeerIP, PeerPort})),
                    {ok, State#state{partition_queue = Queue, whereis_waiting =
                        Waiting2}}
            end
    end.

% two specs:  is the local node available, and does our cache of remote nodes
% say the remote node is available.
determine_best_partition(State) ->
    #state{partition_queue = Queue, busy_nodes = Busies, owners = Owners, whereis_waiting = Waiting} = State,
    SeedPart = queue:out(Queue),
    lager:info("starting partition search"),
    determine_best_partition(SeedPart, Busies, Owners, Waiting, queue:new()).

determine_best_partition({empty, _Q}, _Business, _Owners, _Waiting, AccQ) ->
    lager:info("no partition"),
    % there is no best partition, try again later
    {undefined, AccQ};

determine_best_partition({{value, Part}, Queue}, Busies, Owners, Waiting, AccQ) ->
    case node_available(Part, Owners, Waiting) of
        false ->
            determine_best_partition(queue:out(Queue), Busies, Owners, Waiting, queue:in(Part, AccQ));
        skip ->
            determine_best_partition(queue:out(Queue), Busies, Owners, Waiting, AccQ);
        true ->
            case remote_node_available(Part, Busies) of
                false ->
                    determine_best_partition(queue:out(Queue), Busies, Owners, Waiting, queue:in(Part, AccQ));
                true ->
                    {Part, queue:join(Queue, AccQ)}
            end
    end.

% Items in the whereis_waiting list are counted toward max_sources to avoid
% sending a whereis again just because we didn't bother planning ahead.
below_max_sources(State) ->
    Max = app_helper:get_env(riak_repl, max_fssource_cluster, ?DEFAULT_SOURCE_PER_CLUSTER),
    ( length(State#state.running_sources) + length(State#state.whereis_waiting) ) < Max.

node_available({Partition, _, _}, Owners, Waiting) ->
    LocalNode = proplists:get_value(Partition, Owners),
    Max = app_helper:get_env(riak_repl, max_fssource_node, ?DEFAULT_SOURCE_PER_NODE),
    try riak_repl2_fssource_sup:enabled(LocalNode) of
        RunningList ->
            PartsSameNode = [Part || {Part, PNode} <- Owners, PNode =:= LocalNode],
            PartsWaiting = [Part || {Part, _} <- Waiting, lists:member(Part, PartsSameNode)],
            if
                ( length(PartsWaiting) + length(RunningList) ) < Max ->
                    case proplists:get_value(Partition, RunningList) of
                        undefined ->
                            true;
                        _ ->
                            false
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

remote_node_available({_Partition, _, undefined}, _Busies) ->
    true;
remote_node_available({_Partition, _, RemoteNode}, Busies) ->
    not sets:is_element(RemoteNode, Busies).

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
