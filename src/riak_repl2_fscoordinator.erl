%% @doc Coordinates full sync replication parallelism.

-module(riak_repl2_fscoordinator).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {
    leader_node :: 'undefined' | node(),
    leader_pid :: 'undefined' | node(),
    other_cluster,
    socket,
    transport,
    largest_n,
    owners = [],
    sources = [],
    connection_ref,
    waiting_partitions = queeu:new(),
    delayed_partitions = queue:new(),
    in_progress_partitions = []
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% connection manager Function Exports
%% ------------------------------------------------------------------

-export([connected/5,connect_failed/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Cluster) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Cluster, []).

%% ------------------------------------------------------------------
%% connection manager callbacks
%% ------------------------------------------------------------------

connected(Socket, Transport, Endpoint, Proto, Pid) ->
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

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({connected, Socket, Transport, _Endpoint, _Proto}, _From, State) ->
    #state{other_cluster = Remote} = State,
    Ring = riak_core_ring_manager:get_my_ring(),
    N = largest_n(Ring),
    [P1 | _] = Partitions = sort_partitions(Ring),
    Owners = riak_core_ring:all_owners(Ring),
    {PeerIP, PeerPort} = inet:peername(Socket),
    State2 = State#state{ owners = Owners, waiting_partitions = Partitions,
        largest_n = N, socket = Socket, transport = Transport},
    riak_repl_tcp_server:send(Transport, Socket, {whereis, P1, PeerIP, PeerPort}),
    % TODO kick off the replication
    % for each P in partition, 
    %   ask local pnode if therea new worker can be started.
    %   if yes
    %       reach out to remote side asking for ip:port of matching pnode
    %       on reply, start worker on local pnode
    %   else
    %       put partition in 'delayed' list
    %   
    % of pnode in that dise
    % for each P in partitions, , reach out to the physical node
    % it lives on, tell it to connect to remote, and start syncing
    % link to the fssources, so they when this does,
    % and so this can handle exits fo them.
    {noreply, State2}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Cause}, State) ->
    Partition = erlang:erase(Pid),
    State2 = case {Cause, Partition} of
        {normal, _} ->
            State;
        {_, undefined} ->
            State;
        {_, _} ->
            Delayed = State#state.delayed_partitions,
            State#state{delayed_partitions = queue:in_r(Delayed, Partitions)}
    end,
    {Next, Q} = queue:out(State2State.delayed_partitions),
    case {Next, State2#state.waiting_partitions} of
        {empty, []} ->
            % TODO not sure if this is the right thing to do,
            % but if all partitions successfully synced, it is done.
            {stop, normal, State2#state{delayed_partitions = Q}};
        {empty, _} ->
            {noreply, State2#state{delayed_partitions = Q}};
        {{value, P}, []} ->
            % there are no outstanding 'whereis' requests
            State3 = State2#state{waiting_partitions = [P]},
            #state{socket = Socket, transport = Transport} = State3,
            riak_repl_tcp_server:send(Transport, Socket, {whereis, P}),
            {noreply, State3};
        {{value, P}, _} ->
            % there are outstnading 'whereis' requests
            {noreply, State2}
    end;

handle_info({Proto, Socket, Data}, #state{socket = Socket} = State) ->
    #state{transport = Transport} = State,
    Transport:setopts(Socket, [{active, once}]),
    Data1 = binary_to_term(Data),
    State2 = handle_socket_msg(Data1, State),
    {noreply, State2};

%handle_info({'EXIT', Pid, Cause}, State) ->
    % TODO: handle when a partition fs exploderizes
%    Partition = erlang:erase(Pid),
%    case {Cause, Partition} of
%        {_, undefined} ->
%            {noreply, State};
%        {normal, _} ->
%            start_fssource
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_socket_msg({location, Partition, {Node, Ip, Port}}, #state{waiting_partitions = [Partition | Tail]} = State) ->
    State2 = start_fssource(Partition, Node, Ip, Port, State#state{waiting_partitions = Tail}),
    case Tail of
        [] ->
            State2;
        [P1 | _] ->
            #state{socket = Socket, transport = Transport} = State2,
            riak_repl_tcp_server:send(Transport, Socket, {whereis, P1}),
            State2
    end.

start_fssource(Partition, RemoteNode, Ip, Port, State) ->
    #state{owners = Owners, other_cluster = Cluster} = State,
    LocalNode = proplists:get_value(Partition, Owners),
    Counts = supervisor:count_children({riak_repl2_fssource_sup, LocalNode}),
    Active = proplists:get_value(active, Counts),
    Max = app_healper:get_env(riak_repl, max_fssource, 5),
    if
        Active < Max ->
            {ok, Pid} = supervisor:start_child({riak_repl2_fssource_sup, LocalNode},
                [Cluster, Partition, RemoteNode, Ip, Port]),
            link(Pid),
            erlang:put(Pid, Partition),
            State;
        true ->
            Delayed = State#state.delayed_partitions ++ [Partition],
            State#state{delayed_partitions = Delayed}
    end.

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

lists_pos(Needle, Haystack) ->
    lists_pos(Needle, Haystack, 1).

lists_pos(_Needle, [], _N) ->
    not_found;

lists_pos(Needle, [Needle | _Haystack], N) ->
    N;

lists_pos(Needle, [_NotNeedle | Haystack], N) ->
    lists_pos(Needle, Haystack, N + 1).
