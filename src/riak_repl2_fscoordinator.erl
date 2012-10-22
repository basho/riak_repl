%% @doc Coordinates full sync replication parallelism.

-module(riak_repl2_fscoordinator).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {
    leader_node :: 'undefined' | node(),
    leader_pid :: 'undefined' | node(),
    other_cluster,
    sources = [],
    connection_ref
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
    gen_server:call(Pid, {connected, Socket, Transport, Endpoint, Proto}).

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

handle_call({connected, Socket, Transport, _Endpoint, _Proto}, _From, State) ->
    #state{other_cluster = Remote} = State,
    Ring = riak_core_ring_manager:get_my_ring(),
    N = largest_n(Ring),
    Partitions = sort_partitions(Ring),
    
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
    Owners = riak_core_ring:all_owners(Ring),
    FsSourcePids = start_fssources(Partitions, State#state.other_cluster, Ring),
    {reply, ok, State};
    
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(start, State) ->
    #state{other_cluster = Remote} = State,
    Ring = riak_core_ring_manager:get_my_ring(),
    N = largest_n(Ring),
    Partitions = sort_partitions(Ring),
    % TODO kick off the replication
    % for each P in partitions, , reach out to the physical node
    % it lives on, tell it to connect to remote, and start syncing
    % link to the fssources, so they when this does,
    % and so this can handle exits fo them.
    Owners = riak_core_ring:all_owners(Ring),
    FsSourcePids = start_fssources(Partitions, State#state.other_cluster, Ring),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

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

start_fssources(Partitions, Cluster, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    start_fssources(Partitions, Cluster, Owners, [], []).

start_fssources([], _Cluster, Owners, InProgress, Delayed) ->
    {ordsets:from_list(InProgress), ordsets:from_list(Delayed)};

start_fssources([Partition | Tail], Cluster, Owners, InProgress, Delayed) ->
    Node = proplists:get_value(Partition, Owners),
    {InProg1, Delayed1} = case start_fssource(Node, Cluster, Partition) of
        {ok, Pid} ->
            link(Pid),
            erlang:put(Pid, Partition),
            {[Partition| InProgress], Delayed};
        {error, max_syncs} ->
            {InProgress, [Partition | Delayed]};
        Else ->
            lager:warning("Could not start partion full sync:  ~p", [Else]),
            {InProgress, [Partition | Delayed]}
    end,
    start_fssources(Tail, Cluster, Owners, InProg1, Delayed1).

start_fssource(Node, Cluster, Partition) ->
    Counts = supervisor:count_children({riak_repl2_fssource_sup, Node}),
    Active = proplists:get_value(active, Counts),
    Max = app_helper:get_env(riak_repl, max_fssource, 5),
    if
        Active < Max ->
            supervisor:start_child({riak_repl2_fssource_sup, Node}, [Cluster, Partition]);
        true ->
            {error, max_syncs}
    end.

largest_n(Ring) ->
    Defaults = app_helper:get_env(riak_core, default_bucket_props, []),
    Buckets = riak_core_bucket:get_buckets(Ring),
    lists:foldl(fun(Bucket, Acc) ->
                max(riak_core_bucket:n_val(Bucket), Acc)
        end, riak_core_bucket:n_val(Defaults), Buckets).

sort_partitions(Ring) ->
    BigN = largest_n(Ring),
    %% pick a random partition in the ring
    Partitions = [P || {P, _Node} <- riak_core_ring:all_owners(Ring)],
    R = crypto:rand_uniform(0, length(Partitions)),
    %% pretend that the ring starts at offset R
    {A, B} = lists:split(R, Partitions),
    OffsetPartitions = B ++ A,
    %% now grab every Nth partition out of the ring until there are no more
    sort_partitions(OffsetPartitions, BigN, []).

sort_partitions([], _, Acc) ->
    lists:reverse(Acc);
sort_partitions(In, N, Acc) ->
    Split = case length(In) >= N of
        true ->
            N - 1;
        false ->
            length(In) -1
    end,
    {A, [P|B]} = lists:split(Split, In),
    sort_partitions(B++A, N, [P|Acc]).
