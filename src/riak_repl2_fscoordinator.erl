%% @doc Coordinates full sync replication parallelism.

-module(riak_repl2_fscoordinator).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {
    leader_node :: 'undefined' | node(),
    leader_pid :: 'undefined' | node(),
    other_cluster
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
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Cluster) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Cluster, []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Cluster) ->
    process_flag(trap_exit, true),
    gen_server:cast(?SERVER, start),
    {ok, #state{other_cluster = Cluster}}.

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
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

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
