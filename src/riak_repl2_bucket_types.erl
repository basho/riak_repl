-module(riak_repl2_bucket_types).


%% check for bucket_type changes on THIS cluster every N seconds,
%% notify the cluster manager of the changes to exchange with 
%% sinks

-define(SERVER, riak_repl2_bucket_types).
%%-include_lib("riak_core/include/riak_core_connection.hrl").
-behaviour(gen_server).

-define(BUCKET_TYPE_PREFIX, {core, bucket_types}).
-define(REPL_WHITELIST_PREFIX, {<<"replication">>, <<"whitelist">>}).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         get_bucket_types_list/0,
         store_whitelist/1,
         bucket_type_list/0,
         bucket_type_hash/0]).

-record(state, {
           bucket_types_hash=undefined
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    %% TODO: make this work for multiple sinks
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Retrieve the list of bucket types on this cluster
-spec(get_bucket_types_list() -> [term()]).
get_bucket_types_list() ->
    gen_server:call(?SERVER, {get_bucket_types_list}).

%% @doc Store the whitelist from the sink
store_whitelist(WL) ->
    gen_server:call(?SERVER, {store_whitelist, WL}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_bucket_types, _From, State) ->
  {reply, ok, State};

handle_call(get_bucket_types_list, _From, State) ->
    BL = bucket_type_list(),
    {reply, BL, State};

handle_call({store_whitelist, BL}, _From, State) ->
    save_whitelist(BL),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
bucket_type_list() ->
    It = riak_core_bucket_type:iterator(),
    Acc = bucket_type_build_list(It, []),
    io:format(user, ">>> ~p~n", [Acc]),
    Acc.

bucket_type_build_list(It, Acc) ->
    case riak_core_bucket_type:itr_done(It) of
        true ->
            riak_core_bucket_type:itr_close(It),
            [ riak_core_connection:symbolic_clustername() | Acc ];
        false ->
            BP = {_Type, Props} = riak_core_bucket_type:itr_value(It),
            case proplists:get_value(active, Props, false) of
               true -> bucket_type_build_list( riak_core_bucket_type:itr_next(It), [BP | Acc]);
               false -> bucket_type_build_list(riak_core_bucket_type:itr_next(It), Acc)
            end
    end.

bucket_type_hash() ->
   io:format(user, "<<<~p~n",
             [riak_core_metadata:prefix_hash(?BUCKET_TYPE_PREFIX)]).

save_whitelist(BL) ->
    [CN | BLT] = BL,
    lager:info("Storing whitelist for cluster: ~p", [CN]),
    riak_core_metadata:put(?REPL_WHITELIST_PREFIX, CN, BLT).
