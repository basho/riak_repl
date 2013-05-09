%% Riak EnterpriseDS
%% Copyright 2007-2013 Basho Technologies, Inc. All Rights Reserved.
%%
%% pg_proxy keeps track of which node is servicing proxy_get requests
%% in the cluster. A client can always make requests to the leader
%% pg_proxy, which will then be routed to the appropriate node in the
%% cluster.
%%

-module(riak_repl2_pg_proxy).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3, proxy_get/4, register_pg_node/2]).

-define(SERVER, ?MODULE).

-record(state, {
        source_cluster = undefined,
        pg_node = undefined
        }).

%%%===================================================================
%%% API
%%%===================================================================
proxy_get(Pid, Bucket, Key, Options) ->
    gen_server:call(Pid, {proxy_get, Bucket, Key, Options}).

register_pg_node(Pid, Node) ->
    gen_server:call(Pid, {register, Node}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec start_link(ProxyName :: string()) -> {'ok', pid()}.
start_link(ProxyName) ->
    gen_server:start_link(?MODULE, ProxyName, []).

%%%===================================================================
%%% Gen_server callbacks
%%%===================================================================

init(ProxyName) ->
    lager:debug("Registering pg_proxy ~p", [ProxyName]),
    erlang:register(ProxyName, self()),
    {ok, #state{}}.

handle_call({proxy_get, Bucket, Key, GetOptions}, _From, #state{pg_node=Node} = State) ->
    case Node of
        undefined ->
            lager:warning("No proxy_get node registered"),
            {reply, ok, State};
        N ->
            RegName = riak_repl_util:make_pg_name(State#state.source_cluster),
            Result = gen_server:call({RegName, N}, {proxy_get, Bucket, Key, GetOptions}),
            {reply, Result, State}
    end;

handle_call({register, ClusterName, Node}, _From, State) ->
    lager:debug("registered node for cluster name ~p", [ClusterName]),
    NewState = State#state{pg_node = Node, source_cluster=ClusterName},
    {reply, ok, NewState}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

