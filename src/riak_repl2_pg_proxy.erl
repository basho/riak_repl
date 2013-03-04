%%%-------------------------------------------------------------------
%%% Created : 21 Feb 2013 by Dave Parfitt
%%%-------------------------------------------------------------------
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
    lager:info("riak_repl2_pg_proxy proxy name ~p",[ProxyName]),
    gen_server:start_link(?MODULE, ProxyName, []).

%%%===================================================================
%%% Gen_server callbacks
%%%===================================================================

init(ProxyName) ->
    lager:info("Registering ~p", [ProxyName]),
    erlang:register(ProxyName, self()),
    {ok, #state{}}.

handle_call({proxy_get, _Bucket, _Key, _Options}, _From, #state{pg_node=Node} = State) ->
    case Node of 
        undefined -> lager:error("No proxy_get node registered");
        N -> lager:info("PG_PROXY says hello w/ node ~p", [N])
    end,
    {reply, ok, State};

handle_call({register, ClusterName, Node}, _From, State) ->
    lager:info("registered node for cluster name ~p", [ClusterName]),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================
