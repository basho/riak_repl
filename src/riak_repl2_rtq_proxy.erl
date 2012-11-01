%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtq_proxy).

%% @doc A proxy process that handles realtime messages received while and
%% after the riak_repl application has shut down. This allows us to avoid
%% dropping realtime messages around shutdown events.

-behaviour(gen_server).

%% API
-export([start/0, push/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {nodes=[]}).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

push(NumItems, Bin) ->
    gen_server:cast(?MODULE, {push, NumItems, Bin}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    Nodes = riak_repl_util:get_peer_repl_nodes(),
    %% need to change the group leader so we don't die with the rest of the
    %% repl app.
    erlang:group_leader(whereis(user), self()),
    [erlang:monitor(process, {riak_repl2_rtq, Node}) || Node <- Nodes],
    {ok, #state{nodes=Nodes}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({push, NumItems, _Bin}, State = #state{nodes=[]}) ->
    lager:warning("No available nodes to proxy ~p objects to", [NumItems]),
    {noreply, State};
handle_cast({push, NumItems, Bin}, State) ->
    lager:info("lol proxy"),
    Node = hd(State#state.nodes),
    Nodes = tl(State#state.nodes),
    lager:debug("Proxying ~p items to ~p", [NumItems, Node]),
    gen_server:cast({riak_repl2_rtq, Node}, {push, NumItems, Bin}),
    {noreply, State#state{nodes=Nodes ++ [Node]}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, _, {riak_repl2_rtq, Node}, _}, State) ->
    lager:info("rtq proxy target ~p is down", [Node]),
    {noreply, State#state{nodes=State#state.nodes -- [Node]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

