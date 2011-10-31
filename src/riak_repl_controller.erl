%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_controller).
-author('Andy Gross <andy@andygross.org>').
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([set_is_leader/1]).

-include("riak_repl.hrl").

-record(state, {
          repl_config :: dict(),
          is_leader   :: boolean()
      }).

%% api
          
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

set_is_leader(IsLeader) when is_boolean(IsLeader) ->
    gen_server:call(?MODULE, {set_leader, IsLeader}, infinity).

%% gen_server 

init([]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplConfig = 
        case riak_repl_ring:get_repl_config(Ring) of
            undefined ->
                riak_repl_ring:initial_config();
            RC -> RC
        end,
    {ok, #state{repl_config=ReplConfig,
                is_leader=false
            }}.

handle_call({set_leader, true}, _From, State=#state{is_leader=true}) ->
    {reply, ok, State#state{is_leader=true}};
handle_call({set_leader, false}, _From, State=#state{is_leader=false}) ->
    {reply, ok, State#state{is_leader=false}};
handle_call({set_leader, true}, _From, State=#state{is_leader=false}) ->
    NewState = State#state{is_leader=true},
    handle_became_leader(NewState),
    {reply, ok, NewState};
handle_call({set_leader, false}, _From, State=#state{is_leader=true}) ->
    NewState = State#state{is_leader=false},
    handle_lost_leader(),
    {reply, ok, NewState}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% handler functions

handle_became_leader(_State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_client_sup:ensure_sites(Ring).

handle_lost_leader() ->
    RunningSiteProcs = riak_repl_client_sup:running_site_procs(),
    [riak_repl_client_sup:stop_site(SiteName) || 
        {SiteName, _Pid} <- RunningSiteProcs],
    riak_repl_listener:close_all_connections().
