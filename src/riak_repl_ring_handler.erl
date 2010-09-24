%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_ring_handler).
-author('Andy Gross <andy@basho.com>').

-behaviour(gen_event).

-include("riak_repl.hrl").

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, { ring :: tuple() }).


%% ===================================================================
%% gen_event Callbacks
%% ===================================================================

init([]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            {ok, #state{ring=Ring}};
        _ ->
            reset_leader(Ring),
            {ok, #state{ring=Ring}}
    end.


handle_event({ring_update, Ring}, State=#state{ring=Ring}) ->
    %% Ring is unchanged from previous notification
    {ok, State};
handle_event({ring_update, NewRing}, State=#state{ring=OldRing}) ->
    %% Ring has changed.
    FinalRing = init_repl_config(OldRing, NewRing),
    reset_leader(FinalRing),
    riak_repl_controller:set_repl_config(riak_repl_ring:get_repl_config(FinalRing)),
    {ok, State#state{ring=FinalRing}};
handle_event(_Event, State) ->
    {ok, State}.


handle_call(_Request, State) ->
    {ok, ok, State}.


handle_info(_Info, State) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%%
%% Initialize repl config structure as necessary
%%
init_repl_config(OldRing, NewRing) ->
    %% Check the repl config for changes. The ring is updated
    %% such that we are guaranteed to have an initialized data
    %% structure after this function returns. If necessary, we
    %% also push out the changed ring.
    OldRC = riak_repl_ring:get_repl_config(OldRing),
    NewRC = riak_repl_ring:get_repl_config(NewRing),
    case {OldRC, NewRC} of
        {undefined, undefined} ->
            update_ring(riak_repl_ring:init_repl_config());

        {_, undefined} ->
            update_ring(OldRC);

        _ ->
            NewRing
    end.


%%
%% Given a new repl config, update the system-local ring.
%%
update_ring(ReplConfig) ->
    error_logger:info_msg("Repushing new replconfig!\n", []),
    F = fun(Ring, _) ->
                {new_ring, riak_repl_ring:set_repl_config(Ring, ReplConfig)}
        end,
    {ok, FinalRing} = riak_core_ring_manager:ring_trans(F, undefined),
    riak_core_ring_manager:write_ringfile(),
    FinalRing.


%%
%% Stop and restart the leader if appropriate; it's always necessary
%% to stop the leader as this function is only invoked when the ring
%% has changed in some way.
%%
reset_leader(Ring) ->
    riak_repl_sup:stop_leader(),

    case should_start_leader(Ring) of
        {true, Args} ->
            riak_repl_sup:start_leader(Args);
        _ ->
            ok
    end.

has_sites(ReplConfig) ->
    dict:fetch(sites, ReplConfig) /= [].

has_listeners(ReplConfig) ->
    dict:fetch(listeners, ReplConfig) /= [].

listener_nodes(ReplConfig) ->
    Listeners = dict:fetch(listeners, ReplConfig),
    [L#repl_listener.nodename || L <- Listeners].

should_start_leader(Ring) ->
    AllNodes = riak_core_ring:all_members(Ring),
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            {false, [[], [], false]};
        RC ->
            Listeners = listener_nodes(RC),
            Workers = ordsets:to_list(
                        ordsets:subtract(ordsets:from_list(AllNodes),
                                         ordsets:from_list(Listeners))),
            case {has_sites(RC), has_listeners(RC)} of
                {true, true} ->
                    {true, [Listeners, Workers, true]};
                {true, false} ->
                    {true, [AllNodes, [], false]};
                {false, true} ->
                    {true, [Listeners, Workers, true]};
                {false, false} ->
                    {false, [[], [], false]}
            end
    end.

