%% -------------------------------------------------------------------
%%
%% riak_core_connection_mgr_stats: collect, aggregate, and provide stats for
%%                                 connections made by the connection manager
%%
%% Copyright (c) 3012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%

-module(riak_core_connection_mgr_stats).

-behaviour(gen_server).

%% API
-export([start_link/0, get_stats/0,
         update/3, register_stats/0, produce_stats/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(APP, riak_conn_mgr_stats). %% registered in riak_repl_app:start/2

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

register_stats() ->
    [(catch folsom_metrics:delete_metric(Stat)) || Stat <- folsom_metrics:get_metrics(),
                                                   is_tuple(Stat), element(1, Stat) == ?APP],
    [register_stat({?APP, Name}, Type) || {Name, Type} <- stats()],
    riak_core_stat_cache:register_app(?APP, {?MODULE, produce_stats, []}).

%% @spec get_stats() -> proplist()
%% @doc Get the current aggregation of stats.
get_stats() ->
    case riak_core_stat_cache:get_stats(?APP) of
        {ok, Stats, _TS} ->
            Stats;
        Error -> Error
    end.

update(Stat, Addr, ProtocolId) ->
    gen_server:cast(?SERVER, {update, Stat, Addr, ProtocolId}).

%% gen_server

init([]) ->
    register_stats(),
    {ok, ok}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({update, Stat, Addr, ProtocolId}, State) ->
    update3(Stat, Addr, ProtocolId),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Update a stat for given IP-Address, Cluster, and Protocol-id
update3({conn_error, Error}, IPAddr, Protocol) ->
    create_or_update({?APP, conn_error, Error, total}, {inc, 1}, counter),
    create_or_update({?APP, conn_error, Error}, 1, spiral),
    create_or_update({?APP, conn_error, Error, IPAddr, total}, {inc, 1}, counter),
    create_or_update({?APP, conn_error, Error, IPAddr}, 1, spiral),
    create_or_update({?APP, conn_error, Error, Protocol, total}, {inc, 1}, counter),
    create_or_update({?APP, conn_error, Error, Protocol}, 1, spiral);

update3(Stat, IPAddr, Protocol) ->
    create_or_update({?APP, Stat, total}, {inc, 1}, counter),
    create_or_update({?APP, Stat}, 1, spiral),
    create_or_update({?APP, Stat, Protocol, total}, {inc, 1}, counter),
    create_or_update({?APP, Stat, Protocol}, 1, spiral),
    create_or_update({?APP, Stat, IPAddr, total}, {inc, 1}, counter),
    create_or_update({?APP, Stat, IPAddr }, 1, spiral).

%% private

create_or_update(Name, UpdateVal, Type) ->
    case (catch folsom_metrics:notify_existing_metric(Name, UpdateVal, Type)) of
        ok ->
            ok;
        {'EXIT', _} ->
            register_stat(Name, Type),
            create_or_update(Name, UpdateVal, Type)
    end.

register_stat(Name, spiral) ->
    folsom_metrics:new_spiral(Name);
register_stat(Name, counter) ->
    folsom_metrics:new_counter(Name).

%% @spec produce_stats() -> proplist()
%% @doc Produce a proplist-formatted view of the current aggregation
%%      of stats.
produce_stats() ->
    Stats = [Stat || Stat <- folsom_metrics:get_metrics(), is_tuple(Stat), element(1, Stat) == ?APP],
    lists:flatten([{Stat, get_stat(Stat)} || Stat <- Stats]).

get_stat(Name) ->
    folsom_metrics:get_metric_value(Name).

%% static stats
stats() -> []. %% no static stats to register

    
