%% Riak Replication Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_conn_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SERVER, riak_core_connection_manager).

-record(state, {running = false}).

-export([start_link/0,
         resume/0,
         pause/0,
         set_cluster_finder/1,
         register_protocol/1,
         unregister_protocol/1
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% resume() will begin/resume accepting and establishing new connections, in
%% order to maintain the protocols that have been (or continue to be) registered
%% and unregistered. pause() will not kill any existing connections, but will
%% cease accepting new requests or retrying lost connections.
-spec(resume() -> ok).
resume() ->
    gen_server:cast(?SERVER, resume).

-spec(pause() -> ok).
pause() ->
    gen_server:cast(?SERVER, pause).

%% Specify a function that will return the IP/Port of our Cluster Manager.
%% Connection Manager will call this function each time it wants to find the
%% current ClusterManager
-spec(set_cluster_finder(fun(() -> {ok,ip_addr()} | {error, term()})) -> ok).
set_cluster_finder(Fun) ->
    gen_server:cast(?SERVER, {set_cluster_finder, Fun}).

%% Once a protocol specification is registered, it will be kept available by the
%% Connection Manager. See the protospec() type defined in the Connection layer
-spec(register_protocol(protospec()) -> ok).
register_protocol(Protocol) ->
    gen_server:cast(?SERVER, {register_protocol, Protocol}).

-spec(unregister_protocol(protospec()) -> ok).
unregister_protocol(Protocol) ->
    gen_server:cast(?SERVER, {unregister_protocol, Protocol}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{running = false}}.

handle_call(Unhandled, _From, State) ->
    ?debugFmt("Unhandled gen_server call: ~p", [Unhandled]),
    {reply, {error, unhandled}, State}.

handle_cast(Unhandled, State) ->
    ?debugFmt("Unhandled gen_server cast: ~p", [Unhandled]),
    {reply, {error, unhandled}, State}.

handle_info(Unhandled, State) ->
    ?debugFmt("Unhandled gen_server info: ~p", [Unhandled]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================
