%% Riak Replication Subprotocol Server Dispatch and Client Connections
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_conn_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SERVER, riak_core_connection_manager).
-define(MAX_LISTENERS, 100).

%% connection manager state:
%% cluster_finder := function that returns the ip address
%% registrations := registered protocols, key :: proto_id()
-record(state, {is_paused = false :: boolean(),
                dispatch_addr = {"localhost", 9000} :: ip_addr(),
                cluster_finder = fun() -> {error, undefined} end :: cluster_finder_fun(),
                registrations = orddict:new() :: orddict:orddict(),
                dispatcher_pid = undefined :: pid()
               }).

-export([start_link/1,
         resume/0,
         pause/0,
         is_paused/0,
         set_cluster_finder/1,
         get_cluster_finder/0,
         register_protocol/1,
         unregister_protocol_id/1,
         is_registered/1
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%% start the Connection Manager on the given Ip Address and Port.
%% All sub-protocols will be dispatched from there.
-spec(start_link(ip_addr()) -> {ok, pid()}).
start_link({IP,Port}) ->
    Args = [{IP,Port}],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

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

%% return paused state
is_paused() ->
    gen_server:call(?SERVER, is_paused).

%% Specify a function that will return the IP/Port of our Cluster Manager.
%% Connection Manager will call this function each time it wants to find the
%% current ClusterManager
-spec(set_cluster_finder(cluster_finder_fun()) -> ok).
set_cluster_finder(Fun) ->
    gen_server:cast(?SERVER, {set_cluster_finder, Fun}).

%% Return the current function that finds the Cluster Manager
get_cluster_finder() ->
    gen_server:call(?SERVER, get_cluster_finder).

%% Once a protocol specification is registered, it will be kept available by the
%% Connection Manager. See the protospec() type defined in the Connection layer.
-spec(register_protocol(protospec()) -> ok).
register_protocol(Protocol) ->
    gen_server:cast(?SERVER, {register_protocol, Protocol}).

%% Unregister the given protocol-id.
%% Existing connections for this protocol are not killed. New connections
%% for this protocol will not be accepted until re-registered.
-spec(unregister_protocol_id(proto_id()) -> ok).
unregister_protocol_id(ProtocolId) ->
    gen_server:cast(?SERVER, {unregister_protocol_id, ProtocolId}).

-spec(is_registered(proto_id()) -> boolean()).
is_registered(ProtocolId) ->
    gen_server:call(?SERVER, {is_registered, ProtocolId}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([IpAddr]) ->
    process_flag(trap_exit, true),
    {ok, #state{is_paused = true,
                dispatch_addr = IpAddr
               }}.

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

handle_call({is_registered, ProtocolId}, _From, State) ->
    Found = orddict:is_key(ProtocolId, State#state.registrations),
    {reply, Found, State};

handle_call(get_cluster_finder, _From, State) ->
    {reply, State#state.cluster_finder, State};

handle_call(Unhandled, _From, State) ->
    ?debugFmt("Unhandled gen_server call: ~p", [Unhandled]),
    {reply, {error, unhandled}, State}.

handle_cast(pause, State) ->
    NewState = pause_services(State),
    {noreply, NewState};

handle_cast(resume, State) ->
    NewState = resume_services(State),
    {noreply, NewState};

handle_cast({set_cluster_finder, FinderFun}, State) ->
    {noreply, State#state{cluster_finder=FinderFun}};

handle_cast({register_protocol, Protocol}, State) ->
    {{ProtocolId,_Revs},_M,_F,_A} = Protocol,
    NewDict = orddict:store(ProtocolId, Protocol, State#state.registrations),
    {noreply, State#state{registrations=NewDict}};

handle_cast({unregister_protocol_id, ProtocolId}, State) ->
    NewDict = orddict:erase(ProtocolId, State#state.registrations),
    {noreply, State#state{registrations=NewDict}};

handle_cast(Unhandled, _State) ->
    ?debugFmt("Unhandled gen_server cast: ~p", [Unhandled]),
    {error, unhandled}. %% this will crash the server

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

%% resume, start registered protocols
resume_services(State) ->
    case orddict:size(State#state.registrations) of
        0 ->
            %% no registered protocols yet
            State#state{is_paused=false};
        _NotZero ->
            TcpOptions = [{keepalive, true},
                          {nodelay, true},
                          {packet, 4},
                          {reuseaddr, true},
                          {active, false}],
            IpAddr = State#state.dispatch_addr,
            Protos = [Proto || {_Id, Proto} <- orddict:to_list(State#state.registrations)],
            {ok, Pid} = riak_core_connection:start_dispatcher(IpAddr, ?MAX_LISTENERS,
                                                              TcpOptions, Protos),
            State#state{is_paused=false, dispatcher_pid=Pid}
    end.

%% kill existing service dispatcher if running
pause_services(State) when State#state.is_paused == true ->
    State;
pause_services(State) ->
    case State#state.dispatcher_pid of
        undefined ->
            State#state{is_paused=true};
        _Pid ->
            IpAddr = State#state.dispatch_addr,
            ok = riak_core_connection:stop_dispatcher(IpAddr),
            State#state{is_paused=true, dispatcher_pid=undefined}
    end.
