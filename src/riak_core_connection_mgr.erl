%% Riak Replication Subprotocol Server Dispatch and Client Connections
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_connection_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% controls retry and backoff.
%% This combo will cause attempts
-define(INITIAL_DELAY, 250). %% milliseconds
-define(MAX_DELAY, 1000).

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

-define(SERVER, riak_core_connection_manager).
-define(MAX_LISTENERS, 100).

%% connection manager state:
%% cluster_finder := function that returns the ip address
-record(state, {is_paused = false :: boolean(),
                peer_nodes = [] :: [ip_addr()],
                cluster_finder = fun() -> {error, undefined} end :: cluster_finder_fun(),
                dispatcher_pid = undefined :: pid()
               }).

-export([start_link/0,
         set_peers/1,
         get_peers/0,
         resume/0,
         pause/0,
         is_paused/0,
         set_cluster_finder/1,
         get_cluster_finder/0,
         connect/2
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([try_connect/3, add_connection_proc/4]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() -> {ok, pid()}).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% set a list of peer node IP/Port addresses of the remote cluster.
set_peers(PeerNodeAddrs) ->
    gen_server:cast(?SERVER, {set_peers, PeerNodeAddrs}).

get_peers() ->
    gen_server:call(?SERVER, get_peers).

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

%% Establish a connection to the remote destination. be persistent about it,
%% but not too annoying to the remote end. Connect by name of cluster or
%% IP address.
-spec(connect({name,clustername()} | {addr,ip_addr()}, clientspec()) -> pid()).
connect(Dest, ClientSpec) ->
    gen_server:cast(?SERVER, {connect, Dest, ClientSpec, default}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{is_paused = true}}.

handle_call(get_peers, _From, State) ->
    {reply, State#state.peer_nodes, State};

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

handle_call(get_cluster_finder, _From, State) ->
    {reply, State#state.cluster_finder, State};

handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.

handle_cast({set_peers, PeerNodeAddrs}, State) ->
    {noreply, State#state{peer_nodes = PeerNodeAddrs}};

handle_cast(pause, State) ->
    {noreply, State#state{is_paused = true}};

handle_cast(resume, State) ->
    {noreply, State#state{is_paused = false}};

handle_cast({set_cluster_finder, FinderFun}, State) ->
    {noreply, State#state{cluster_finder=FinderFun}};

handle_cast({connect, Dest, Protocol, Strategy}, State) ->
    CMFun = State#state.cluster_finder,
    spawn(?MODULE, add_connection_proc, [Dest, Protocol, Strategy, CMFun]),
    {noreply, State};

handle_cast(_Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [_Unhandled])),
    {error, unhandled}. %% this will crash the server

handle_info(_Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [_Unhandled])),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

extend_delay(Delay) ->
    Delay * 2.

try_connect(_Addr, _Protocol, Delay) when Delay > ?MAX_DELAY ->
    ?TRACE(?debugFmt("try_connect: giving up on ~p after ~p msec",
                     [_Addr, Delay])),
    {error, timedout};
try_connect(Addr, Protocol, Delay) ->
    ?TRACE(?debugFmt("try_connect: trying ~p", [Addr])),
    case riak_core_connection:sync_connect(Addr, Protocol) of
        ok ->
            ok;
        {error, _Reason} ->
            %% try again after some backoff
            NewDelay = extend_delay(Delay),
            ?TRACE(?debugFmt("try_connect: waiting ~p msecs", [Delay])),
            timer:sleep(NewDelay),
            try_connect(Addr, Protocol, NewDelay)
    end.

%% a spawned process that will try and connect to a remote address
%% or named cluster, with retries on failure to connect.
add_connection_proc({addr, Addr}, Protocol, _Strategy, _CMFun) ->
    try_connect(Addr, Protocol, ?INITIAL_DELAY);
add_connection_proc({name,RemoteCluster}, Protocol, default, CMFun) ->
    {ok,ClusterManagerNode} = CMFun(),
    ?TRACE(?debugFmt("Got cluster node = ~p", [ClusterManagerNode])),
    {{ProtocolId, _Revs}, _Rest} = Protocol,
    Resp = gen_server:call({?CLUSTER_MANAGER_SERVER, ClusterManagerNode},
                           {get_addrs_for_proto_id, RemoteCluster, ProtocolId},
                           ?CM_CALL_TIMEOUT),
    ?TRACE(?debugFmt("Got ip_addrs response = ~p", [Resp])),
    case Resp of
        {ok, Addrs} ->
            %% try all in list until success
            Addr = hd(Addrs),
            try_connect(Addr, Protocol, ?INITIAL_DELAY);
        _Error ->
            ?TRACE(?debugFmt("add_connection_proc: failed to reach cluster manager: ~p",
                             [_Error])),
            ok
    end.
