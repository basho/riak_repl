%% Riak Core Cluster Manager Connections to Remote Clusters
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% Connections get started by who-ever and they report to the cluster manager.
%% Once an ip-address has been resolved to a cluster, the cluster manager
%% might remove the connection if it already has a connection to that cluster.

-module(riak_core_cluster_conn).

-behavior(gen_fsm).

-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([current_state/1]).

%% For testing, we need to have two different cluster manager services running
%% on the same node, which is normally not done. The remote cluster service is
%% the one we're testing, so use a different protocol for the client connection
%% during eunit testing, which will emulate a cluster manager from the test.
-define(REMOTE_CLUSTER_PROTO_ID, test_cluster_mgr).
-else.
-define(REMOTE_CLUSTER_PROTO_ID, ?CLUSTER_PROTO_ID).
-endif.

%% API
-export([start_link/1,
         start_link/2,
         status/1,
         connected/6,
         connect_failed/3,
         stop/1]).

%% gen_fsm callbacks
-export([init/1,
         initiating_connection/2,
         initiating_connection/3,
         connecting/2,
         connecting/3,
         waiting_for_cluster_name/2,
         waiting_for_cluster_name/3,
         waiting_for_cluster_members/2,
         waiting_for_cluster_members/3,
         connected/2,
         connected/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-type remote() :: {cluster_by_name, clustername()} | {cluster_by_addr, ip_addr()}.
-type peer_address() :: {string(), pos_integer()}.
-record(state, {mode :: atom(),
                remote :: remote(),
                socket :: port(),
                name :: clustername(),
                previous_name="undefined" :: clustername(),
                members=[] :: [peer_address()],
                connection_ref :: reference(),
                connection_timeout :: timeout(),
                transport :: atom(),
                address :: peer_address(),
                connection_props :: proplist:proplist()}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% start a connection with a locator type, either {cluster_by_name, clustername()}
%% or {cluster_by_addr, ip_addr()}. This is asynchronous. If it dies, the connection
%% supervisior will restart it.
-spec start_link(remote()) -> {ok, pid()} | {error, term()}.
start_link(Remote) ->
    start_link(Remote, normal).

-spec start_link(remote(), atom()) -> {ok, pid()} | {error, term()}.
start_link(Remote, Mode) ->
    gen_fsm:start_link(?MODULE, [Remote, Mode], []).

-spec stop(pid()) -> ok.
stop(Ref) ->
    gen_fsm:sync_send_all_state_event(Ref, force_stop).

-spec status(pid()) -> term().
status(Ref) ->
    gen_fsm:sync_send_event(Ref, status).

-spec connected(port(), atom(), ip_addr(), term(), term(), proplists:proplist()) -> ok.
connected(Socket,
          Transport,
          Addr,
          {?REMOTE_CLUSTER_PROTO_ID, _MyVer, _RemoteVer},
          {_Remote, Client},
          Props) ->
    %% give control over the socket to the `Client' process.
    %% tell client we're connected and to whom
    Transport:controlling_process(Socket, Client),
    gen_fsm:send_event(Client,
                       {connected_to_remote, Socket, Transport, Addr, Props}).

-spec connect_failed({term(), term()}, {error, term()}, {_, atom() | pid() | port() | {atom(), _} | {via, _, _}}) -> ok.
connect_failed({_Proto, _Vers}, {error, _}=Error, {_Remote, Client}) ->
    %% increment stats for "client failed to connect"
    riak_repl_stats:client_connect_errors(),
    %% tell client we bombed and why
    gen_fsm:send_event(Client, {connect_failed, Error}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

-spec init([remote() | atom()]) -> {ok, initiating_connection, state(), timeout()}.
init([Remote, test]) ->
    _ = lager:debug("connecting to ~p", [Remote]),
    State = #state{connection_timeout=infinity,
                   mode=test,
                   remote=Remote},
    {ok, initiating_connection, State};
init([Remote, Mode]) ->
    _ = lager:debug("connecting to ~p", [Remote]),
    State = #state{connection_timeout=?INITIAL_CONNECTION_RESPONSE_TIMEOUT,
                   mode=Mode,
                   remote=Remote},
    {ok, initiating_connection, State, 0}.

%% Async message handling for the `initiating_connection' state
initiating_connection(timeout, State) ->
    UpdState = initiate_connection(State),
    {next_state, connecting, UpdState, ?CONNECTION_SETUP_TIMEOUT + 5000};
initiating_connection(_, State) ->
    {next_state, initiating_connection, State}.

%% Sync message handling for the `initiating_connection' state
initiating_connection(status, _From, _State) ->
    {reply, initiating_connection, initiating_connection, _State};
initiating_connection(_, _From, _State) ->
    {reply, ok, initiating_connection, _State}.

%% Async message handling for the `connecting' state
connecting(timeout, State=#state{remote=Remote}) ->
    %% @TODO Original comment below says we want to die, but code was
    %% implemented to loop in the same function. Determine which is
    %% the better choice. Stopping for now.

    %% die with error once we've passed the timeout period that the
    %% core_connection module will expire. Go round and let the connection
    %% manager keep trying.
    _ = lager:debug("cluster_conn: client timed out waiting for ~p", [Remote]),
    {stop, giving_up, State};
connecting({connect_failed, Error}, State=#state{remote=Remote}) ->
    _ = lager:debug("ClusterManager Client: connect_failed to ~p because ~p."
                    " Will retry.",
                    [Remote, Error]),
    _ = lager:warning("ClusterManager Client: connect_failed to ~p because ~p."
                      " Will retry.",
                      [Remote, Error]),
    %% This is fatal! We are being supervised by conn_sup and if we
    %% die, it will restart us.
    {stop, Error, State};
connecting({connected_to_remote, Socket, Transport, Addr, Props}, State) ->
    RemoteName = proplists:get_value(clustername, Props),
    _ = lager:debug("Cluster Manager control channel client connected to"
                    " remote ~p at ~p named ~p",
                    [State#state.remote, Addr, RemoteName]),
    _ = lager:debug("Cluster Manager control channel client connected to"
                    " remote ~p at ~p named ~p",
                    [State#state.remote, Addr, RemoteName]),
    UpdState = State#state{socket=Socket,
                           transport=Transport,
                           address=Addr,
                           connection_props=Props},
    _ = request_cluster_name(UpdState),
    {next_state, waiting_for_cluster_name, UpdState, ?CONNECTION_SETUP_TIMEOUT};
connecting(poll_cluster, State) ->
    %% cluster manager doesn't know we haven't connected yet.
    %% just ignore this while we're waiting to connect or fail
    {next_state, connecting, State};
connecting(Other, State=#state{remote=Remote}) ->
    _ = lager:error("cluster_conn: client got unexpected "
                    "msg from remote: ~p, ~p",
                    [Remote, Other]),
    {next_state, connecting, State}.

%% Sync message handling for the `connecting' state
connecting(status, _From, State) ->
    {reply, connecting, connecting, State};
connecting(_, _From, _State) ->
    {reply, ok, connecting, _State}.

%% Async message handling for the `waiting_for_cluster_name' state
waiting_for_cluster_name({cluster_name, NewName}, State=#state{previous_name="undefined"}) ->
    UpdState = State#state{name=NewName},
    _ = request_member_ips(UpdState),
    {next_state, waiting_for_cluster_members, UpdState, ?CONNECTION_SETUP_TIMEOUT};
waiting_for_cluster_name({cluster_name, NewName}, State=#state{name=Name}) ->
    UpdState = State#state{name=NewName, previous_name=Name},
    _ = request_member_ips(UpdState),
    {next_state, waiting_for_cluster_members, UpdState, ?CONNECTION_SETUP_TIMEOUT};
waiting_for_cluster_name(_, _State) ->
    {next_state, waiting_for_cluster_name, _State}.

%% Sync message handling for the `waiting_for_cluster_name' state
waiting_for_cluster_name(status, _From, State) ->
    {reply, waiting_for_cluster_name, waiting_for_cluster_name, State};
waiting_for_cluster_name(_, _From, _State) ->
    {reply, ok, waiting_for_cluster_name, _State}.

%% Async message handling for the `waiting_for_cluster_members' state
waiting_for_cluster_members({cluster_members, Members}, State) ->
    #state{address=Addr,
           name=Name,
           previous_name=PreviousName,
           remote=Remote} = State,
    %% This is the first time we're updating the cluster manager
    %% with the name of this cluster, so it's old name is undefined.
    ClusterUpdatedMsg = {cluster_updated,
                         PreviousName,
                         Name,
                         Members,
                         Addr,
                         Remote},
    gen_server:cast(?CLUSTER_MANAGER_SERVER, ClusterUpdatedMsg),
    {next_state, connected, State#state{members=Members}};
waiting_for_cluster_members(_, _State) ->
    {next_state, waiting_for_cluster_members, _State}.

%% Sync message handling for the `waiting_for_cluster_members' state
waiting_for_cluster_members(status, _From, State) ->
    {reply, waiting_for_cluster_members, waiting_for_cluster_members, State};
waiting_for_cluster_members(_, _From, _State) ->
    {reply, ok, waiting_for_cluster_members, _State}.

%% Async message handling for the `connected' state
connected(poll_cluster, State) ->
    _ = request_cluster_name(State),
    {next_state, waiting_for_cluster_name, State};
connected(_, State) ->
    {next_state, connected, State}.

%% Sync message handling for the `connected' state
connected(status, _From, State) ->
    #state{address=Addr,
           name=Name,
           members=Members,
           transport=Transport} = State,
    %% request for our connection status
    %% don't try talking to the remote cluster; we don't want to stall our status
    Status = {Addr, Transport, Name, Members},
    {reply, {self(), status, Status}, connected, State};
connected(_, _From, _State) ->
    {reply, ok, connected, _State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(current_state, _From, StateName, State) ->
    Reply = {StateName, State},
    {reply, Reply, StateName, State};
handle_sync_event(force_stop, _From, _StateName, State) ->
    {stop, normal, ok, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc Handle any non-fsm messages
handle_info({tcp, Socket, {cluster_members_changed, BinMembers}},
            StateName,
            State=#state{address=Addr,
                         name=Name,
                         remote=Remote,
                         socket=Socket}) ->
    Members = binary_to_term(BinMembers),
    ClusterUpdMsg = {cluster_updated, Name, Name, Members, Addr, Remote},
    gen_server:cast(?CLUSTER_MANAGER_SERVER, ClusterUpdMsg),
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State#state{members=Members}};
handle_info({tcp, Socket, Name},
            waiting_for_cluster_name,
            State=#state{socket=Socket}) ->
    gen_fsm:send_event(self(), {cluster_name, binary_to_term(Name)}),
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, waiting_for_cluster_name, State};
handle_info({tcp, Socket, Members},
            waiting_for_cluster_members,
            State=#state{socket=Socket}) ->
    gen_fsm:send_event(self(), {cluster_members, binary_to_term(Members)}),
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, waiting_for_cluster_members, State};
handle_info({tcp, Socket, Other},
            StateName,
            State=#state{remote=Remote,
                         socket=Socket}) ->
    _ = lager:error("cluster_conn: client got unexpected "
                    "msg from remote: ~p, ~p",
                    [Remote, Other]),
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State};
handle_info({tcp_error, Socket, Error},
            waiting_for_cluster_members,
            State=#state{remote=Remote,
                         socket=Socket}) ->
    _ = lager:error("cluster_conn: failed to recv "
                    "members from remote cluster at ~p because ~p",
                    [Remote, Error]),
    {stop, Error, State};
handle_info({tcp_error, Socket, Error},
            waiting_for_cluster_name,
            State=#state{remote=Remote,
                         socket=Socket}) ->
    _ = lager:error("cluster_conn: failed to recv name from "
                    "remote cluster at ~p because ~p",
                    [Remote, Error]),
    {stop, Error, State};
handle_info({tcp_error, Socket, Err},
            _StateName,
            State=#state{socket=Socket}) ->
    {stop, Err, State};
handle_info({tcp_closed, Socket},
            _StateName,
            State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp, _, _}, StateName, State=#state{socket=Socket}) ->
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State};
handle_info({tcp_error, _, _Err}, StateName, State=#state{socket=Socket}) ->
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State};
handle_info({tcp_closed, _}, StateName, State=#state{socket=Socket}) ->
    _ = inet:setopts(Socket, [{active, once}]),
    {next_state, StateName, State};
handle_info(_, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%% Internal functions
%%%===================================================================

-spec request_cluster_name(state()) -> ok | {error, term()}.
request_cluster_name(#state{mode=test}) ->
    ok;
request_cluster_name(#state{socket=Socket, transport=Transport}) ->
    _ = inet:setopts(Socket, [{active, once}]),
    Transport:send(Socket, ?CTRL_ASK_NAME).

-spec request_member_ips(state()) -> ok | {error, term()}.
request_member_ips(#state{mode=test}) ->
    ok;
request_member_ips(#state{socket=Socket, transport=Transport}) ->
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    %% get the IP we think we've connected to
    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
    %% make it a string
    PeerIPStr = inet_parse:ntoa(PeerIP),
    Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort})).

initiate_connection(State=#state{mode=test}) ->
    State;
initiate_connection(State=#state{remote=Remote}) ->
    %% Dialyzer complains about this call because the spec for
    %% `riak_core_connection_mgr::connect/4' is incorrect.
    {ok, Ref} = riak_core_connection_mgr:connect(
                  Remote,
                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,0}]},
                   {?CTRL_OPTIONS, ?MODULE, {Remote, self()}}},
                  default),
    State#state{connection_ref=Ref}.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state(pid()) -> {atom(), #state{}} | {error, term()}.
current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state).

-endif.
