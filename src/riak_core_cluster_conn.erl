%% Riak Core Cluster Manager Connections to Remote Clusters
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% Connections get started by who-ever and they report to the cluster manager.
%% Once an ip-address has been resolved to a cluster, the cluster manager
%% might remove the connection if it already has a connection to that cluster.

-module(riak_core_cluster_conn).

-behavior(gen_fsm).

-include("riak_core_cluster.hrl").
-include_lib("riak_core/include/riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%% For testing, we need to have two different cluster manager services running
%% on the same node, which is normally not done. The remote cluster service is
%% the one we're testing, so use a different protocol for the client connection
%% during eunit testing, which will emulate a cluster manager from the test.
-define(REMOTE_CLUSTER_PROTO_ID, test_cluster_mgr).
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(REMOTE_CLUSTER_PROTO_ID, ?CLUSTER_PROTO_ID).
-define(TRACE(Stmt),ok).
-endif.

% api
-export([start_link/1]).
-export([status/1, poll_remote/1]).

% gen_fsm
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
    terminate/3, code_change/4]).
% fsm stats
-export([
    connecting/2,
    ask_cluster_name/2,
    ask_member_ips/2,
    waiting/2]).

% connection mgr callbacks
-export([connected/6, connect_failed/3]).

% old stuff
%-export([connected/6, connect_failed/3, ctrlClientProcess/3]).

-record(state, {
    remote, members = [],
    name = "undefined",
    old_name = "undefined", socket, transport, addr, timeout
    }).

%%%===================================================================
%%% API
%%%===================================================================

%% start a connection with a locator type, either {cluster_by_name, clustername()}
%% or {cluster_by_addr, ip_addr()}. This is asynchronous. If it dies, the connection
%% supervisior will restart it.
-spec(start_link(string()) -> {ok,pid()}).
start_link(Remote) ->
    ?TRACE(?debugFmt("connecting to ~p", [Remote])),
    Members = [],
    gen_fsm:start_link(?MODULE, [Remote, Members], []).
%    Pid = proc_lib:spawn_link(?MODULE,
%                              ctrlClientProcess,
%                              [Remote, unconnected, Members]),
%    {ok, Pid}.

status(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, status).

poll_remote(Pid) ->
    gen_fsm:send_event(Pid, poll_remote).

%%%===================================================================
%%% core_connection callbacks
%%%===================================================================

connected(Socket, Transport, Addr,
          {?REMOTE_CLUSTER_PROTO_ID, _MyVer, _RemoteVer},
          {_Remote,Client},
          Props) ->
    %% give control over the socket to the Client process.
    %% tell client we're connected and to whom
    Transport:controlling_process(Socket, Client),
    gen_fsm:send_event(Client, {connected_to_remote, Socket, Transport, Addr, Props}).

connect_failed({_Proto,_Vers}, {error, _Reason}=Error, {_Remote,Client}) ->
    %% tell client we bombed and why
    gen_fsm:send_event(Client, {connect_failed, Error}),
    %% increment stats for "client failed to connect"
    riak_repl_stats:client_connect_errors(),
    ok.

%%%===================================================================
%%% init
%%%===================================================================

init([Remote, Members]) ->
    Args = {Remote, self()},
    {ok, _Ref} = riak_core_connection_mgr:connect(Remote,
                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,0}]},
                   {?CTRL_OPTIONS, ?MODULE, Args}},
                  default),
    State = #state{remote = Remote, members = Members},
    {ok, connecting, State}.

%%%===================================================================
%%% states
%%%===================================================================

connecting({connected_to_remote, Socket, Transport, Addr, _Props}, State) ->
    State2 = State#state{socket = Socket, transport = Transport, addr = Addr},
    Transport:send(Socket, ?CTRL_ASK_NAME),
    Transport:setopts(Socket, [{active, once}]),
    State3 = set_response_timeout(State2),
    {next_state, ask_cluster_name, State3};

connecting({connect_failed, Error} = StopRes, State) ->
    lager:warning("ClusterManager Client: connect_failed to ~p because ~p. Will retry.", [State#state.remote, Error]),
    {stop, StopRes, State}.

ask_cluster_name({timeout, Ref, no_response}, #state{timeout = Ref} = State) ->
    {stop, {error, reponse_timeout}, State}.

ask_member_ips({timeout, Ref, no_response}, #state{timeout = Ref} = State) ->
    {stop, {error, response_timeout}, State}.

waiting({timeout, _Ref, no_response}, State) ->
    % likely a late waiting timeout.
    {next_state, waiting, State#state{timeout = undefined}};

waiting(poll_remote, State) ->
    #state{transport = Transport, socket = Socket} = State,
    Transport:send(Socket, ?CTRL_ASK_NAME),
    State2 = set_response_timeout(State),
    {next_state, ask_cluster_name, State2}.






%%%===================================================================
%%% handle_sync_event
%%%===================================================================

handle_event(_Evt, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, connecting, State) ->
    {reply, connecting, connecting, State};

handle_sync_event(status, _From, StateName, State) ->
    #state{name = Name, transport = Transport, addr = Addr, members = Members} = State,
    Status = {Addr, Transport, Name, Members},
    {reply, Status, StateName, State};

handle_sync_event(_Evt, _From, StateName, State) ->
    {reply, {error, not_supported}, StateName, State}.

%%%===================================================================
%%% handle_info
%%%===================================================================

% supporting old version
handle_info({From, status}, connectng, State) ->
    From ! {self, connecting, State#state.remote},
    {next_state, connecting, State};

% supporting old version
handle_info({From, status}, connected, State) ->
    #state{name = Name, transport = Transport, addr = Addr, members = Members} = State,
    Status = {Addr, Transport, Name, Members},
    From ! {self(), status, Status},
    {next_state, connecting, State};

handle_info({ErrTransport, Socket, Error}, _StateName, #state{socket = Socket} = State) when ErrTransport =:= ssl_error; ErrTransport =:= tcp_error ->
    {stop, {connection_error, Error}, State};

handle_info({ClosedTransport, Socket}, _StateName, #state{socket = Socket} = State) when ClosedTransport =:= tcp_closed; ClosedTransport =:= ssl_closed ->
    lager:info("Remote side closed connection, exiting normally"),
    {stop, normal, State};

handle_info({_InternalTransport, Socket, Data}, ask_cluster_name, #state{socket = Socket} = State) ->
    Transport = State#state.transport,
    Transport:setopts(Socket, [{active, once}]),
    Name = binary_to_term(Data),
    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
    PeerIPStr = inet_parse:ntoa(PeerIP),
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort})),
    State2 = set_response_timeout(State),
    State3 = State2#state{name = Name, old_name = State2#state.name},
    {next_state, ask_member_ips, State3};

handle_info({_InternalTrasport, Socket, Data}, ask_member_ips, #state{socket = Socket} = State) ->
    Transport = State#state.transport,
    Transport:setopts(Socket, [{active, once}]),
    Members = binary_to_term(Data),
    State2 = State#state{members = Members},
    #state{name = Name, old_name = OldName, addr = Addr, remote = Remote} = State2,
    gen_server:cast(?CLUSTER_MANAGER_SERVER, {cluster_updated, OldName, Name, Members, Addr, Remote}),
    {next_state, waiting, State2};

handle_info({_InternalTrasport, Socket, Data}, waiting, State) ->
    Transport = State#state.transport,
    Transport:setopts(Socket, [{active, once}]),
    case binary_to_term(Data) of
        {cluster_members_changed, NewMembers} ->
            #state{name = Name, addr = Addr, remote = Remote} = State,
            gen_server:cast(?CLUSTER_MANAGER_SERVER, {cluster_updated, Name, Name, NewMembers, Addr, Remote}),
            State2 = State#state{members = NewMembers},
            {next_state, waiting, State2};
        Other ->
            lager:info("unexpected message from remote: ~p: ~p", [State#state.remote, Other]),
            {next_state, waiting, State}
    end;

% supporting old version
handle_info({_From, poll_cluster}, waiting, State) ->
    waiting(poll_remote, State);

handle_info(Info, StateName, State) ->
    lager:debug("unrecognized message while in state ~s: ~p", [StateName, Info]),
    {next_state, StateName, State}.

%%%===================================================================
%%% Other otp callbacks
%%%===================================================================

terminate(Why, StateName, _State) ->
    lager:debug("Exiting from state ~s due to ~p", [StateName, Why]).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%% private
%%%===================================================================

set_response_timeout(#state{timeout = undefined} = State) ->
    Ref = gen_fsm:start_timer(?CONNECTION_SETUP_TIMEOUT, no_response),
    State#state{timeout = Ref};

set_response_timeout(#state{timeout = Ref} = State) ->
    gen_fsm:cancel_timer(Ref),
    set_response_timeout(State#state{timeout = undefined}).

%% request a connection from connection manager. When connected, our
%% module's "connected" function will be called, which sends a message
%% "connected_to_remote", which we forward to the cluster manager. Dying
%% is ok; we'll get restarted by a supervisor.
%ctrlClientProcess(Remote, unconnected, Members0) ->
%    Args = {Remote, self()},
%    {ok,_Ref} = riak_core_connection_mgr:connect(
%                  Remote,
%                  {{?REMOTE_CLUSTER_PROTO_ID, [{1,0}]},
%                   {?CTRL_OPTIONS, ?MODULE, Args}},
%                  default),
%    ctrlClientProcess(Remote, connecting, Members0);
%%% We're trying to connect via the connection manager now
%ctrlClientProcess(Remote, connecting, Members0) ->
%    %% wait a long-ish time for the connection to establish
%    receive
%        {From, status} ->
%            %% someone wants our status. Don't do anything that blocks!
%            From ! {self(), connecting, Remote},
%            ctrlClientProcess(Remote, connecting, Members0);
%        {_From, {connect_failed, Error}} ->
%            ?TRACE(?debugFmt("ClusterManager Client: connect_failed to ~p because ~p. Will retry.", [Remote, Error])),
%            lager:warning("ClusterManager Client: connect_failed to ~p because ~p. Will retry.",
%                          [Remote, Error]),
%            %% This is fatal! We are being supervised by conn_sup and if we
%            %% die, it will restart us.
%            {error, Error};
%        {_From, {connected_to_remote, Socket, Transport, Addr, Props}} ->
%            RemoteName = proplists:get_value(clustername, Props),
%            ?TRACE(?debugFmt("Cluster Manager control channel client connected to remote ~p at ~p named ~p", [Remote, Addr, RemoteName])),
%            lager:debug("Cluster Manager control channel client connected to remote ~p at ~p named ~p",
%                       [Remote, Addr, RemoteName]),
%            %% ask it's name and member list, even if it's a previously
%            %% resolved cluster. Then we can sort everything out in the
%            %% gen_server. If the name or members fails, these matches
%            %% will fail and the connection will get restarted.
%            case ask_cluster_name(Socket, Transport, Remote) of
%                {ok, Name} ->
%                    case ask_member_ips(Socket, Transport, Addr, Remote) of
%                        {ok, Members} ->
%                            %% This is the first time we're updating the cluster manager
%                            %% with the name of this cluster, so it's old name is undefined.
%                            OldName = "undefined",
%                            gen_server:cast(?CLUSTER_MANAGER_SERVER,
%                                            {cluster_updated, OldName, Name, Members, Addr, Remote}),
%                            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members);
%                        {error, closed} ->
%                            {error, connection_closed};
%                        Error ->
%                            Error
%                    end;
%                {error, closed} ->
%                    {error, connection_closed};
%                Error ->
%                    Error
%            end;
%        {_From, poll_cluster} ->
%            %% cluster manager doesn't know we haven't connected yet.
%            %% just ignore this while we're waiting to connect or fail
%            ctrlClientProcess(Remote, connecting, Members0);
%        Other ->
%            lager:error("cluster_conn: client got unexpected msg from remote: ~p, ~p",
%                        [Remote, Other]),
%            ctrlClientProcess(Remote, connecting, Members0)
%    after (?CONNECTION_SETUP_TIMEOUT + 5000) ->
%            %% die with error once we've passed the timeout period that the
%            %% core_connection module will expire. Go round and let the connection
%            %% manager keep trying.
%            lager:debug("cluster_conn: client timed out waiting for ~p", [Remote]),
%            ctrlClientProcess(Remote, connecting, Members0)
%    end;
%ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members0) ->
%    %% trade our time between checking for updates from the remote cluster
%    %% and commands from our local cluster manager. TODO: what if the name
%    %% of the remote cluster changes?
%    receive
%        %% cluster manager asking us to poll the remove cluster
%        {_From, poll_cluster} ->
%            case ask_cluster_name(Socket, Transport, Remote) of
%                {ok, NewName} ->
%                    case ask_member_ips(Socket, Transport, Addr, Remote) of
%                        {ok, Members} ->
%                            gen_server:cast(?CLUSTER_MANAGER_SERVER,
%                                            {cluster_updated, Name, NewName, Members, Addr, Remote}),
%                            ctrlClientProcess(Remote, {NewName, Socket, Transport, Addr}, Members);
%                        {error, closed} ->
%                            {error, connection_closed};
%                        Error ->
%                            Error
%                    end;
%                {error, closed} ->
%                    {error, connection_closed};
%                Error ->
%                    Error
%            end;
%        %% request for our connection status
%        {From, status} ->
%            %% don't try talking to the remote cluster; we don't want to stall our status
%            Status = {Addr, Transport, Name, Members0},
%            From ! {self(), status, Status},
%            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members0)
%    after 1000 ->
%            %% check for push notifications from remote cluster about member changes
%            Members1 =
%                case Transport:recv(Socket, 0, 250) of
%                    {ok, {cluster_members_changed, BinMembers}} ->
%                        Members = {ok, binary_to_term(BinMembers)},
%                        gen_server:cast(?CLUSTER_MANAGER_SERVER,
%                                        {cluster_updated, Name, Name, Members, Addr, Remote}),
%                        Members;
%                    {ok, Other} ->
%                        lager:error("cluster_conn: client got unexpected msg from remote: ~p, ~p",
%                                    [Remote, Other]),
%                        Members0;
%                    {error, timeout} ->
%                        %% timeouts are ok; we'll just go round and try again
%                        Members0;
%                    {error, closed} ->
%                        %%erlang:exit(connection_closed);
%                        {error, connection_closed};
%                    {error, ebadf} ->
%                        %% like a closed file descriptor
%                        {error, connection_closed};
%                    {error, Reason} ->
%                        lager:error("cluster_conn: client got error from remote: ~p, ~p",
%                                    [Remote, Reason]),
%                        {error, Reason}
%                end,
%            ctrlClientProcess(Remote, {Name, Socket, Transport, Addr}, Members1)
%    end.
%
%ask_cluster_name(Socket, Transport, Remote) ->
%    Transport:send(Socket, ?CTRL_ASK_NAME),
%    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
%        {ok, BinName} ->
%            {ok, binary_to_term(BinName)};
%        {error, closed} ->
%            %% the other side hung up. Stop quietly.
%            {error, closed};
%        Error ->
%            lager:error("cluster_conn: failed to recv name from remote cluster at ~p because ~p",
%                        [Remote, Error]),
%            Error
%    end.
%
%ask_member_ips(Socket, Transport, _Addr, Remote) ->
%    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
%    %% get the IP we think we've connected to
%    {ok, {PeerIP, PeerPort}} = Transport:peername(Socket),
%    %% make it a string
%    PeerIPStr = inet_parse:ntoa(PeerIP),
%    Transport:send(Socket, term_to_binary({PeerIPStr, PeerPort})),
%    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
%        {ok, BinMembers} ->
%            {ok, binary_to_term(BinMembers)};
%        {error, closed} ->
%            %% the other side hung up. Stop quietly.
%            {error, closed};
%        Error ->
%            lager:error("cluster_conn: failed to recv members from remote cluster at ~p because ~p",
%                        [Remote, Error]),
%            Error
%    end.

