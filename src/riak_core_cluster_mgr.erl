%% Riak Replication Subprotocol Server Dispatch and Client Connections
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_cluster_mgr).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?CLUSTER_MANAGER_SERVER).
-define(TEST_ADDR, {"127.0.0.1",4097}).
-define(CTRL_PROTO_ID, cluster_mgr).
-define(CTRL_OPTIONS, [binary,
                       {keepalive, true},
                       {nodelay, true},
                       {packet, 4},
                       {reuseaddr, true},
                       {active, false}]).

%% remotes := orddict, key = ip_addr(), value = unresolved | clustername()

-record(state, {is_paused = false :: boolean(),
                my_name = "" :: string(),
                remotes = orddict:new() :: orddict:orddict()
               }).

-export([start_link/0,
         set_name/1,
         get_name/0,
         add_remote_cluster/1,
         remote_clusters/0,
         resume/0,
         pause/0,
         is_paused/0,
         get_ipaddrs_for_proto/3
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([ctrlService/4, ctrlServiceProcess/5, connected/5, connect_failed/3]).

%%%===================================================================
%%% API
%%%===================================================================

%% start the Cluster Manager
-spec(start_link() -> ok).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% Set my cluster name
-spec(set_name(clustername()) -> ok).
set_name(MyName) ->
    gen_server:cast(?SERVER, {set_name, MyName}).

%% Return my cluster name
-spec(get_name() -> clustername()).
get_name() ->
    gen_server:call(?SERVER, get_name).

%% Specify how to reach a remote cluster, it's name is
%% retrieved by asking it via the control channel.
-spec(add_remote_cluster(ip_addr()) -> ok).
add_remote_cluster({IP,Port}) ->
    gen_server:cast(?SERVER, {add_remote_cluster, {IP,Port}}).

%% retrieve a list of known remote clusters.
-spec(remote_clusters() -> {ok,[{(unresolved|clustername()), ip_addr()}]} | {error, term()}).
remote_clusters() ->
    gen_server:call(?SERVER, remote_clusters).

%% Start and stop the Cluster Manager
-spec(resume() -> ok).
resume() ->
    gen_server:cast(?SERVER, resume).

-spec(pause() -> ok).
pause() ->
    gen_server:cast(?SERVER, resume).

%% return paused state
is_paused() ->
    gen_server:call(?SERVER, is_paused).

-spec(get_ipaddrs_for_proto(clustername(), proto_id(), client_scheduler_strategy()) ->
             {ok,[ip_addr()]} | {error, term()}).
get_ipaddrs_for_proto(RemoteCluster, ProtocolId, Strategy) ->
%%    ?debugFmt("TODO: get_ipaddrs_for_proto(~p,~p,~p)", [RemoteCluster, ProtocolId, Strategy]),
    gen_server:cast(?SERVER, {get_ipaddrs_for_proto, RemoteCluster, ProtocolId, Strategy}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    CtrlProtocol = {{?CTRL_PROTO_ID, [{1,0}]}, {?CTRL_OPTIONS, ?MODULE, testService, []}},
    MaxConnections = 30, %% that's a lot of data centers
    riak_core_conn_mgr:register_service(CtrlProtocol, {round_robin,MaxConnections}),
    {ok, #state{is_paused = true}}.

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

handle_call(get_name, _From, State) ->
    {reply, State#state.my_name, State};

handle_call(remote_clusters, _From, State) ->
    Remotes = [{Name,Addr} || {Addr,Name} <- orddict:to_list(State#state.remotes)],
    {reply, Remotes, State};

handle_call({connected_to_remote, unresolved, Socket, Transport, Addr, Args}, _From, State) ->
    %% too bad, we got connected but the other cluster couldn't answer it's get_name
    {reply, ok, State};

handle_call({connected_to_remote, Name, Socket, Transport, Addr, Args}, _From, State) ->
    {reply, ok, State};

handle_call({get_addrs_for_proto_id, RemoteClusterName,ProtocolId}, _From, State) ->
    Addrs = [?TEST_ADDR],
    {reply, Addrs, State}.

handle_cast(pause, State) ->
    NewState = pause_services(State),
    {noreply, NewState};

handle_cast({set_name, MyName}, State) ->
    NewState = State#state{my_name = MyName},
    {noreply, NewState};

handle_cast({add_remote_cluster, {IP,Port}}, State) ->
    NewDict = orddict:store({IP,Port}, unresolved, State#state.remotes),
    {noreply, State#state{remotes=NewDict}};

handle_cast({resolved_cluster, {IP,Port}, RemoteName}, State) ->
    NewDict = orddict:store({IP,Port}, RemoteName, State#state.remotes),
    {noreply, State#state{remotes=NewDict}};

handle_cast(resume, State) ->
    NewState = resume_services(State),
    {noreply, NewState};

handle_cast(Unhandled, _State) ->
%%    ?debugFmt("Unhandled gen_server cast: ~p", [Unhandled]),
    {error, unhandled}. %% this will crash the server

handle_info(Unhandled, State) ->
%%    ?debugFmt("Unhandled gen_server info: ~p", [Unhandled]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% TODO: pause/resume stuff as needed
resume_services(State) ->
    %% TODO: start resolver
    State#state{is_paused=false}.

pause_services(State) when State#state.is_paused == true ->
    State;
pause_services(State) ->
    %% TODO: kill/pause resolver
    State#state{is_paused=true}.

%%-------------------------
%% control channel services
%%-------------------------

ctrlService(_Socket, _Transport, {error, Reason}, _Args) ->
    lager:error("Failed to accept control channel connection: ~p", [Reason]);
ctrlService(Socket, Transport, {ok, {cluster_mgr, MyVer, RemoteVer}}, Args) ->
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlProcess,
                              [Socket, Transport, MyVer, RemoteVer, Args]),
    {ok, Pid}.

%% process instance for handling control channel requests from remote clusters.
ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, ?CTRL_GET_NAME} ->
            %% remote wants my name
            MyName = gen_server:call(?SERVER, get_name),
            Transport:send(Socket, term_to_binary(MyName)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {error, Reason} ->
            lager:error("Failed recv on control channel. Error = ~p", [Reason]),
            {error, Reason}
    end.

%%------------------------
%% control channel clients
%%------------------------

connected(Socket, Transport, Addr, {cluster_mgr, _MyVer, _RemoteVer}, Args) ->
    %% ask it's name
    Transport:send(Socket, ?CTRL_GET_NAME),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinName} ->
            Name = binary_to_term(BinName),
%%            ?debugFmt("Connected to remote cluster ~p at ~p", [Name, Addr]),
            gen_server:call(?SERVER, {connected_to_remote, Name, Socket, Transport, Addr, Args});
        {error, Reason} ->
            lager:error("Failed recv on control channel. Error = ~p", [Reason]),
            gen_server:call(?SERVER, {connected_to_remote, unresolved, Socket, Transport, Addr, Args})
        end.

connect_failed({_Proto,_Vers}, {error, _Reason}, _Args) ->
%%    ?assert(false).
    ok.
