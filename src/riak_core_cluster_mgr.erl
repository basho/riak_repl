%% Riak Core Cluster Manager
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% A cluster manager runs on every node. It registers a service via the riak_core_service_mgr
%% with protocol 'cluster_mgr'. The service will either answer queries (if it's the leader),
%% or foward them to the leader (if it's not the leader).
%%
%% Every cluster manager instance (one per node in the cluster) is told when it's the leader
%% by a call to set_is_leader(boolean()). An outside agent is responsible for determining
%% which instance of cluster manager is the leader. For example, the riak_repl_leader server
%% is probably a good place to do this from.
%%
%% If I'm the leader, I answer local RPC requests from non-leader cluster managers.
%% I also establish out-bound connections to any IP address added via add_remote_cluster(ip_addr()),
%% in order to resolve the name of the remote cluster and to collect any additional member addresses
%% of that cluster. I keep a database of members per named cluster.
%%
%% If I am not the leader, I proxy all requests to the actual leader because I probably don't
%% have the latest inforamtion. I don't make outbound connections either.
%%
%% The local cluster's members list is supplied by the members_fun in register_members_fun()
%% API call. The cluster manager will call the registered function to get a list of the local
%% cluster members; that function should return a list of {IP,Port} tuples in order of the least
%% "busy" to most "busy". Busy is probably proportional to the number of connections it has for
%% replication or handoff. The cluster manager will then hand out the full list to remote cluster
%% managers when asked for its members, except that each time it hands our the list, it will
%% rotate the list so that the fist "least busy" is moved to the end, and all others are pushed
%% up the front of the list. This helps balance the load when the local connection manager
%% asks the cluster manager for a list of IPs to connect for a single connection request. Thus,
%% successive calls from the connection manager will appear to round-robin through the last
%% known list of IPs from the remote cluster. The remote clusters are occasionaly polled to
%% get a fresh list, which will also help balance the connection load on them.
%%
%% TODO:
%% 1. should the service side do push notifications to the client when nodes are added/deleted?
%% 2. implement RPC calls from non-leaders to leaders in the local cluster.
%% 3. add supervision of outbound connections? How do we know if one closes and how do we reconnect?


-module(riak_core_cluster_mgr).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(TRACE(Stmt),ok).
-endif.

-define(SERVER, ?CLUSTER_MANAGER_SERVER).
-define(CLUSTER_NAME_LOCATOR_TYPE, cluster_by_name).
-define(CLUSTER_ADDR_LOCATOR_TYPE, cluster_by_addr).
-define(CLUSTER_PROTO_ID, cluster_mgr).
-define(MAX_CONS, 20).
-define(CLUSTER_POLLING_INTERVAL, 10 * 1000).
-define(CTRL_OPTIONS, [binary,
                       {keepalive, true},
                       {nodelay, true},
                       {packet, 4},
                       {reuseaddr, true},
                       {active, false}]).

-define(TEST_ADDRS, [{"127.0.0.1",5001}, {"127.0.0.1",5002}, {"127.0.0.1",5003}]).

%%                                                        Reference, Socket, Transport
-type(conn_status() :: down
                     | {pending, reference()}
                     | {connected, {inet:socket(), module()}}).

%% State of a resolved remote cluster
-record(cluster, {name :: string(),     % obtained from the remote cluster by ask_name()
                  members :: [ip_addr()], % list of suspected ip addresses for cluster
                  conn = down :: conn_status(), % remote connection
                  last_conn :: erlang:now() % last time we connected to the remote cluster
                 }).

%% unresolved cluster ip addresses
-record(uip, {addr :: ip_addr(),     % ip address of remote machine
              conn = down :: conn_status() % remote connection
             }).

%% remotes := orddict, key = ip_addr(), value = unresolved | clustername()

-record(state, {is_leader = false :: boolean(),                % true when the buck stops here
                my_name = "" :: string(),                      % my local cluster name
                member_fun = fun() -> [] end,                  % return members of my cluster
                clusters = orddict:new() :: orddict:orddict(), % resolved clusters by name
                unresolved = [] :: [#uip{}]        % ip addresses that we haven't resolved yet
               }).

-export([start_link/0,
         set_my_name/1,
         get_my_name/0,
         set_is_leader/1,
         get_is_leader/0,
         register_member_fun/1,
         add_remote_cluster/1,
         get_known_clusters/0,
         get_ipaddrs_of_cluster/1,
         get_unresolved_clusters/0
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
-spec(set_my_name(clustername()) -> ok).
set_my_name(MyName) ->
    gen_server:cast(?SERVER, {set_my_name, MyName}).

%% Return my cluster name
-spec(get_my_name() -> clustername()).
get_my_name() ->
    gen_server:call(?SERVER, get_my_name).

set_is_leader(true) ->
    gen_server:cast(?SERVER, resume);
set_is_leader(false) ->
    gen_server:cast(?SERVER, pause).

get_is_leader() ->
    gen_server:call(?SERVER, get_is_leader).

register_member_fun(MemberFun) ->
    gen_server:cast(?SERVER, {register_member_fun, MemberFun}).

%% Specify how to reach a remote cluster, it's name is
%% retrieved by asking it via the control channel.
-spec(add_remote_cluster(ip_addr()) -> ok).
add_remote_cluster({IP,Port}) ->
    gen_server:cast(?SERVER, {add_remote_cluster, {IP,Port}}).

%% Retrieve a list of known remote clusters that have been resolved (they responded).
-spec(get_known_clusters() -> [clustername()]).
get_known_clusters() ->
    gen_server:call(?SERVER, get_known_clusters).

%% Return a list of the known IP addresses of all nodes in the remote cluster.
-spec(get_ipaddrs_of_cluster(clustername()) -> [ip_addr()]).
get_ipaddrs_of_cluster(ClusterName) ->
        gen_server:call(?SERVER, {get_known_ipaddrs_of_cluster, {name,ClusterName}}).

get_unresolved_clusters() ->
        gen_server:call(?SERVER, get_unresolved_clusters).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% start our cluster_mgr service
    ServiceProto = {?CLUSTER_PROTO_ID, [{1,0}]},
    ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, ?MODULE, ctrlService, []}},
    riak_core_service_mgr:register_service(ServiceSpec, {round_robin,?MAX_CONS}),
    %% schedule a timer to poll remote clusters occasionaly
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {ok, #state{is_leader = false}}.

handle_call(get_is_leader, _From, State) ->
    {reply, State#state.is_leader, State};

handle_call(get_my_name, _From, State) ->
    {reply, State#state.my_name, State};

handle_call(get_my_members, _From, State) ->
    %% This doesn't need to RPC to the leader.
    MemberFun = State#state.member_fun,
    MyMembers = MemberFun(),
    {reply, MyMembers, State};

handle_call(get_known_clusters, _From, State) ->
    Remotes = [{Name,C#cluster.members} || {Name,C} <- orddict:to_list(State#state.clusters)],
    {reply, Remotes, State};

handle_call(get_unresolved_clusters, _From, State) ->
    Unresolved = [Uip#uip.addr || Uip <- State#state.unresolved],
    {reply, Unresolved, State};

%% Return possible IP addrs of nodes on the named remote cluster.
handle_call({get_known_ipaddrs_of_cluster, {name, ClusterName}}, _From, State) ->
    %% TODO: Should we get a fresh list from the remote cluster?
    Addrs = case orddict:find(ClusterName, State#state.clusters) of
                error -> [];
                {ok,C} -> C#cluster.members
            end,
    {reply, Addrs, State}.

handle_cast(resume, State) ->
    NewState = resume_services(State),
    {noreply, NewState};

handle_cast(pause, State) ->
    NewState = pause_services(State),
    {noreply, NewState};

handle_cast({set_my_name, MyName}, State) ->
    NewState = State#state{my_name = MyName},
    {noreply, NewState};

handle_cast({register_member_fun, Fun}, State) ->
    {noreply, State#state{member_fun=Fun}};

handle_cast({add_remote_cluster, {_IP,_Port} = Addr}, State) ->
    State2 = case lists:keymember(Addr, #uip.addr, State#state.unresolved) of
                 false ->
                     %% fresh never-before-seen IP. Add it to our list of unresolved
                     %% addresses and kick off connections to any unresolved. Really,
                     %% should be just this one. If the IP is already in a cluster,
                     %% that will be detected when we talk to it's manager :-)
                     Uips = [#uip{addr=Addr} | State#state.unresolved],
                     State3 = State#state{unresolved=Uips},
                     case State#state.is_leader of
                         true ->
                             connect_to_all_unresolved_ips(State3);
                         false ->
                             %% we'll make connections when we become leader
                             State3
                     end;
                 true ->
                     %% already on the list. nothing to do.
                     State
             end,
    {noreply, State2};

handle_cast({connected_to_remote, NameResp, Members, Socket, Transport, Addr, Args}, State) ->
    case NameResp of
        {ok, Name} ->
            %% we have an updated cluster name. We can replace our previous entry if there is one.
            Clusters = orddict:store(Name,
                                     #cluster{name = Name,
                                              members = Members,
                                              conn = {connected, Socket, Transport},
                                              last_conn = erlang:now()},
                                     State#state.clusters),
            %% we can aggressively remove Addr from the unresolved list, even if not there.
            Uips = case lists:keytake(Addr, #uip.addr, State#state.unresolved) of
                       false ->
                           State#state.unresolved;
                       {_Addr, _Uip, RestUips} ->
                           RestUips
                   end,
            %% The only remaining business is to check if the Addr we spoke to has
            %% been moved to a new cluster. If so, we want to remove the IP from the
            %% old cluster's members.
            Clusters2 = case Args of
                            {cluster, ClusterName} when ClusterName =/= Name ->
                                %% hey man, you changed cluster names.
                                %% Remove from old cluster
                                remove_member_from_cluster(Addr, ClusterName, Clusters);
                            _Other ->
                                %% nothing to do
                                Clusters
                        end,
            {reply, State#state{unresolved = Uips, clusters = Clusters2}};
        {error, Error} ->
            lager:error("Failed to receive name from remote cluster at ~p because ~p",
                        [Addr, Error]),
            {reply, State} 
    end;

handle_cast(Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [Unhandled])),
    {error, unhandled}. %% this will crash the server

%% it is time to poll all clusters and get updated member lists
handle_info(poll_clusters_timer, State = #state{clusters = Clusters}) ->
    Clusters2 = poll_clusters(Clusters),
    {noreply, State#state{clusters=Clusters2}};

handle_info(Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [Unhandled])),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

poll_cluster(Cluster = #cluster{conn = Connection, name = Name}) ->
                    case Connection of
                        {connected, Socket, Transport} ->
                            case ask_member_ips(Socket, Transport) of
                                {ok, Members} ->
                                    Cluster#cluster{members = Members};
                                {error, Reason} ->
                                    %% TODO: report? Reconnect?
                                    lager:error("Failed to read members from cluster: ~p because ~p",
                                                [Name, Reason])
                            end;
                        {pending, _Ref} ->
                            %% waiting for a connection to establish
                            Cluster;
                        _NotConnected ->
                            %% TODO: start a new connection
                            lager:error("Failed to poll cluster: ~p, because it's not connected",
                                        [Name]),
                            Cluster
                    end.

poll_clusters(Clusters) ->
    orddict:map(fun(_Name,C) ->
                        poll_cluster(C)
                end,
                Clusters).

remove_member_from_cluster(Addr, ClusterName, Clusters) ->
    case orddict:find(ClusterName, Clusters) of
        error ->
            %% no cluster by that name!
            Clusters;
        {ok, C} ->
            Members = lists:delete(Addr, C#cluster.members),
            orddict:store(ClusterName, C#cluster{members=Members}, Clusters)
    end.

register_cluster_name_locator() ->
    Locator = fun(ClusterName, _Policy) ->
                      get_ipaddrs_of_cluster(ClusterName)
              end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_NAME_LOCATOR_TYPE, Locator).

%% For all resolved clusters, establish a control channel connection
%% and refresh our list of ip addresses for the remote cluster.
connect_to_all_known_clusters(State) ->
    register_cluster_name_locator(),
    Clusters = orddict:map(
                 fun(ClusterName,C) ->
                         Args = {name, ClusterName}, % client is talking to a known cluster
                         Target = {?CLUSTER_NAME_LOCATOR_TYPE, ClusterName},
                         {ok,Ref} = riak_core_connection_mgr:connect(Target,
                                                                     {{?CLUSTER_PROTO_ID, [{1,0}]},
                                                                      {?CTRL_OPTIONS, ?MODULE, Args}},
                                                                     default),
                         C#cluster{conn={pending,Ref}}
                 end,
                 State#state.clusters),
    State#state{clusters=Clusters}.

%% for each un-resolved ip address, establish a control channel connection and
%% attempt to resolve it's name and find it's other IP addresses.
connect_to_all_unresolved_ips(State) ->
    % Identity locator function just returns a list with single member Addr
    Locator = fun(Addr, _Policy) -> {ok, [Addr]} end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_ADDR_LOCATOR_TYPE, Locator),
    % start a connection for each unresolved ip address 
    Uips = lists:map(
             fun(Uip = #uip{addr = Addr, conn = Conn}) ->
                     case Conn of
                         down ->
                             Args = {addr, Addr}, % client is talking to unresolved cluster
                             Target = {?CLUSTER_ADDR_LOCATOR_TYPE, Addr},
                             {ok,Ref} = riak_core_connection_mgr:connect(Target,
                                                                         {{?CLUSTER_PROTO_ID, [{1,0}]},
                                                                          {?CTRL_OPTIONS, ?MODULE, Args}},
                                                                         default),
                             Uip#uip{conn={pending, Ref}};
                         _Other ->
                             %% already connected or connecting. don't start another
                             Uip
                     end
             end,
             State#state.unresolved),
    State#state{unresolved=Uips}.

%% resume cluster manager leader role
resume_services(State) ->
    %% Open a connection to each named cluster, using any of it's known ip addresses
    State2 = connect_to_all_known_clusters(State),
    %% Open a connection to every un-resolved ip address to resolve it's cluster name
    State3 = connect_to_all_unresolved_ips(State2),
    State3#state{is_leader=true}.

%% stop being a cluster manager leader
pause_services(State) when State#state.is_leader == false ->
    %% still not the leader
    State;
pause_services(State) ->
    %% stop leading
    %% close any outbound connections to remote clusters and mark as down.
    %% keep the cluster info around in case we started up again and don't
    %% get any new information right away. stale info is probably better
    %% than no info! At least when our job is know stuff!
    Clusters = orddict:map(fun(_Name,C) ->
                                   case C#cluster.conn of
                                       {connected, Socket, Transport} ->
                                           Transport:close(Socket),
                                           C#cluster{conn=down};
                                       _Other ->
                                           C
                                   end
                           end,
                           State#state.clusters),
    State#state{is_leader = false,
                clusters = Clusters}.

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
        {ok, ?CTRL_ASK_NAME} ->
            %% remote wants my name
            MyName = gen_server:call(?SERVER, get_my_name),
            Transport:send(Socket, term_to_binary(MyName)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {ok, ?CTRL_ASK_MEMBERS} ->
            %% remote wants list of member machines in my cluster
            Members = gen_server:call(?SERVER, get_my_members),
            Transport:send(Socket, term_to_binary(Members)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args);
        {error, Reason} ->
            lager:error("Failed recv on control channel. Error = ~p", [Reason]),
            % nothing to do now but die
            {error, Reason};
        Other ->
            lager:error("Recv'd unknown message on cluster control channel: ~p",
                        [Other]),
            % ignore and keep trying to be a nice service
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, Args)
    end.

%%------------------------
%% control channel clients
%%------------------------

ask_cluster_name(Socket, Transport) ->
    Transport:send(Socket, ?CTRL_ASK_NAME),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinName} ->
            {ok, binary_to_term(BinName)};
        {error, Reason} ->
            {error, Reason}
    end.

ask_member_ips(Socket, Transport) ->
    Transport:send(Socket, ?CTRL_ASK_MEMBERS),
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinMembers} ->
            {ok, binary_to_term(BinMembers)};
        {error, Reason} ->
            {error, Reason}
    end.

connected(Socket, Transport, Addr, {?CLUSTER_PROTO_ID, _MyVer, _RemoteVer}, Args) ->
    %% when first connecting to a remote node, we ask it's and member list, even if
    %% it's a previously resolved cluster. Then we can sort everything out in the
    %% gen_server.
    Name = ask_cluster_name(Socket, Transport),
    Members = ask_cluster_name(Socket, Transport),
    ?TRACE(?debugFmt("Connected to remote cluster ~p at ~p with members: ~p",
                     [Name, Addr, Members])),
    gen_server:cast(?SERVER,
                    {connected_to_remote, Name, Members, Socket, Transport, Addr, Args}).

connect_failed({_Proto,_Vers}, {error, _Reason}, _Args) ->
    %% It's ok, the connection manager will keep trying.
    %% TODO: mark this addr/cluster as having connection issues.
    ok.
