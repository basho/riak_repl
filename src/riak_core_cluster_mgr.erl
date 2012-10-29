%% Riak Core Cluster Manager
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% A cluster manager runs on every node. It registers a service via the riak_core_service_mgr
%% with protocol 'cluster_mgr'. The service will either answer queries (if it's the leader),
%% or foward them to the leader (if it's not the leader).
%%
%% Every cluster manager instance (one per node in the cluster) is told who the leader is
%% when there is a leader change. An outside agent is responsible for determining
%% which instance of cluster manager is the leader. For example, the riak_repl2_leader server
%% is probably a good place to do this from. Call set_leader_node(node(), pid()).
%%
%% If I'm the leader, I answer local gen_server:call requests from non-leader cluster managers.
%% I also establish out-bound connections to any IP address added via add_remote_cluster(ip_addr()),
%% in order to resolve the name of the remote cluster and to collect any additional member addresses
%% of that cluster. I keep a database of members per named cluster.
%%
%% If I am not the leader, I proxy all requests to the actual leader because I probably don't
%% have the latest inforamtion. I don't make outbound connections either.
%%
%% The local cluster's members list is supplied by the members_fun in register_member_fun()
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


-module(riak_core_cluster_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TRACE(Stmt),Stmt).
%%-define(TRACE(Stmt),ok).
-else.
-define(TRACE(Stmt),ok).
-endif.

-define(SERVER, ?CLUSTER_MANAGER_SERVER).
-define(MAX_CONS, 20).
-define(CLUSTER_POLLING_INTERVAL, 10 * 1000).
-define(GC_INTERVAL, 60 * 1000).
-define(PROXY_CALL_TIMEOUT, 30 * 1000).

%% State of a resolved remote cluster
-record(cluster, {name :: string(),     % obtained from the remote cluster by ask_name()
                  members :: [ip_addr()], % list of suspected ip addresses for cluster
                  last_conn :: erlang:now() % last time we connected to the remote cluster
                 }).

%% remotes := orddict, key = ip_addr(), value = unresolved | clustername()

-record(state, {is_leader = false :: boolean(),                % true when the buck stops here
                leader_node = undefined :: undefined | node(),
                member_fun = fun(_Addr) -> [] end,             % return members of local cluster
                restore_targets_fun = fun() -> [] end,         % returns persisted cluster targets
                save_members_fun = fun(_C,_M) -> ok end,       % persists remote cluster members
                balancer_fun = fun(Addrs) -> Addrs end,        % registered balancer function
                clusters = orddict:new() :: orddict:orddict()  % resolved clusters by name
               }).

-export([start_link/0,
         set_leader/2,
         get_leader/0,
         get_is_leader/0,
         register_cluster_locator/0,
         register_member_fun/1,
         register_restore_cluster_targets_fun/1,
         register_save_cluster_members_fun/1,
         add_remote_cluster/1, remove_remote_cluster/1,
         get_known_clusters/0,
         get_connections/0,
         get_ipaddrs_of_cluster/1,
         stop/0
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([ctrlService/5, ctrlServiceProcess/5, round_robin_balancer/1, cluster_mgr_sites_fun/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% start the Cluster Manager
-spec(start_link() -> ok).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% register a bootstrap cluster locator that just uses the address passed to it
%% for simple ip addresses and one that looks up clusters by name. This is needed
%% before the cluster manager is up and running so that the connection supervisior
%% can kick off some initial connections if some remotes are already known (in the
%% ring) from previous additions.
register_cluster_locator() ->
    register_cluster_addr_locator().

%% Called by riak_repl_leader whenever a leadership election
%% takes place. Tells us who the leader is.
set_leader(LeaderNode, _LeaderPid) ->
    gen_server:call(?SERVER, {set_leader_node, LeaderNode}).

%% Reply with the current leader node.
get_leader() ->
    gen_server:call(?SERVER, leader_node).

get_is_leader() ->
    gen_server:call(?SERVER, get_is_leader).

%% Register a function that will get called to get out local riak node member's IP addrs.
%% MemberFun(inet:addr()) -> [{IP,Port}] were IP is a string
register_member_fun(MemberFun) ->
    gen_server:cast(?SERVER, {register_member_fun, MemberFun}).

register_restore_cluster_targets_fun(ReadClusterFun) ->
    gen_server:cast(?SERVER, {register_restore_cluster_targets_fun, ReadClusterFun}).

register_save_cluster_members_fun(WriteClusterFun) ->
    gen_server:cast(?SERVER, {register_save_cluster_members_fun, WriteClusterFun}).

%% Specify how to reach a remote cluster, it's name is
%% retrieved by asking it via the control channel.
-spec(add_remote_cluster(ip_addr()) -> ok).
add_remote_cluster({IP,Port}) ->
    gen_server:cast(?SERVER, {add_remote_cluster, {IP,Port}}).

%% Remove a remote cluster by name
-spec(remove_remote_cluster(ip_addr() | string()) -> ok).
remove_remote_cluster(Cluster) ->
    gen_server:cast(?SERVER, {remove_remote_cluster, Cluster}).

%% Retrieve a list of known remote clusters that have been resolved (they responded).
-spec(get_known_clusters() -> {ok,[clustername()]} | term()).
get_known_clusters() ->
    gen_server:call(?SERVER, get_known_clusters).

%% Retrieve a list of IP,Port tuples we are connected to or trying to connect to
get_connections() ->
    gen_server:call(?SERVER, get_connections).

%% Return a list of the known IP addresses of all nodes in the remote cluster.
-spec(get_ipaddrs_of_cluster(clustername()) -> [ip_addr()]).
get_ipaddrs_of_cluster(ClusterName) ->
        gen_server:call(?SERVER, {get_known_ipaddrs_of_cluster, {name,ClusterName}}).

stop() ->
    gen_server:call(?SERVER, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    lager:info("Cluster Manager: starting"),
    register_cluster_locator(),
    %% start our cluster_mgr service
    ServiceProto = {?CLUSTER_PROTO_ID, [{1,0}]},
    ServiceSpec = {ServiceProto, {?CTRL_OPTIONS, ?MODULE, ctrlService, []}},
    riak_core_service_mgr:register_service(ServiceSpec, {round_robin,?MAX_CONS}),
    %% schedule a timer to poll remote clusters occasionaly
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    %% schedule a timer to garbage collect old cluster and endpoint data
    erlang:send_after(?GC_INTERVAL, self(), garbage_collection_timer),
    BalancerFun = fun(Addr) -> round_robin_balancer(Addr) end,
    {ok, #state{is_leader=false,
                balancer_fun=BalancerFun
               }}.

handle_call(get_is_leader, _From, State) ->
    {reply, State#state.is_leader, State};

handle_call({get_my_members, MyAddr}, _From, State) ->
    %% This doesn't need to call the leader.
    MemberFun = State#state.member_fun,
    MyMembers = [{string_of_ip(IP),Port} || {IP,Port} <- MemberFun(MyAddr)],
    {reply, MyMembers, State};

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call({set_leader_node, LeaderNode}, _From, State) ->
    State2 = State#state{leader_node = LeaderNode},
    case node() of
        LeaderNode ->
            %% oh crap, it's me!
            lager:info("ClusterManager(repl2): ~p Becoming the leader", [LeaderNode]),
            {reply, ok, become_leader(State2)};
        _ ->
            %% not me.
            lager:info("ClusterManager(repl2): ~p Becoming a proxy to ~p",
                       [node(), LeaderNode]),
            {reply, ok, become_proxy(State2)}
    end;

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

%% Reply with list of resolved cluster names.
%% If a leader has not been elected yet, return an empty list.
handle_call(get_known_clusters, _From, State) ->
    case State#state.is_leader of
        true ->
            Remotes = [Name || {Name,_C} <- orddict:to_list(State#state.clusters)],
            {reply, {ok, Remotes}, State};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call(get_known_clusters, NoLeaderResult, State)
    end;

handle_call(get_connections, _From, State) ->
    case State#state.is_leader of
        true ->
            Conns = riak_core_cluster_conn_sup:connections(),
            {reply, {ok, Conns}, State};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call(get_connections, NoLeaderResult, State)
    end;
    

%% Return possible IP addrs of nodes on the named remote cluster.
%% If a leader has not been elected yet, return an empty list.
%% This list will get rotated or randomized depending on the balancer
%% function installed. Every time we poll the remote cluster or it
%% pushes an update, the list will get reset to whatever the remote
%% thinks is the best order. The first call here will return the most
%% recently updated list and then it will rebalance and save for next time.
%% So, if no updates come from the remote, we'll just keep cycling through
%% the list of known members according to the balancer fun.
handle_call({get_known_ipaddrs_of_cluster, {name, ClusterName}}, _From, State) ->
    case State#state.is_leader of
        true ->
            %% Call a balancer function that will rotate or randomize
            %% the list. Return original members and save reblanced ones
            %% for next iteration.
            Members = members_of_cluster(ClusterName, State),
            BalancerFun = State#state.balancer_fun,
            RebalancedMembers = BalancerFun(Members),
            {reply, {ok, Members},
             State#state{clusters=add_ips_to_cluster(ClusterName, RebalancedMembers,
                                                     State#state.clusters)}};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call({get_known_ipaddrs_of_cluster, {name, ClusterName}},
                       NoLeaderResult,
                       State)
    end.

handle_cast({register_member_fun, Fun}, State) ->
    {noreply, State#state{member_fun=Fun}};

handle_cast({register_save_cluster_members_fun, Fun}, State) ->
    {noreply, State#state{save_members_fun=Fun}};

handle_cast({register_restore_cluster_targets_fun, Fun}, State) ->
    %% If we are already the leader, connect to known clusters after some delay.
    %% TODO: 5 seconds is arbitrary. It's enough time for the ring to be stable
    %% so that the call into the repl_ring handler won't crash. Fix this.
    erlang:send_after(5000, self(), connect_to_clusters),
    {noreply, State#state{restore_targets_fun=Fun}};

handle_cast({add_remote_cluster, {_IP,_Port} = Addr}, State) ->
    case State#state.is_leader of
        false ->
            %% forward request to leader manager
            proxy_cast({add_remote_cluster, Addr}, State);
        true ->
            %% start a connection if one does not already exist
            Remote = {?CLUSTER_ADDR_LOCATOR_TYPE, Addr},
            ensure_remote_connection(Remote)
    end,
    {noreply, State};

%% remove a connection if one already exists, by name or by addr.
%% This is usefull if you accidentally add a bogus cluster address or
%% just want to disconnect from one.
handle_cast({remove_remote_cluster, Cluster}, State) ->
    State2 =
        case State#state.is_leader of
            false ->
                %% forward request to leader manager
                proxy_cast({remove_remote_cluster, Cluster}, State),
                State;
            true ->
                remove_remote(Cluster, State)
        end,
    {noreply, State2};

%% The client connection recived (or polled for) an update from the remote cluster.
%% Note that here, the Members are sorted in least connected order. Preserve that.
%% TODO: we really want to keep all nodes we have every seen, except remove nodes
%% that explicitly leave the cluster or show up in other clusters.
handle_cast({cluster_updated, Name, Members, Addr, Remote}, State) ->
    OldMembers = lists:sort(members_of_cluster(Name, State)),
    case OldMembers == lists:sort(Members) of
        true ->
            ok;
        false ->
            persist_members_to_ring(State, Name, Members),
            lager:info("Cluster Manager: updated by remote ~p at ~p named ~p with members: ~p",
                       [Remote, Addr, Name, Members])
    end,
    %% clean out IPs from other clusters in case of membership movement or
    %% cluster name changes.
    Clusters1 = remove_ips_from_all_clusters(Members, State#state.clusters),
    %% Ensure we have as few connections to a cluster as possible
    remove_connection_if_aliased(Remote, Name),
    %% save latest rebalanced members
    {noreply, State#state{clusters=add_ips_to_cluster(Name, Members, Clusters1)}};

handle_cast(_Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [_Unhandled])),
    {error, unhandled}. %% this will crash the server

%% it is time to poll all clusters and get updated member lists
handle_info(poll_clusters_timer, State) when State#state.is_leader == true ->
    Connections = riak_core_cluster_conn_sup:connections(),
    [Pid ! {self(), poll_cluster} || {_Remote, Pid} <- Connections],
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};
handle_info(poll_clusters_timer, State) ->
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};

%% Remove old clusters that no longer have any IP addresses associated with them.
%% They are probably old cluster names that no longer exist. If we don't have an IP,
%% then we can't connect to it anyhow.
handle_info(garbage_collection_timer, State0) ->
    State = collect_garbage(State0),
    erlang:send_after(?GC_INTERVAL, self(), garbage_collection_timer),
    {noreply, State};

handle_info(connect_to_clusters, State) ->
    %% Open a connection to all known (persisted) clusters
    case State#state.is_leader of
        true ->
            Fun = State#state.restore_targets_fun,
            ClusterTargets = Fun(),
            lager:info("Cluster Manager will connect to clusters: ~p", [ClusterTargets]),
            connect_to_targets(ClusterTargets);
        _ ->
            ok
    end,
    {noreply, State};

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

%% if the cluster name is not the same as the initial remote we connected to,
%% then remove this connection and ensure that we have one to the actual cluster
%% name. This should reduce the number of connections to a single one per
%% remote cluster.
remove_connection_if_aliased({cluster_by_name, CName}=Remote, Name) when CName =/= Name ->
    lager:info("Removing sink alias ~p named ~p", [CName, Name]),
    remove_remote_connection(Remote),
    ensure_remote_connection({cluster_by_name,Name});
remove_connection_if_aliased({cluster_by_addr, Addr}=Remote, Name) ->
    lager:info("Replacing remote sink ~p with ~p", [Addr, Name]),
    remove_remote_connection(Remote),
    ensure_remote_connection({cluster_by_name,Name});
remove_connection_if_aliased(_,_) ->
    ok.

collect_garbage(State0) ->
    %% remove clusters that have no member IP addrs from our view
    State1 = orddict:fold(fun(Name, Cluster, State) ->
                                  case Cluster#cluster.members of
                                      [] ->
                                          remove_remote(Name, State);
                                      _ ->
                                          State
                                  end
                          end,
                          State0,
                          State0#state.clusters),
    State1.

%% Remove the given "remote" from all state and persisted ring and connections.
remove_remote(RemoteName, State) ->
    case RemoteName of
        {IP, Port} ->
            Remote = {?CLUSTER_ADDR_LOCATOR_TYPE, {IP, Port}},
            remove_remote_connection(Remote),
            State;
        ClusterName ->
            Remote = {?CLUSTER_NAME_LOCATOR_TYPE, ClusterName},
            remove_remote_connection(Remote),
            UpdatedClusters = orddict:erase(ClusterName, State#state.clusters),
            %% remove cluster from ring, which is done by saving an empty member list
            persist_members_to_ring(State, ClusterName, []),
            State#state{clusters = UpdatedClusters}
    end.

%% Simple Round Robin Balancer moves head to tail each time called.
round_robin_balancer([]) ->
    [];
round_robin_balancer([Addr|Addrs]) ->
    Addrs ++ [Addr].

%% Convert an inet:address to a string if needed.
string_of_ip(IP) when is_tuple(IP) ->    
    inet_parse:ntoa(IP);
string_of_ip(IP) ->
    IP.

members_of_cluster(ClusterName, State) ->
    case orddict:find(ClusterName, State#state.clusters) of
        error -> [];
        {ok,C} -> C#cluster.members
    end.

ensure_remote_connection(Remote) ->
    %% add will make sure there is only one connection per remote
    riak_core_cluster_conn_sup:add_remote_connection(Remote).

%% Drop our connection to the remote cluster.
remove_remote_connection(Remote) ->
    case riak_core_cluster_conn_sup:is_connected(Remote) of
        true ->
            riak_core_cluster_conn_sup:remove_remote_connection(Remote);
        _ ->
            ok
    end.

proxy_cast(_Cast, _State = #state{leader_node=Leader}) when Leader == undefined ->
    ok;
proxy_cast(Cast, _State = #state{leader_node=Leader}) ->
    gen_server:cast({?SERVER, Leader}, Cast).

%% Make a proxy call to the leader. If there is no leader elected or the request fails,
%% it will return the NoLeaderResult supplied.
proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
    {reply, NoLeaderResult, State};
proxy_call(Call, _NoLeaderResult, State = #state{leader_node=Leader}) ->
    Reply = gen_server:call({?SERVER, Leader}, Call, ?PROXY_CALL_TIMEOUT),
    {reply, Reply, State}.

%% Remove given IP Addresses from all clusters. Returns revised clusters orddict.
remove_ips_from_all_clusters(Addrs, Clusters) ->
    orddict:map(fun(_Name,C) ->
                        Mbrs = lists:foldl(fun(Addr, Acc) -> lists:delete(Addr, Acc) end,
                                           C#cluster.members,
                                           Addrs),
                        C#cluster{members=Mbrs}
                end,
                Clusters).

%% Add Members to Name'd cluster. Returns revised clusters orddict.
add_ips_to_cluster(Name, RebalancedMembers, Clusters) ->
    orddict:store(Name,
                  #cluster{name = Name,
                           members = RebalancedMembers,
                           last_conn = erlang:now()},
                  Clusters).

%% register a locator that just uses the address passed to it.
%% This is a way to boot strap a connection to a cluster by IP address.
register_cluster_addr_locator() ->
    % Identity locator function just returns a list with single member Addr
    Locator = fun(Addr, _Policy) -> {ok, [Addr]} end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_ADDR_LOCATOR_TYPE, Locator).

%% Setup a connection to all given cluster targets
connect_to_targets(Targets) ->
    lists:foreach(fun(Target) -> ensure_remote_connection(Target) end,
                  Targets).

%% start being a cluster manager leader
become_leader(State) when State#state.is_leader == false ->
    %% start leading and tell ourself to connect to known clusters in a bit.
    %% TODO: 5 seconds is arbitrary. It's enough time for the ring to be stable
    %% so that the call into the repl_ring handler won't crash. Fix this.
    erlang:send_after(5000, self(), connect_to_clusters),
    State#state{is_leader = true};
become_leader(State) ->
    ?TRACE(?debugMsg("Already the leader")),
    State.

%% stop being a cluster manager leader
become_proxy(State) when State#state.is_leader == false ->
    %% still not the leader
    ?TRACE(?debugMsg("Already a proxy")),
    State;
become_proxy(State) ->
    %% stop leading
    %% remove all outbound connections
    Connections = riak_core_cluster_conn_sup:connections(),
    [riak_core_cluster_conn_sup:remove_remote_connection(Remote) || {Remote, _Pid} <- Connections],
    State#state{is_leader = false}.

persist_members_to_ring(State, ClusterName, Members) ->
    SaveFun = State#state.save_members_fun,
    SaveFun(ClusterName, Members).

%% Return a list of locators, in our case, we'll use cluster names
%% that were saved in the ring
cluster_mgr_sites_fun() ->
    %% get cluster names from cluster manager
    Ring = riak_core_ring_manager:get_my_ring(),
    Clusters = riak_repl_ring:get_clusters(Ring),
    [{?CLUSTER_NAME_LOCATOR_TYPE, Name} || {Name, _Addrs} <- Clusters].    

%%-------------------------
%% control channel services
%%-------------------------

ctrlService(_Socket, _Transport, {error, Reason}, _Args, _Props) ->
    lager:error("Failed to accept control channel connection: ~p", [Reason]);
ctrlService(Socket, Transport, {ok, {cluster_mgr, MyVer, RemoteVer}}, _Args, Props) ->
    {ok, ClientAddr} = inet:peername(Socket),
    RemoteClusterName = proplists:get_value(clustername, Props),
    lager:info("Cluster Manager: accepted connection from cluster at ~p namded ~p",
               [ClientAddr, RemoteClusterName]),
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlServiceProcess,
                              [Socket, Transport, MyVer, RemoteVer, ClientAddr]),
    Transport:controlling_process(Socket, Pid),
    {ok, Pid}.

read_ip_address(Socket, Transport, Remote) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinAddr} ->
            MyAddr = binary_to_term(BinAddr),
            MyAddr;
        Error ->
            lager:error("Cluster Manager: failed to receive ip addr from remote ~p: ~p",
                        [Remote, Error]),
            undefined
    end.

%% process instance for handling control channel requests from remote clusters.
ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, ClientAddr) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, ?CTRL_ASK_NAME} ->
            %% remote wants my name
            MyName = riak_core_connection:symbolic_clustername(),
            ok = Transport:send(Socket, term_to_binary(MyName)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, ClientAddr);
        {ok, ?CTRL_ASK_MEMBERS} ->
            %% remote wants list of member machines in my cluster
            MyAddr = read_ip_address(Socket, Transport, ClientAddr),
            BalancedMembers = gen_server:call(?SERVER, {get_my_members, MyAddr}),
            ok = Transport:send(Socket, term_to_binary(BalancedMembers)),
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, ClientAddr);
        {error, timeout} ->
            %% timeouts are OK, I think.
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, ClientAddr);
        {error, Reason} ->
            lager:error("Cluster Manager: serice ~p failed recv on control channel. Error = ~p",
                        [self(), Reason]),
            % nothing to do now but die
            {error, Reason};
        Other ->
            lager:error("Cluster Manger: service ~p recv'd unknown message on cluster control channel: ~p",
                        [self(), Other]),
            % ignore and keep trying to be a nice service
            ctrlServiceProcess(Socket, Transport, MyVer, RemoteVer, ClientAddr)
    end.
