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
%% which instance of cluster manager is the leader. For example, the riak_repl_leader server
%% is probably a good place to do this from. Call set_leader_node(node(), pid()).
%%
%% If I'm the leader, I answer local RPC requests from non-leader cluster managers.
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
-define(MAX_CONS, 20).
-define(CLUSTER_POLLING_INTERVAL, 10 * 1000).

%% State of a resolved remote cluster
-record(cluster, {name :: string(),     % obtained from the remote cluster by ask_name()
                  members :: [ip_addr()], % list of suspected ip addresses for cluster
                  last_conn :: erlang:now() % last time we connected to the remote cluster
                 }).

%% remotes := orddict, key = ip_addr(), value = unresolved | clustername()

-record(state, {is_leader = false :: boolean(),                % true when the buck stops here
                leader_node = undefined :: undefined | node(),
                my_name = "" :: string(),                      % my local cluster name
                member_fun = fun() -> [] end,                  % return members of my cluster
                sites_fun = fun() -> [] end,                   % return all remote site IPs
                clusters = orddict:new() :: orddict:orddict()  % resolved clusters by name
               }).

-export([start_link/0,
         set_my_name/1,
         get_my_name/0,
         set_leader/1,
         get_leader/0,
         get_is_leader/0,
         register_cluster_locator/0,
         register_member_fun/1,
         register_sites_fun/1,
         add_remote_cluster/1,
         get_known_clusters/0,
         get_ipaddrs_of_cluster/1
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([ctrlService/4, ctrlServiceProcess/5]).

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
    register_cluster_name_locator(),
    register_cluster_addr_locator().

%% Set my cluster name
-spec(set_my_name(clustername()) -> ok).
set_my_name(MyName) ->
    gen_server:cast(?SERVER, {set_my_name, MyName}).

%% Called by riak_repl_leader whenever a leadership election
%% takes place. Tells us who the leader is.
set_leader(LeaderNode) ->
    gen_server:call(?SERVER, {set_leader_node, LeaderNode}).

%% Reply with the current leader node.
get_leader() ->
    gen_server:call(?SERVER, leader_node).

%% Return my cluster name
-spec(get_my_name() -> clustername()).
get_my_name() ->
    gen_server:call(?SERVER, get_my_name).

get_is_leader() ->
    gen_server:call(?SERVER, get_is_leader).

register_member_fun(MemberFun) ->
    gen_server:cast(?SERVER, {register_member_fun, MemberFun}).

register_sites_fun(SitesFun) ->
    gen_server:cast(?SERVER, {register_sites_fun, SitesFun}).

%% Specify how to reach a remote cluster, it's name is
%% retrieved by asking it via the control channel.
-spec(add_remote_cluster(ip_addr()) -> ok).
add_remote_cluster({IP,Port}) ->
    gen_server:cast(?SERVER, {add_remote_cluster, {IP,Port}}).

%% Retrieve a list of known remote clusters that have been resolved (they responded).
-spec(get_known_clusters() -> {ok,[clustername()]} | term()).
get_known_clusters() ->
    gen_server:call(?SERVER, get_known_clusters).

%% Return a list of the known IP addresses of all nodes in the remote cluster.
-spec(get_ipaddrs_of_cluster(clustername()) -> [ip_addr()]).
get_ipaddrs_of_cluster(ClusterName) ->
        gen_server:call(?SERVER, {get_known_ipaddrs_of_cluster, {name,ClusterName}}).

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

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call({set_leader_node, LeaderNode}, _From, State) ->
    State2 = State#state{leader_node = LeaderNode},
    case node() of
        LeaderNode ->
            %% oh crap, it's me!
            {reply, ok, become_leader(State2)};
        _ ->
            %% not me.
            {reply, ok, become_proxy(State2)}
    end;

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

%% Return possible IP addrs of nodes on the named remote cluster.
%% If a leader has not been elected yet, return an empty list.
handle_call({get_known_ipaddrs_of_cluster, {name, ClusterName}}, _From, State) ->
    case State#state.is_leader of
        true ->
            Addrs = case orddict:find(ClusterName, State#state.clusters) of
                        error -> [];
                        {ok,C} -> C#cluster.members
                    end,
            {reply, {ok, Addrs}, State};
        false ->
            NoLeaderResult = {ok, []},
            proxy_call({get_known_ipaddrs_of_cluster, {name, ClusterName}},
                       NoLeaderResult, State)
    end.

handle_cast({set_my_name, MyName}, State) ->
    NewState = State#state{my_name = MyName},
    {noreply, NewState};

handle_cast({register_member_fun, Fun}, State) ->
    {noreply, State#state{member_fun=Fun}};

handle_cast({register_sites_fun, Fun}, State) ->
    {noreply, State#state{sites_fun=Fun}};

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

%% The client connection recived (or polled for) an update from the remote cluster
handle_cast({cluster_updated, Name, Members, Remote}, State) ->
    lager:info("Cluster Manager: updated by remote ~p named ~p with members: ~p",
               [Remote, Name, Members]),
    State#state{clusters=add_ips_to_cluster(Name, Members, State#state.clusters)};

handle_cast({connected_to_remote, Name, Members, _IpAddr, Remote}, State) ->
    lager:info("Cluster Manager: resolved remote ~p named ~p with members: ~p",
               [Remote, Name, Members]),
    %% Remove all Members from all clusters.
    Clusters1 = remove_ips_from_all_clusters(Members, State#state.clusters),
    %% Add Members to Name'd cluster.
    Clusters2 = add_ips_to_cluster(Name, Members, Clusters1),
    {noreply, State#state{clusters = Clusters2}};

handle_cast(Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [Unhandled])),
    {error, unhandled}. %% this will crash the server

%% it is time to poll all clusters and get updated member lists
handle_info(poll_clusters_timer, State) when State#state.is_leader == true ->
    Connections = riak_core_cluster_conn_sup:connected(),
    [Pid ! {self(), poll_cluster} || {_Remote, Pid} <- Connections],
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};
handle_info(poll_clusters_timer, State) ->
    erlang:send_after(?CLUSTER_POLLING_INTERVAL, self(), poll_clusters_timer),
    {noreply, State};

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

ensure_remote_connection(Remote) ->
    case riak_core_cluster_conn_sup:is_connected(Remote) of
        false ->
            riak_core_cluster_conn_sup:add_remote_connection(Remote);
        true ->
            ok
    end.

proxy_cast(_Cast, _State = #state{leader_node=Leader}) when Leader == undefined ->
    ?TRACE(?debugFmt("proxy_cast: dropping because leader is undefined: ~p", [_Cast])),
    ok;
proxy_cast(Cast, _State = #state{leader_node=Leader}) ->
    ?TRACE(?debugFmt("proxy_cast: forwarding to leader: ~p", [Cast])),
    gen_server:cast({?SERVER, Leader}, Cast).

proxy_call(_Call, NoLeaderResult, State = #state{leader_node=Leader}) when Leader == undefined ->
    ?TRACE(?debugFmt("proxy_call: dropping because leader is undefined: ~p", [_Call])),
    {reply, NoLeaderResult, State};
proxy_call(Call, _NoLeaderResult, State = #state{leader_node=Leader}) ->
    ?TRACE(?debugFmt("proxy_call: forwarding to leader: ~p", [Call])),
    {reply, gen_server:cast({?SERVER, Leader}, Call), State}.

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
add_ips_to_cluster(Name, Addrs, Clusters) ->
    orddict:store(Name,
                  #cluster{name = Name,
                           members = Addrs,
                           last_conn = erlang:now()},
                  Clusters).

register_cluster_name_locator() ->
    Locator = fun(ClusterName, _Policy) ->
                      get_ipaddrs_of_cluster(ClusterName)
              end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_NAME_LOCATOR_TYPE, Locator).

%% register a locator that just uses the address passed to it.
register_cluster_addr_locator() ->
    % Identity locator function just returns a list with single member Addr
    Locator = fun(Addr, _Policy) -> {ok, [Addr]} end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_ADDR_LOCATOR_TYPE, Locator).

%% Setup a connection to all given IP addresses
connect_to_ips(IPs) ->
    lists:foreach(fun(Addr) ->
                          Target = {?CLUSTER_ADDR_LOCATOR_TYPE, Addr},
                          ensure_remote_connection(Target)
                  end,
                  IPs).

%% start being a cluster manager leader
become_leader(State) when State#state.is_leader == false ->
    ?TRACE(?debugMsg("Becoming the leader")),
    lager:info("Becoming the leader"),
    %% start leading
    %% Open a connection to all registered IP addresses
    SitesFun = State#state.sites_fun,
    IPs = SitesFun(),
    connect_to_ips(IPs),
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
    ?TRACE(?debugMsg("Becoming a proxy")),
    lager:info("Becoming a proxy"),
    %% stop leading
    %% remove all outbound connections
    Connections = riak_core_cluster_conn_sup:connected(),
    [riak_core_cluster_conn_sup:remove_remote_connection(Remote) || {Remote, _Pid} <- Connections],
    State#state{is_leader = false}.

%%-------------------------
%% control channel services
%%-------------------------

ctrlService(_Socket, _Transport, {error, Reason}, _Args) ->
    lager:error("Failed to accept control channel connection: ~p", [Reason]);
ctrlService(Socket, Transport, {ok, {cluster_mgr, MyVer, RemoteVer}}, Args) ->
    {ok, ClientAddr} = inet:peername(Socket),
    lager:info("Cluster Manager: accepted connection from cluster at ~p", [ClientAddr]),
    Pid = proc_lib:spawn_link(?MODULE,
                              ctrlServiceProcess,
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
