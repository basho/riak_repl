%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_app).
-author('Andy Gross <andy@basho.com>').
-behaviour(application).
-export([start/2,stop/1]).
-export([get_matching_address/2, prep_stop/1]).

-include("riak_core_connection.hrl").

-define(TRACE(_Stmt),ok).
%%-define(TRACE(Stmt),Stmt).

%% @spec start(Type :: term(), StartArgs :: term()) ->
%%          {ok,Pid} | ignore | {error,Error}
%% @doc The application:start callback for riak_repl.
%%      Arguments are ignored as all configuration is done via the erlenv file.
start(_Type, _StartArgs) ->
    riak_core_util:start_app_deps(riak_repl),

    %% Ensure that the KV service has fully loaded.
    riak_core:wait_for_service(riak_kv),

    IncarnationId = erlang:phash2({make_ref(), now()}),
    application:set_env(riak_repl, incarnation, IncarnationId),
    ok = ensure_dirs(),

    riak_core:register([{bucket_fixup, riak_repl}]),

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_repl_cinfo),


    %% Spin up supervisor
    case riak_repl_sup:start_link() of
        {ok, Pid} ->
            %% register functions for cluster manager to find it's own
            %% nodes' ip addrs
            riak_core_cluster_mgr:register_member_fun(
                fun cluster_mgr_member_fun/1),
            %% cluster manager leader will follow repl leader
            riak_repl2_leader:register_notify_fun(
              fun riak_core_cluster_mgr:set_leader/2),
            name_this_cluster(),
            riak_core_node_watcher:service_up(riak_repl, Pid),
            riak_core:register(riak_repl, [{stat_mod, riak_repl_stats}]),
            ok = riak_core_ring_events:add_guarded_handler(riak_repl_ring_handler, []),
            %% Add routes to webmachine
            [ webmachine_router:add_route(R)
              || R <- lists:reverse(riak_repl_web:dispatch_table()) ],
            %% Now that we have registered the ring handler, we can register the
            %% cluster manager name locator function (which reads the ring).
            register_cluster_name_locator(),

            %% makes service manager start connection dispatcher
            riak_repl2_rtsink_conn:register_service(),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @spec stop(State :: term()) -> ok
%% @doc The application:stop callback for riak_repl.
stop(_State) -> 
    lager:info("Stopped application riak_repl"),
    ok.

ensure_dirs() ->
    {ok, DataRoot} = application:get_env(riak_repl, data_root),
    LogDir = filename:join(DataRoot, "logs"),
    case filelib:ensure_dir(filename:join(LogDir, "empty")) of
        ok ->
            application:set_env(riak_repl, log_dir, LogDir),
            ok;
        {error, Reason} ->
            Msg = io_lib:format("riak_repl couldn't create log dir ~p: ~p~n", [LogDir, Reason]),
            riak:stop(lists:flatten(Msg))
    end,
    {ok, Incarnation} = application:get_env(riak_repl, incarnation),
    WorkRoot = filename:join([DataRoot, "work"]),
    prune_old_workdirs(WorkRoot),
    WorkDir = filename:join([WorkRoot, integer_to_list(Incarnation)]),
    case filelib:ensure_dir(filename:join([WorkDir, "empty"])) of
        ok ->
            application:set_env(riak_repl, work_dir, WorkDir),
            ok;
        {error, R} ->
            M = io_lib:format("riak_repl couldn't create work dir ~p: ~p~n", [WorkDir,R]),
            riak:stop(lists:flatten(M)),
            {error, R}
    end.

prune_old_workdirs(WorkRoot) ->
    case file:list_dir(WorkRoot) of
        {ok, SubDirs} ->
            DirPaths = [filename:join(WorkRoot, D) || D <- SubDirs],
            Cmds = [lists:flatten(io_lib:format("rm -rf ~s", [D])) || D <- DirPaths],
            [os:cmd(Cmd) || Cmd <- Cmds];
        _ ->
            ignore
    end.

%% Get the list of nodes of our ring
cluster_mgr_member_fun({IP, Port}) ->
    %% find the subnet for the interface we connected to
    {ok, MyIPs} = inet:getifaddrs(),
    {ok, NormIP} = riak_repl_util:normalize_ip(IP),
    ?TRACE(lager:info("normIP is ~p", [NormIP])),
    MyMask = lists:foldl(fun({_IF, Attrs}, Acc) ->
                case lists:member({addr, NormIP}, Attrs) of
                    true ->
                        NetMask = lists:foldl(fun({netmask, NM = {_, _, _, _}}, _) ->
                                    NM;
                                (_, Acc2) ->
                                    Acc2
                            end, undefined, Attrs),
                        %% convert the netmask to CIDR
                        CIDR = cidr(list_to_binary(tuple_to_list(NetMask)),0),
                        ?TRACE(lager:info("~p is ~p in CIDR", [NetMask, CIDR])),
                        CIDR;
                    false ->
                        Acc
                end
        end, undefined, MyIPs),
    case MyMask of
        undefined ->
            lager:warning("Connected IP not present locally, must be NAT. Returning ~p",
                         [{IP,Port}]),
            %% might as well return the one IP we know will work
            [{IP, Port}];
        _ ->
            ?TRACE(lager:notice("Mask is ~p", [MyMask])),
            AddressMask = mask_address(NormIP, MyMask),
            ?TRACE(lager:notice("address mask is ~p", [AddressMask])),
            Nodes = riak_core_node_watcher:nodes(riak_kv),
            {Results, _BadNodes} = rpc:multicall(Nodes, riak_repl_app,
                get_matching_address, [NormIP, AddressMask]),
            Results
    end.

%% count the number of 1s in netmask to get the CIDR
%% Maybe there's a better way....?
cidr(<<>>, Acc) ->
    Acc;
cidr(<<X:1/bits, Rest/bits>>, Acc) ->
    case X of
        <<1:1>> ->
            cidr(Rest, Acc + 1);
        _ ->
            cidr(Rest, Acc)
    end.

%% get the subnet mask as an integer, stolen from an old post on
%% erlang-questions
mask_address(Addr={_, _, _, _}, Maskbits) ->
    B = list_to_binary(tuple_to_list(Addr)),
    ?TRACE(lager:info("address as binary: ~p ~p", [B,Maskbits])),
    <<Subnet:Maskbits, _Host/bitstring>> = B,
    Subnet;
mask_address(_, _) ->
    %% presumably ipv6, don't have a function for that one yet
    undefined.

rfc1918({10, _, _, _}) ->
    true;
rfc1918({192.168, _, _}) ->
    true;
rfc1918(IP={172, _, _, _}) ->
    %% this one is a /12, not so simple
    mask_address({172, 16, 0, 0}, 12) == mask_address(IP, 12);
rfc1918(_) ->
    false.

%% find the right address to serve given the IP the node connected to
get_matching_address(IP, Mask) ->
    {RawListenIP, Port} = app_helper:get_env(riak_core, cluster_mgr),
    {ok, ListenIP} = riak_repl_util:normalize_ip(RawListenIP),
    case ListenIP of
        {0, 0, 0, 0} ->
            {ok, MyIPs} = inet:getifaddrs(),
            lists:foldl(fun({_IF, Attrs}, Acc) ->
                        V4Attrs = lists:filter(fun({addr, {_, _, _, _}}) ->
                                    true;
                                ({netmask, {_, _, _, _}}) ->
                                    true;
                                (_) ->
                                    false
                            end, Attrs),
                        case V4Attrs of
                            [] ->
                                Acc;
                            _ ->
                                MyIP = proplists:get_value(addr, V4Attrs),
                                NetMask = proplists:get_value(netmask, V4Attrs),
                                CIDR = cidr(list_to_binary(tuple_to_list(NetMask)), 0),
                                case mask_address(MyIP, CIDR) of
                                    Mask ->
                                        {MyIP, Port};
                                    _Other ->
                                        ?TRACE(lager:info("IP ~p with CIDR ~p masked as ~p",
                                                          [MyIP, CIDR, _Other])),
                                        Acc
                                end
                        end
                end, undefined, MyIPs); %% TODO if result is undefined, check NAT
            _ ->
                case rfc1918(IP) == rfc1918(ListenIP) of
                    true ->
                        %% Both addresses are either internal or external.
                        %% We'll have to assume the user knows what they're
                        %% doing
                        ?TRACE(lager:info("returning speific listen IP ~p",
                                          [ListenIP])),
                        {ListenIP, Port};
                    false ->
                        lager:warning("NAT detected?"),
                        undefined
                end
        end.

%% TODO: check the config for a name. Don't overwrite one a user has set via cmd-line
name_this_cluster() ->
    ClusterName = case riak_core_connection:symbolic_clustername() of
                      "undefined" ->
                          {ok, Ring} = riak_core_ring_manager:get_my_ring(),
                          lists:flatten(
                            io_lib:format("~p", [riak_core_ring:cluster_name(Ring)]));
                      Name ->
                          Name
                  end,
    riak_core_connection:set_symbolic_clustername(ClusterName).


%% Persist the named cluster and it's members to the repl ring metadata.
%% TODO: an empty Members list means "delete this cluster name"
cluster_mgr_write_cluster_members_to_ring(ClusterName, Members) ->
    ?TRACE(lager:info("Saving cluster to the ring: ~p of ~p", [ClusterName, Members])),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:set_clusterIpAddrs/2,
                                      {ClusterName, Members}).

%% Return a list of cluster targets by cluster name.
%% These will then be resolved by calls back to our registered
%% locator function, registered below.
cluster_mgr_read_cluster_targets_from_ring() ->
    %% get cluster names from cluster manager
    Ring = get_ring(),
    Clusters = riak_repl_ring:get_clusters(Ring),
    [{?CLUSTER_NAME_LOCATOR_TYPE, Name} || {Name, _Addrs} <- Clusters].

%% Register a locator for cluster names. MUST do this BEFORE we
%% register the save/restore functions because the restore function
%% is going to immediately try and locate functions if the cluster
%% manager is already the leader.
register_cluster_name_locator() ->
    Locator = fun(ClusterName, _Policy) ->
                      Ring = get_ring(),
                      {ok,riak_repl_ring:get_clusterIpAddrs(Ring, ClusterName)}
              end,
    ok = riak_core_connection_mgr:register_locator(?CLUSTER_NAME_LOCATOR_TYPE, Locator),
    %% Register functions to save/restore cluster names and their members
    riak_core_cluster_mgr:register_save_cluster_members_fun(
      fun cluster_mgr_write_cluster_members_to_ring/2),
    riak_core_cluster_mgr:register_restore_cluster_targets_fun(
      fun cluster_mgr_read_cluster_targets_from_ring/0).

get_ring() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:ensure_config(Ring).

prep_stop(_State) ->
    %% TODO: this should only run with BNW

    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.

        %% the repl bucket hook will check to see if the queue is running and deliver to
        %% another node if it's shutting down
        lager:info("Redirecting realtime replication traffic"),
        riak_repl2_rtq:shutdown(),

        lager:info("Stopping application riak_repl - marked service down.\n", []),
        %riak_core_ring_manager:force_update(),
        %% mark the service down so other nodes don't try to migrate to this
        %% one while it's going down
        riak_core_node_watcher:service_down(riak_repl),


        case riak_repl_migration:start_link() of
            {ok, _Pid} ->
                lager:info("Started migration server"),
                riak_repl_migration:migrate_queue();
            {error, _} -> lager:error("Can't start replication migration server")
        end
       catch
        Type:Reason ->
            lager:error("Stopping application riak_api - ~p:~p.\n", [Type, Reason])
    end,
    stopping.



