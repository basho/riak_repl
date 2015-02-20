%% @doc Console commands for "Version 3" replication, aka
%% 'mode_repl13'.
-module(riak_repl_console13).
-include("riak_repl.hrl").
-export([register/0, commands_usage/0, upgrade/1]).

-import(riak_repl_console, [register_command/4, script_name/0]).

-import(clique_status, [text/1, alert/1, table/1]).

-export([cluster_mgr_stats/0]).

%%-----------------------
%% Interface
%%-----------------------

-spec register() -> ok.
register() ->
    ok = register_commands(),
    ok = register_usage(),
    %% TODO: add max_fs* settings
    %% ok = register_configs(),
    ok.

register_commands() ->
    ok = register_command(["clusterstats"], [],
                          [{host, [{longname, "host"},
                                   {datatype, ip}]},
                           {protocol, [{longname, "protocol"},
                                       {datatype, atom}]}],
                          fun clusterstats/2),
    ok = register_command(["clustername"], [],
                          [{name, [{shortname, "n"},
                                   {longname, "name"},
                                   {datatype, string}]}],
                          fun clustername/2),
    ok = register_command(["clusters"], [], [], fun clusters/2),
    ok = register_command(["connections"], [], [], fun connections/2),
    ok = register_command(["connect"], [{address, [{datatype, ip}]}], [],
                          fun connect/2),
    ok = register_command(["disconnect"], [{remote, [{datatype, [ip, string]}]}], [],
                          fun disconnect/2),
    ok = register_command(["realtime", "enable"],
                          [{remote, [{datatype, string}]}],
                          [],
                          fun realtime_enable/2),
    ok = register_command(["realtime", "disable"],
                          [{remote, [{datatype, string}]}],
                          [],
                          fun realtime_disable/2),
    ok = register_command(["realtime", "start"],
                          [{remote, [{datatype, string}]}],
                          [{all, [{longname, "all"},
                                  {shortname, "a"}]}],
                          fun realtime_start/2),
    ok = register_command(["realtime", "stop"],
                          [{remote, [{datatype, string}]}],
                          [{all, [{longname, "all"},
                                  {shortname, "a"}]}],
                          fun realtime_stop/2),
    ok = register_command(["realtime", "cascades", "enable"],
                          [],[],
                          fun realtime_cascades_enable/2),
    ok = register_command(["realtime", "cascades", "disable"],
                          [],[],
                          fun realtime_cascades_disable/2),
    ok = register_command(["realtime", "cascades", "show"],
                          [],[],
                         fun realtime_cascades_show/2),
    ok = register_command(["fullsync", "enable"],
                          [{remote, [{datatype, string}]}],
                          [],
                          fun fullsync_enable/2),
    ok = register_command(["fullsync", "disable"],
                          [{remote, [{datatype, string}]}],
                          [],
                          fun fullsync_disable/2),
    ok = register_command(["fullsync", "start"],
                          [{remote, [{datatype, string}]}],
                          [{all, [{longname, "all"},
                                  {shortname, "a"}]}],
                          fun fullsync_start/2),
    ok = register_command(["fullsync", "stop"],
                          [{remote, [{datatype, string}]}],
                          [{all, [{longname, "all"},
                                  {shortname, "a"}]}],
                          fun fullsync_stop/2),
    ok = register_command(["proxy-get", "enable"],
                          [{remote, [{datatype, string}]}],
                          [], fun proxy_get_enable/2),
    ok = register_command(["proxy-get", "disable"],
                          [{remote, [{datatype, string}]}],
                          [], fun proxy_get_disable/2),
    ok = register_command(["proxy-get", "redirect", "cluster-id"],
                          [],
                          [], fun proxy_get_redirect_cluster_id/2),
    ok = register_command(["proxy-get", "redirect", "show"],
                          [{from, [{datatype, string}]}],
                          [], fun proxy_get_redirect_show/2),
    ok = register_command(["proxy-get", "redirect", "add"],
                          [{from, [{datatype, string}]},
                           {to, [{datatype, string}]}],
                          [], fun proxy_get_redirect_add/2),
    ok = register_command(["proxy-get", "redirect", "delete"],
                          [{from, [{datatype, string}]},
                           {to, [{datatype, string}]}],
                          [], fun proxy_get_redirect_delete/2),
    ok.


register_usage(Cmd, Usage) ->
    riak_repl_console:register_usage(Cmd, Usage).

register_usage() ->
    ok = register_usage(["clusterstats"],
                        "clusterstats [ --protocol=PROTO | --host=IP:PORT ]\n\n"
                        "  Displays cluster statistics, optionally filtered by a protocol or host connection.\n\n"
                        "  Options:\n"
                        "    --protocol=PROTO    Filters to a protocol where PROTO is one of:\n"
                        "                        rt_repl, proxy_get, identity\n"
                        "    --host=IP:PORT      Filters to a specific host, identified by IP and PORT"),
    ok = register_usage(["clustername"],
                        "clustername [ (-n | --name) NAME ]\n\n"
                        "  Shows or sets the symbolic clustername. Supplying the `-n` option sets the name.\n\n"
                        "  Options:\n"
                        "    -n NAME, --name NAME   Sets the symbolic name to NAME"),
    ok = register_usage(["clusters"],
                        "clusters\n\n"
                        "  Displays information about known clusters."),
    ok = register_usage(["connections"],
                        "connections\n\n"
                        "  Displays a list of current replication connections."),
    ok = register_usage(["connect"],
                        "connect address=IP:PORT\n\n"
                        "  Connects to a remote cluster."),
    ok = register_usage(["disconnect"],
                        "disconnect remote=(IP:PORT | NAME)\n\n"
                        "  Disconnects from a connected remote cluster."),
    ok = register_usage(["realtime"], realtime_usage()),
    ok = register_usage(["realtime", "enable"], realtime_enable_disable_usage()),
    ok = register_usage(["realtime", "disable"], realtime_enable_disable_usage()),
    ok = register_usage(["realtime", "start"], realtime_start_stop_usage()),
    ok = register_usage(["realtime", "stop"], realtime_start_stop_usage()),
    ok = register_usage(["realtime", "cascades"], realtime_cascades_usage()),
    ok = register_usage(["fullsync"], fullsync_usage()),
    ok = register_usage(["fullsync", "enable"], fullsync_enable_disable_usage()),
    ok = register_usage(["fullsync", "disable"], fullsync_enable_disable_usage()),
    ok = register_usage(["fullsync", "start"], fullsync_start_stop_usage()),
    ok = register_usage(["fullsync", "stop"], fullsync_start_stop_usage()),
    ok = register_usage(["proxy-get"], proxy_get_usage()),
    ok = register_usage(["proxy-get", "enable"], proxy_get_enable_disable_usage()),
    ok = register_usage(["proxy-get", "disable"], proxy_get_usage()),
    ok = register_usage(["proxy-get", "redirect"], proxy_get_usage()),
    ok = register_usage(["proxy-get", "redirect", "show"], fun proxy_get_redirect_show_usage/0),
    ok = register_usage(["proxy-get", "redirect", "add"], fun proxy_get_redirect_add_delete_usage/0),
    ok = register_usage(["proxy-get", "redirect", "delete"], fun proxy_get_redirect_add_delete_usage/0),
    ok = register_usage(["proxy-get", "redirect", "cluster-id"], proxy_get_redirect_usage()).


-spec commands_usage() -> string().
commands_usage() ->
    "  Version 3 Commands:\n"
    "    clustername                 Show or set the cluster name\n"
    "    clusterstats                Display cluster stats\n"
    "    clusters                    Display known clusters\n"
    "    connect                     Connect to a remote cluster\n"
    "    connections                 Display a list of connections\n"
    "    disconnect                  Disconnect from a remote cluster\n"
    "    fullsync                    Manipulate fullsync replication\n"
    "    nat-map                     Manipulate NAT mappings\n"
    "    proxy-get                   Manipulate proxy-get\n"
    "    realtime                    Manipulate realtime replication".

realtime_usage() ->
    "realtime <sub-command> [<arg> ...]\n\n"
    "  Manipulate realtime replication. Realtime replication streams\n"
    "  incoming writes on the source cluster to the sink cluster(s).\n\n"
    "  Sub-commands:\n"
    "    enable      Enable realtime replication\n"
    "    disable     Disable realtime replication\n"
    "    start       Start realtime replication\n"
    "    stop        Stop realtime replication\n"
    "    cascades    Manipulate cascading realtime replication".

realtime_cascades_usage() ->
    "realtime cascades SUBCOMMAND\n\n"
    "  Manipulate cascading realtime replication. When this cluster is a\n"
    "  sink and is receiving realtime replication, it can propagate\n"
    "  incoming writes to any clusters for which it is a source and\n"
    "  realtime replication is enabled.\n\n"
    "  Sub-commands:\n"
    "    enable      Enable cascading realtime replication\n"
    "    disable     Disable cascading realtime replication\n"
    "    show        Show the current cascading realtime replication setting".

realtime_enable_disable_usage() ->
    "realtime ( enable | disable ) remote=CLUSTERNAME\n\n"
    "  Enable or disable realtime replication to CLUSTERNAME.".

realtime_start_stop_usage() ->
    "realtime ( start | stop ) ( remote=CLUSTERNAME | --all )\n\n"
    "  Start or stop realtime replication. When 'remote' is given, only\n"
    "  the specified sink CLUSTERNAME will be affected. When --all is given,\n"
    "  all realtime replication to all sinks will be started or stopped.".

fullsync_usage() ->
    "fullsync SUBCOMMAND ...\n\n"
    "  Manipulate fullsync replication. Fullsync replication compares data\n"
    "  on the source and the sink and then sends detected differences to\n"
    "  the sink cluster.\n\n"
    "  Sub-commands:\n"
    "    enable      Enable fullsync replication\n"
    "    disable     Disable fullsync replication\n"
    "    start       Start fullsync replication\n"
    "    stop        Stop fullsync replication\n".

fullsync_enable_disable_usage() ->
    "fullsync ( enable | disable ) remote=CLUSTERNAME\n\n"
    "  Enable or disable fullsync replication to CLUSTERNAME.".

fullsync_start_stop_usage() ->
    "fullsync ( start | stop ) ( remote=CLUSTERNAME | --all )\n\n"
    "  Start or stop fullsync replication. When 'remote' is given, only\n"
    "  the specified sink CLUSTERNAME will be affected. When --all is given,\n"
    "  all realtime replication to all sinks will be started or stopped.".

proxy_get_usage() ->
    "proxy-get SUBCOMMAND ...\n\n"
     "  Manipulate proxy-get functionality. Proxy-get allows sink clusters\n"
     "  to actively fetch remote objects over a realtime replication\n"
     "  connection. Currently, this is only used by Riak CS.\n\n"
     "  Sub-commands:\n"
     "    enable     Enable proxy-get on the source\n"
     "    disable    Disable proxy-get on the source\n"
     "    redirect   Manipulation proxy-get redirection".

proxy_get_enable_disable_usage() ->
    "proxy-get ( enable | disable ) remote=CLUSTERNAME\n\n"
    "  Enables or disables proxy-get requests from sink CLUSTERNAME to this\n"
    "  source cluster.".

proxy_get_redirect_usage() ->
    "proxy-get redirect SUBCOMMAND ...\n\n"
    "  Manipulate proxy-get redirection functionality. Redirection allows\n"
    "  existing proxy-get connections to be redirected to new source\n"
    "  clusters so that the original source cluster can be decommissioned.\n\n"
    "  Sub-commands:\n"
    "    add          Add a proxy-get redirection\n"
    "    delete       Delete an existing proxy-get redirection\n"
    "    show         Show a proxy-get redirection\n"
    "    cluster-id   Display the local cluster's identifier".

proxy_get_redirect_show_usage() ->
    "proxy-get redirect show from=SOURCE\n\n"
    "  Show an existing proxy-get redirection. SOURCE must correspond to\n"
    "  the result from the `" ++ script_name() ++ "proxy-get redirect cluster-id` command.".

proxy_get_redirect_add_delete_usage() ->
    "proxy-get redirect ( add | delete ) from=SOURCE to=DESTINATION\n\n"
    "  Add or delete a proxy-get redirection. Arguments SOURCE and\n"
    "  DESTINATION must correspond to the result from the `" ++ script_name() ++ "\n"
    "  proxy-get redirect cluster-id` command.".


upgrade(["clustername", [$-|_]|_]=Args) ->
    %% Don't upgrade a call that includes a flag
    Args;
upgrade(["clustername", Arg]=Args) ->
    upgrade_warning(Args, "Use `clustername --name ~s`", [Arg]),
    ["clustername", "-n", Arg];
upgrade(["clusterstats", [$-|_]|_]=Args) ->
    %% Don't upgrade a call that includes a flag
    Args;
upgrade(["clusterstats", Arg]=Args) ->
    case string:words(Arg, ":") of
        1 ->
            upgrade_warning(Args, "Use `clusterstats --protocol ~s`", [Arg]),
            ["clusterstats", "--protocol", Arg];
        2 ->
            upgrade_warning(Args, "Use `clusterstats --host ~s`", [Arg]),
            ["clusterstats", "--host", Arg];
        _ -> Args
    end;
upgrade(["connect", Arg|Rest]=Args) ->
    case string:words(Arg, "=") of
        2 -> Args;
        1 ->
            upgrade_warning(Args, "Use `connect address=~s`", [Arg]),
            ["connect", "address="++Arg|Rest];
        _ -> Args
    end;
upgrade(["disconnect", Arg|Rest]=Args) ->
    case string:words(Arg, "=") of
        2 -> Args;
        1 ->
            upgrade_warning(Args, "Use `disconnect remote=~s`", [Arg]),
            ["disconnect", "remote="++Arg|Rest]
    end;
upgrade(["realtime", Command, Arg|Rest]=Args) when Command == "enable";
                                                   Command == "disable";
                                                   Command == "start";
                                                   Command == "stop" ->
    case string:words(Arg, "=") of
        2 -> Args;
        1 ->
            upgrade_warning(Args, "Use `realtime ~s remote=~s`", [Command, Arg]),
            ["realtime", Command, "remote="++Arg|Rest]
    end;
upgrade(["realtime", Command]=Args) when Command == "start";
                                    Command == "stop" ->
    upgrade_warning(Args, "Use `realtime ~s --all`", [Command]),
    ["realtime", Command, "--all"];
upgrade(["realtime", "cascades", "always"]=Args) ->
    upgrade_warning(Args, "Use `realtime cascades enable`", []),
    ["realtime", "cascades", "enable"];
upgrade(["realtime", "cascades", "never"]=Args) ->
    upgrade_warning(Args, "Use `realtime cascades disable`", []),
    ["realtime", "cascades", "disable"];
upgrade(["fullsync", Command, Arg|Rest]=Args) when Command == "enable";
                                                   Command == "disable";
                                                   Command == "start";
                                                   Command == "stop" ->
    case string:words(Arg, "=") of
        2 -> Args;
        1 ->
            upgrade_warning(Args, "Use `fullsync ~s remote=~s`", [Command, Arg]),
            ["fullsync", Command, "remote="++Arg|Rest]
    end;
upgrade(["fullsync", Command]=Args) when Command == "start";
                                    Command == "stop" ->
    upgrade_warning(Args, "Use `fullsync ~s --all`", [Command]),
    ["fullsync", Command, "--all"];
upgrade(["fullsync", Key]=Args) when Key == "max_fssource_node";
                                     Key == "max_fssource_cluster";
                                     Key == "max_fssink_node" ->
    TKey = config_key_translation(Key),
    upgrade_warning(Args, "Use `show ~s`", [TKey]),
    ["show", TKey];
upgrade(["fullsync", Key, Value]=Args) when Key == "max_fssource_node";
                                            Key == "max_fssource_cluster";
                                            Key == "max_fssink_node" ->
    TKey = config_key_translation(Key),
    upgrade_warning(Args, "Use `show ~s`", [TKey]),
    ["set", TKey++"="++Value];
upgrade(Args) ->
    Args.

%% @doc Registers a warning about using a deprecated form of a
%% command.
upgrade_warning(Args, Fmt, FArgs) ->
    put(upgrade_warning, {string:join(Args, " "), io_lib:format(Fmt, FArgs)}).

config_key_translation("max_fssource_node")    -> "mdc.fullsync.source.max_workers_per_node";
config_key_translation("max_fssource_cluster") -> "mdc.fullsync.source.max_workers_per_cluster";
config_key_translation("max_fssink_node")      -> "mdc.fullsync.sink.max_workers_per_node".

output(CmdOut) ->
    case get(upgrade_warning) of
        undefined -> CmdOut;
        {Arguments, Message} ->
            erase(upgrade_warning),
            [error_msg("The command form `~s` is deprecated. ~s~n", [Arguments, Message]),
             CmdOut]
    end.

error_out(Fmt, Args) ->
    output(error_msg(Fmt, Args)).

error_msg(Fmt, Args) ->
    [alert(text_msg(Fmt, Args))].

text_out(Str) ->
    text_out(Str, []).

text_out(Str, Args) ->
    output(text_msg(Str, Args)).

text_msg(Fmt, Args) ->
    [text(io_lib:format(Fmt, Args))].

%%-----------------------
%% Command: clusterstats
%%-----------------------
%% Show cluster stats for this node
clusterstats(_, Flags) ->
    try
        CMStats = cluster_mgr_stats(),
        CConnStats = case Flags of
                         [] ->
                             riak_core_connection_mgr_stats:get_consolidated_stats();
                         [{host, {IP, Port}}] when is_list(IP), is_integer(Port) ->
                             riak_core_connection_mgr_stats:get_stats_by_ip({IP,Port});
                         [{protocol, ProtocolId}] when is_atom(ProtocolId) ->
                             riak_core_connection_mgr_stats:get_stats_by_protocol(ProtocolId);
                         _ ->
                             throw(badflags)
                     end,
        %% TODO: make this output better
        text_out("~p~n", [CMStats ++ CConnStats])
    catch
        throw:badflags -> usage
    end.

%% rtq_stats() ->
%%     case erlang:whereis(riak_repl2_rtq) of
%%         Pid when is_pid(Pid) ->
%%             [{realtime_queue_stats, riak_repl2_rtq:status()}];
%%         _ -> []
%%     end.

cluster_mgr_stats() ->
    case erlang:whereis(riak_repl_leader_gs) of
        Pid when is_pid(Pid) ->
            ConnectedClusters = case riak_core_cluster_mgr:get_known_clusters() of
                                    {ok, Clusters} ->
                                        [erlang:list_to_binary(Cluster) || Cluster <-
                                                                               Clusters];
                                    Error -> Error
                                end,
            [{cluster_name,
              erlang:list_to_binary(riak_core_connection:symbolic_clustername())},
             {cluster_leader, riak_core_cluster_mgr:get_leader()},
             {connected_clusters, ConnectedClusters}];
        _ -> []
    end.

%% clusterstats([Arg]) ->
%%     NWords = string:words(Arg, $:),
%%     case NWords of
%%         1 ->
%%             %% assume protocol-id
%%             ProtocolId = list_to_atom(Arg),
%%             CConnStats = riak_core_connection_mgr_stats:get_stats_by_protocol(ProtocolId),
%%             CMStats = cluster_mgr_stats(),
%%             Stats = CMStats ++ CConnStats,
%%             io:format("~p~n", [Stats]);
%%         2 ->
%%              Address = Arg,
%%              IP = string:sub_word(Address, 1, $:),
%%              PortStr = string:sub_word(Address, 2, $:),
%%              {Port,_Rest} = string:to_integer(PortStr),
%%              CConnStats = riak_core_connection_mgr_stats:get_stats_by_ip({IP,Port}),
%%              CMStats = cluster_mgr_stats(),
%%              Stats = CMStats ++ CConnStats,
%%              io:format("~p~n", [Stats]);
%%         _ ->
%%             {error, {badarg, Arg}}
%%     end.

%%-----------------------
%% Command: clustername
%%-----------------------
clustername([], []) ->
    text_out("Cluster name: ~s~n", [riak_core_connection:symbolic_clustername()]);
clustername([], [{name, ClusterName}]) ->
    riak_core_ring_manager:ring_trans(fun riak_core_connection:set_symbolic_clustername/2,
                                      ClusterName),
    text_out("Cluster name was set to: ~s~n", [ClusterName]).

%%-----------------------
%% Command: clusters
%%-----------------------
clusters([],[]) ->
    {ok, Clusters} = riak_core_cluster_mgr:get_known_clusters(),
    output(text([ begin
                      {ok,Members} = riak_core_cluster_mgr:get_ipaddrs_of_cluster(ClusterName),
                      IPs = [string_of_ipaddr(Addr) || Addr <- Members],
                      io_lib:format("~s: ~p~n", [ClusterName, IPs])
                  end || ClusterName <- Clusters])).

%%-----------------------
%% Command: connections
%%-----------------------
connections([], []) ->
    %% get cluster manager's outbound connections to other "remote" clusters,
    %% which for now, are all the "sinks".
    {ok, Conns} = riak_core_cluster_mgr:get_connections(),
    Headers = [{connection, "Connection"},
               {cluster_name, "Cluster Name"},
               {pid, "Ctrl-Pid"},
               {members, "Members"},
               {status, "Status"}],
    Rows = [format_cluster_conn(Conn) || Conn <- Conns],
    output(table([Headers|Rows])).

string_of_ipaddr({IP, Port}) ->
    lists:flatten(io_lib:format("~s:~p", [IP, Port])).

choose_best_addr({cluster_by_addr, {IP,Port}}, _ClientAddr) ->
    string_of_ipaddr({IP,Port});
choose_best_addr({cluster_by_name, _}, ClientAddr) ->
    string_of_ipaddr(ClientAddr).

string_of_remote({cluster_by_addr, {IP,Port}}) ->
    string_of_ipaddr({IP,Port});
string_of_remote({cluster_by_name, ClusterName}) ->
    ClusterName.

%% Format info about this sink into a clique table row.
%% Remote :: {ip,port} | ClusterName
format_cluster_conn({Remote,Pid}) ->
    {ClusterName, MemberList, Status} = get_cluster_conn_status(Remote, Pid),
    [{connection, string_of_remote(Remote)},
     {cluster_name, ClusterName},
     {pid, io_lib:format("~p", [Pid])},
     {members, format_cluster_conn_members(MemberList)},
     {status, format_cluster_conn_status(Status)}].

get_cluster_conn_status(Remote, Pid) ->
    %% try to get status from Pid of cluster control channel.  if we
    %% haven't connected successfully yet, it will time out, which we
    %% will fail fast for since it's a local process, not a remote
    %% one.
    try riak_core_cluster_conn:status(Pid, 2) of
        {Pid, status, {ClientAddr, _Transport, Name, Members}} ->
            CAddr = choose_best_addr(Remote, ClientAddr),
            {Name, Members, {via, CAddr}};
        {_StateName, SRemote} ->
            {"", [], {connecting, SRemote}}
    catch
        'EXIT':{timeout, _} ->
            {"", [], timeout}
    end.

format_cluster_conn_status({via, CAddr}) -> io_lib:format("via ~s", [CAddr]);
format_cluster_conn_status({connecting, SRemote}) -> io_lib:format("connecting to ~s", [string_of_remote(SRemote)]);
format_cluster_conn_status(timeout) -> "timed out".

format_cluster_conn_members(Members) ->
    string:join([ string_of_ipaddr(Addr) || Addr <- Members ], ",").

%%-----------------------
%% Command: connect
%%-----------------------
connect([{address, {IP, Port}}], []) ->
    ?LOG_USER_CMD("Connect to cluster at ~p:~p", [IP, Port]),
    case riak_core_connection:symbolic_clustername() of
        "undefined" ->
            %% TODO: This should return an error, not a bare status,
            %% but we still want to be able to print to stderr. This
            %% will require a clique enhancement.
            error_out("Error: Unable to establish connections until local cluster is named.~n"
                      "First use ~s clustername --name NAME ~n", [script_name()]);
        _Name ->
            riak_core_cluster_mgr:add_remote_cluster({IP, Port}),
            text_out("Connecting to remote cluster at ~p:~p.", [IP, Port])
    end;
connect(_, _) ->
    usage.


%%-----------------------
%% Command: disconnect
%%-----------------------
disconnect([{remote, {IP, Port}}], []) ->
    ?LOG_USER_CMD("Disconnect from cluster at ~p:~p", [IP, Port]),
    riak_core_cluster_mgr:remove_remote_cluster({IP, Port}),
    text_out("Disconnecting from cluster at ~p:~p~n", [IP, Port]);
disconnect([{remote, Name}], []) ->
    ?LOG_USER_CMD("Disconnect from cluster ~p", [Name]),
    riak_core_cluster_mgr:remove_remote_cluster(Name),
    text_out("Disconnecting from cluster ~p~n", [Name]);
disconnect(_, _) ->
    usage.


%%--------------------------
%% Command: realtime enable
%%--------------------------

realtime_enable([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Enable Realtime Replication to cluster ~p", [Remote]),
    case riak_repl2_rt:enable(Remote) of
        not_changed ->
            error_out("Realtime replication to cluster ~p already enabled!~n", [Remote]);
        {ok, _} ->
            text_out("Realtime replication to cluster ~p enabled.~n", [Remote])
    end;
realtime_enable(_, _) ->
    usage.

%%--------------------------
%% Command: realtime disable
%%--------------------------
realtime_disable([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Disable Realtime Replication to cluster ~p", [Remote]),
    case riak_repl2_rt:disable(Remote) of
        not_changed ->
            error_out("Realtime replication to cluster ~p already disabled!~n", [Remote]);
        {ok, _} ->
            text_out("Realtime replication to cluster ~p disabled.~n", [Remote])
    end;
realtime_disable(_, _) ->
    usage.

%%--------------------------
%% Command: realtime start
%%--------------------------
realtime_start([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Start Realtime Replication to cluster ~p", [Remote]),
    case riak_repl2_rt:start(Remote) of
        not_changed ->
            error_out("Realtime replication to cluster ~p is already started or not enabled!~n", [Remote]);
        {ok, _} ->
            text_out("Realtime replication to cluster ~p started.~n", [Remote])
    end;
realtime_start([], [{all, _}]) ->
    ?LOG_USER_CMD("Start Realtime Replication to all connected clusters", []),
    Remotes = riak_repl2_rt:enabled(),
    [ realtime_start([{remote, Remote}], []) || Remote <- Remotes ];
realtime_start(_, _) ->
    usage.

%%--------------------------
%% Command: realtime stop
%%--------------------------
realtime_stop([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Stop Realtime Replication to cluster ~p", [Remote]),
    case riak_repl2_rt:stop(Remote) of
        not_changed ->
            error_out("Realtime replication to cluster ~p is already stopped or not enabled!~n", [Remote]);
        {ok, _} ->
            text_out("Realtime replication to cluster ~p stopped.~n", [Remote])
    end;
realtime_stop([], [{all, _}]) ->
    ?LOG_USER_CMD("Stop Realtime Replication to all connected clusters", []),
    Remotes = riak_repl2_rt:enabled(),
    [ realtime_stop([{remote, Remote}], []) || Remote <- Remotes ];
realtime_stop(_, _) ->
    usage.

%%--------------------------
%% Command: realtime cascades enable
%%--------------------------
realtime_cascades_enable([], []) ->
    ?LOG_USER_CMD("Enable Realtime Replication cascading", []),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:rt_cascades_trans/2,
                                      always),
    text_out("Realtime cascades enabled.~n");
realtime_cascades_enable(_,_) ->
    usage.

%%--------------------------
%% Command: realtime cascades disable
%%--------------------------

realtime_cascades_disable([], []) ->
    ?LOG_USER_CMD("Disable Realtime Replication cascading", []),
    riak_core_ring_manager:ring_trans(fun riak_repl_ring:rt_cascades_trans/2,
                                      never),
    text_out("Realtime cascades disabled.~n");
realtime_cascades_disable(_,_) ->
    usage.

%%--------------------------
%% Command: realtime cascades show
%%--------------------------

realtime_cascades_show([], []) ->
    case app_helper:get_env(riak_repl, realtime_cascades, always) of
        always ->
            text_out("Realtime cascades are enabled.~n");
        never ->
            text_out("Realtime cascades are disabled.~n")
    end;
realtime_cascades_show(_, _) ->
    usage.


%%--------------------------
%% Command: fullsync enable
%%--------------------------

fullsync_enable([{remote, Remote}], []) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    ?LOG_USER_CMD("Enable Fullsync Replication to cluster ~p", [Remote]),
    riak_core_ring_manager:ring_trans(fun
                                          riak_repl_ring:fs_enable_trans/2, Remote),
    _ = riak_repl2_fscoordinator_sup:start_coord(Leader, Remote),
    text_out("Fullsync replication to cluster ~p enabled.", [Remote]);
fullsync_enable(_, _) ->
    usage.

%%--------------------------
%% Command: fullsync disable
%%--------------------------

fullsync_disable([{remote, Remote}], []) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    ?LOG_USER_CMD("Disable Fullsync Replication to cluster ~p", [Remote]),
    riak_core_ring_manager:ring_trans(fun
                                          riak_repl_ring:fs_disable_trans/2, Remote),
    _ = riak_repl2_fscoordinator_sup:stop_coord(Leader, Remote),
    text_out("Fullsync replication to cluster ~p disabled.", [Remote]);
fullsync_disable(_, _) ->
    usage.


%%--------------------------
%% Command: fullsync start
%%--------------------------

fullsync_start([{remote, Remote}], []) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    ?LOG_USER_CMD("Start Fullsync Replication to cluster ~p", [Remote]),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
    case proplists:get_value(Remote, Fullsyncs) of
        undefined ->
            %% io:format("Fullsync not enabled for cluster ~p~n", [Remote]),
            %% io:format("Use 'fullsync enable ~p' before start~n", [Remote]),
            %% {error, not_enabled};
            error_out("Fullsync not enabled for cluster ~p~n"
                      "Use 'fullsync enable ~p' before start~n", [Remote, Remote]);
        Pid ->
            riak_repl2_fscoordinator:start_fullsync(Pid),
            text_out("Fullsync replication to cluster ~p started.", [Remote])
    end;
fullsync_start([], [{all,_}]) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
    ?LOG_USER_CMD("Start Fullsync Replication to all connected clusters",[]),
    _ = [riak_repl2_fscoordinator:start_fullsync(Pid) || {_, Pid} <-
                                                             Fullsyncs],
    text("Fullsync replication started to all connected clusters.");
fullsync_start(_, _) ->
    usage.

%%--------------------------
%% Command: fullsync stop
%%--------------------------

fullsync_stop([{remote, Remote}], []) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    ?LOG_USER_CMD("Stop Fullsync Replication to cluster ~p", [Remote]),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
    case proplists:get_value(Remote, Fullsyncs) of
        undefined ->
            %% Fullsync is not enabled, but carry on quietly.
            error_out("Fullsync is not enabled for cluster ~p.", [Remote]);
        Pid ->
            riak_repl2_fscoordinator:stop_fullsync(Pid),
            text_out("Fullsync stopped for cluster ~p.", [Remote])
    end;
fullsync_stop([], [{all,_}]) ->
    Leader = riak_core_cluster_mgr:get_leader(),
    Fullsyncs = riak_repl2_fscoordinator_sup:started(Leader),
    ?LOG_USER_CMD("Stop Fullsync Replication to all connected clusters",[]),
    _ = [riak_repl2_fscoordinator:stop_fullsync(Pid) || {_, Pid} <-
                                                            Fullsyncs],
    text("Fullsync replication stopped to all connected clusters.");
fullsync_stop(_, _) ->
    usage.

%%--------------------------
%% Command: proxy-get enable
%%--------------------------

proxy_get_enable([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Enable Riak CS Proxy GET block provider for ~p",[Remote]),
    riak_core_ring_manager:ring_trans(fun
                                          riak_repl_ring:pg_enable_trans/2, Remote),
    text_out("Proxy-get to cluster ~s has been enabled.", [Remote]);
proxy_get_enable(_, _) ->
    usage.


%%--------------------------
%% Command: proxy-get disable
%%--------------------------

proxy_get_disable([{remote, Remote}], []) ->
    ?LOG_USER_CMD("Disable Riak CS Proxy GET block provider for ~p",[Remote]),
    riak_core_ring_manager:ring_trans(fun
                                          riak_repl_ring:pg_disable_trans/2, Remote),
    text_out("Proxy-get to cluster ~s has been disabled.", [Remote]);
proxy_get_disable(_, _) ->
    usage.

%%--------------------------
%% Command: proxy-get redirect cluster-id
%%--------------------------

proxy_get_redirect_cluster_id([], []) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ClusterId = lists:flatten(
                  io_lib:format("~p", [riak_core_ring:cluster_name(Ring)])),
    text_out("local cluster id: ~p~n", [ClusterId]);
proxy_get_redirect_cluster_id(_, _) ->
    usage.

%%--------------------------
%% Command: proxy-get redirect show
%%--------------------------

proxy_get_redirect_show([{from, FromClusterId}], []) ->
    case riak_core_metadata:get({<<"replication">>, <<"cluster-mapping">>}, FromClusterId) of
        undefined ->
            text_out("No mapping for ~p~n", [FromClusterId]);
        ToClusterId ->
            text_out("Cluster id ~p redirecting to cluster id ~p~n", [FromClusterId, ToClusterId])
    end;
proxy_get_redirect_show(_, _) ->
    usage.

%%--------------------------
%% Command: proxy-get redirect add
%%--------------------------

proxy_get_redirect_add([{to, _}=To, {from, _}=From], []) ->
    proxy_get_redirect_add([From, To], []);
proxy_get_redirect_add([{from, FromClusterId}, {to, ToClusterId}], []) ->
    lager:info("Redirecting cluster id: ~p to ~p", [FromClusterId, ToClusterId]),
    riak_core_metadata:put({<<"replication">>, <<"cluster-mapping">>},
                           FromClusterId, ToClusterId),
    text_out("Redirected proxy-get from cluster ~s to cluster ~s~n",
             [FromClusterId, ToClusterId]);
proxy_get_redirect_add(_, _) ->
    usage.


%%--------------------------
%% Command: proxy-get redirect delete
%%--------------------------

proxy_get_redirect_delete([{from, FromClusterId}], []) ->
    lager:info("Deleting redirect to ~p", [FromClusterId]),
    riak_core_metadata:delete({<<"replication">>, <<"cluster-mapping">>}, FromClusterId),
    text_out("Deleted proxy-get redirect from cluster ~s~n", [FromClusterId]);
proxy_get_redirect_delete(_, _) ->
    usage.
