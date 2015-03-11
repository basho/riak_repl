%% @doc Console commands for "Version 2" replication, aka
%% 'mode_repl12'.
-module(riak_repl_console12).
-include("riak_repl.hrl").

%% Points where riak_repl_console calls in.
-export([register_usage/0, dispatch/1, commands_usage/0]).

%% Commands
-export([add_listener/1, del_listener/1, add_nat_listener/1,
         add_site/1, del_site/1,
         start_fullsync/1, cancel_fullsync/1,
         pause_fullsync/1, resume_fullsync/1]).

-import(riak_repl_console,
        [register_usage/2, get_ring/0,
         maybe_set_ring/2]).

-define(USAGE,
        [{"add-listener", "add-listener <nodename> <ip> <port>"},
         {"add-nat-listener", "add-nat-listener <nodename> <internal_ip> <internal_port> <public_ip> <public_port>"},
         {"del-listener", "del-listener <nodename> <ip> <port>"},
         {"add-site", "add-site <ipaddr> <portnum> <sitename>"},
         {"del-site", "del-site <sitename>"},
         {"start-fullsync", "start-fullsync"},
         {"cancel-fullsync", "cancel-fullsync"},
         {"pause-fullsync", "pause-fullsync"},
         {"resume-fullsync", "resume-fullsync"}
        ]).

%% @doc Attempts to dispatch a legacy command, returning `nomatch' if
%% the command did not match by name or number of arguments. If the
%% last thing on the line is `-h' or `--help', `nomatch' is returned
%% so that usage can be printed.
-spec dispatch([string()]) ->  ok | error | nomatch.
dispatch(Cmd) ->
    case lists:last(Cmd) of
        "--help" -> nomatch;
        "-h"     -> nomatch;
        _ -> dispatch_internal(Cmd)
    end.

dispatch_internal(["add-listener"|[_,_,_]=Params]) ->
    add_listener(Params);
dispatch_internal(["add-nat-listener"|[_,_,_,_,_]=Params]) ->
    add_nat_listener(Params);
dispatch_internal(["del-listener"|[_,_,_]=Params]) ->
    del_listener(Params);
dispatch_internal(["add-site"|[_,_,_]=Params]) ->
    add_site(Params);
dispatch_internal(["del-site"|[_]=Params]) ->
    del_site(Params);
dispatch_internal(["start-fullsync"]) ->
    start_fullsync([]);
dispatch_internal(["cancel-fullsync"]) ->
    cancel_fullsync([]);
dispatch_internal(["pause-fullsync"]) ->
    pause_fullsync([]);
dispatch_internal(["resume-fullsync"]) ->
    resume_fullsync([]);
dispatch_internal(_) -> nomatch.

%% @doc List of implemented commands for this module, for printing out
%% at the top-level command.
-spec commands_usage() -> string().
commands_usage() ->
    "  Version 2 Commands:\n"
    "    add-listener                Add a sink listener\n"
    "    add-nat-listener            Add a sink listener with NAT\n"
    "    add-site                    Add a sink site\n"
    "    cancel-fullsync             Cancel running fullsync replication\n"
    "    del-listener                Delete a sink listener\n"
    "    del-site                    Delete a sink site\n"
    "    pause-fullsync              Pause running fullsync replication\n"
    "    resume-fullsync             Resume paused fullsync replication\n"
    "    start-fullsync              Start fullsync replication".

%% @doc Registers usage output with Clique.
register_usage() ->
    _ = [ true = register_usage([Cmd], [UsageStr, "\n\n", ?V2REPLDEP, "\n\n"]) ||
            {Cmd, UsageStr} <- ?USAGE ],
    ok.

add_listener(Params) ->
    warn_v2_repl(),
    Ring = get_ring(),
    case add_listener_internal(Ring,Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

warn_v2_repl() ->
    io:format(?V2REPLDEP++"~n~n", []),
    lager:warning(?V2REPLDEP, []).

add_nat_listener(Params) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    case add_nat_listener_internal(Ring, Params) of
        {ok, NewRing} ->
            ok = maybe_set_ring(Ring, NewRing);
        error -> error
    end.

add_listener_internal(Ring, [NodeName, IP, Port]) ->
    Listener = make_listener(NodeName, IP, Port),
    case lists:member(Listener#repl_listener.nodename, riak_core_ring:all_members(Ring)) of
        true ->
            case catch rpc:call(Listener#repl_listener.nodename,
                                riak_repl_util, valid_host_ip, [IP]) of
                true ->
                    NewRing = riak_repl_ring:add_listener(Ring, Listener),
                    {ok,NewRing};
                false ->
                    io:format("~p is not a valid IP address for ~p\n",
                              [IP, Listener#repl_listener.nodename]),
                    error;
                Error ->
                    io:format("Node ~p must be available to add listener: ~p\n",
                              [Listener#repl_listener.nodename, Error]),
                    error
            end;
        false ->
            io:format("~p is not a member of the cluster\n", [Listener#repl_listener.nodename]),
            error
    end.

add_nat_listener_internal(Ring, [NodeName, IP, Port, PublicIP, PublicPort]) ->
    case add_listener_internal(Ring, [NodeName, IP, Port]) of
        {ok,NewRing} ->
            case inet_parse:address(PublicIP) of
                {ok, _} ->
                    NatListener = make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort),
                    NewRing2 = riak_repl_ring:add_nat_listener(NewRing, NatListener),
                    {ok, NewRing2};
                {error, IPParseError} ->
                    io:format("Invalid NAT IP address: ~p~n", [IPParseError]),
                    error
            end;
        error ->
            io:format("Error adding nat address. ~n"),
            error
    end.

del_listener([NodeName, IP, Port]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    Listener = make_listener(NodeName, IP, Port),
    NewRing0 = riak_repl_ring:del_listener(Ring, Listener),
    NewRing = riak_repl_ring:del_nat_listener(NewRing0, Listener),
    ok = maybe_set_ring(Ring, NewRing).

add_site([IP, Port, SiteName]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    Site = make_site(SiteName, IP, Port),
    NewRing = riak_repl_ring:add_site(Ring, Site),
    ok = maybe_set_ring(Ring, NewRing).

del_site([SiteName]) ->
    lager:warning(?V2REPLDEP, []),
    Ring = get_ring(),
    NewRing = riak_repl_ring:del_site(Ring, SiteName),
    ok = maybe_set_ring(Ring, NewRing).

start_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:start_fullsync(Pid) ||
            Pid <- riak_repl_listener_sup:server_pids()],
    io:format("Fullsync started~n"),
    ok.

cancel_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:cancel_fullsync(Pid) ||
            Pid <- riak_repl_listener_sup:server_pids()],
    io:format("Fullsync canceled~n"),
    ok.

pause_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:pause_fullsync(Pid) ||
            Pid <- riak_repl_listener_sup:server_pids()],
    io:format("Fullsync paused~n"),
    ok.

resume_fullsync([]) ->
    lager:warning(?V2REPLDEP, []),
    _ = [riak_repl_tcp_server:resume_fullsync(Pid) ||
            Pid <- riak_repl_listener_sup:server_pids()],
    io:format("Fullsync resumed~n"),
    ok.

make_listener(NodeName, IP, Port) ->
    #repl_listener{nodename=list_to_atom(NodeName),
                   listen_addr={IP, list_to_integer(Port)}}.

make_nat_listener(NodeName, IP, Port, PublicIP, PublicPort) ->
    #nat_listener{nodename=list_to_atom(NodeName),
                  listen_addr={IP, list_to_integer(Port)},
                  nat_addr={PublicIP, list_to_integer(PublicPort)}}.

make_site(SiteName, IP, Port) ->
    #repl_site{name=SiteName, addrs=[{IP, list_to_integer(Port)}]}.
