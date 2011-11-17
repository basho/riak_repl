%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_util).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([make_peer_info/0,
         validate_peer_info/2,
         capability_from_vsn/1,
         get_partitions/1,
         do_repl_put/1,
         site_root_dir/1,
         ensure_site_dir/1,
         binpack_bkey/1,
         binunpack_bkey/1,
         merkle_filename/3,
         keylist_filename/3,
         valid_host_ip/1,
         format_socketaddrs/1,
         choose_strategy/2,
         strategy_module/2,
         configure_socket/1,
         repl_helper_send/1]).

make_peer_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    SafeRing = riak_core_ring:downgrade(1, Ring),
    {ok, RiakVSN} = application:get_key(riak_kv, vsn),
    {ok, ReplVSN} = application:get_key(riak_repl, vsn),
    #peer_info{riak_version=RiakVSN, repl_version=ReplVSN, ring=SafeRing}.

validate_peer_info(T=#peer_info{}, M=#peer_info{}) ->
    TheirPartitions = get_partitions(T#peer_info.ring),
    OurPartitions = get_partitions(M#peer_info.ring),
    TheirPartitions =:= OurPartitions.

%% Build a default capability from the version information in #peerinfo{}
capability_from_vsn(#peer_info{repl_version = ReplVsnStr}) ->
    ReplVsn = parse_vsn(ReplVsnStr),
    case ReplVsn >= {0, 14, 0} of
        true ->
            [bounded_queue];
        false ->
            []
    end.

get_partitions(_Ring) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lists:sort([P || {P, _} <- riak_core_ring:all_owners(Ring)]).

do_repl_put(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    case repl_helper_recv(Object) of
        ok ->
            ReqId = erlang:phash2(erlang:now()),
            B = riak_object:bucket(Object),
            K = riak_object:key(Object),
            Opts = [asis, disable_hooks, {update_last_modified, false}],

            riak_kv_put_fsm_sup:start_put_fsm(node(), [ReqId, Object, 1, 1,
                    ?REPL_FSM_TIMEOUT,
                    self(), Opts]),

            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted obj ~p/~p", [B, K]),
                    reap(ReqId, B, K);
                false ->
                    lager:debug("Incoming obj ~p/~p", [B, K])
            end;
        cancel ->
            lager:debug("Skipping repl received object ~p/~p", [B, K])
    end.

reap(ReqId, B, K) ->
    riak_kv_get_fsm_sup:start_get_fsm(node(),
                                      [ReqId, B, K, 1, ?REPL_FSM_TIMEOUT,
                                       self()]).

repl_helper_recv(Object) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> ok;
        {ok, Mods} ->
            lager:debug("repl recv helpers: ~p", [Mods]),
            repl_helper_recv(Mods, Object)
    end.

repl_helper_recv([], _O) ->
    ok;
repl_helper_recv([{App, Mod}|T], Object) ->
    try Mod:recv(Object) of
        ok ->
            repl_helper_recv(T, Object);
        cancel ->
            cancel;
        Other ->
            lager:error("Unexpected result running repl recv helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_recv(T, Object)
    catch
        What:Why ->
            lager:error("Crash while running repl recv helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_recv(T, Object)
    end.

repl_helper_send(Object) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> [];
        {ok, Mods} ->
            lager:debug("repl send helpers: ~p", [Mods]),
            repl_helper_send(Mods, Object, [])
    end.

repl_helper_send([], _O, Acc) ->
    Acc;
repl_helper_send([{App, Mod}|T], Object, Acc) ->
    try Mod:send(Object) of
        Objects when is_list(Objects) ->
            repl_helper_send(T, Object, Objects ++ Acc);
        ok ->
            repl_helper_send(T, Object, Acc);
        cancel ->
            cancel;
         Other ->
            lager:error("Unexpected result running repl send helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_send(T, Object, Acc)
    catch
        What:Why ->
            lager:error("Crash while running repl send helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_send(T, Object, Acc)
    end.

site_root_dir(Site) ->
    {ok, DataRootDir} = application:get_env(riak_repl, data_root),
    SitesRootDir = filename:join([DataRootDir, "sites"]),
    filename:join([SitesRootDir, Site]).

ensure_site_dir(Site) ->
    ok = filelib:ensure_dir(
           filename:join([riak_repl_util:site_root_dir(Site), ".empty"])).

binpack_bkey({B, K}) ->
    SB = size(B),
    SK = size(K),
    <<SB:32/integer, B/binary, SK:32/integer, K/binary>>.

binunpack_bkey(<<SB:32/integer,B:SB/binary,SK:32/integer,K:SK/binary>>) -> 
    {B,K}.


merkle_filename(WorkDir, Partition, Type) ->
    case Type of
        ours ->
            Ext=".merkle";
        theirs ->
            Ext=".theirs"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

keylist_filename(WorkDir, Partition, Type) ->
    case Type of
        ours ->
            Ext=".ours.sterm";
        theirs ->
            Ext=".theirs.sterm"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

%% Returns true if the IP address given is a valid host IP address            
valid_host_ip(IP) ->     
    {ok, IFs} = inet:getif(),
    {ok, NormIP} = normalize_ip(IP),
    ValidIPs = [ValidIP || {ValidIP, _, _} <- IFs],
    lists:member(NormIP, ValidIPs).

%% Convert IP address the tuple form
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when is_tuple(IP) ->
    {ok, IP}.

format_socketaddrs(Socket) ->
    {ok, {LocalIP, LocalPort}} = inet:sockname(Socket),
    {ok, {RemoteIP, RemotePort}} = inet:peername(Socket),
    lists:flatten(io_lib:format("~s:~p-~s:~p", [inet_parse:ntoa(LocalIP),
                                                LocalPort,
                                                inet_parse:ntoa(RemoteIP),
                                                RemotePort])).

choose_strategy(ServerStrats, ClientStrats) ->
    %% find the first common strategy in both lists
    CalcPref = fun(E, Acc) ->
            Index = length(Acc) + 1,
            Preference = math:pow(2, Index),
            [{E, Preference}|Acc]
    end,
    LocalPref = lists:foldl(CalcPref, [], ServerStrats),
    RemotePref = lists:foldl(CalcPref, [], ClientStrats),
    %% sum the common strategy preferences together
    TotalPref = [{S, Pref1+Pref2} || {S, Pref1} <- LocalPref, {RS, Pref2} <- RemotePref, S == RS],
    case TotalPref of
        [] ->
            %% no common strategies, force legacy
            ?LEGACY_STRATEGY;
        _ ->
            %% sort and return the first one
            element(1, hd(lists:keysort(2, TotalPref)))
    end.

strategy_module(Strategy, server) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_server"]));
strategy_module(Strategy, client) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_client"])).

%% set some common socket options, based on appenv
configure_socket(Socket) ->
    RB = case app_helper:get_env(riak_repl, recbuf) of
        RecBuf when is_integer(RecBuf), RecBuf > 0 ->
            [{recbuf, RecBuf}];
        _ ->
            []
    end,
    SB = case app_helper:get_env(riak_repl, sndbuf) of
        SndBuf when is_integer(SndBuf), SndBuf > 0 ->
            [{sndbuf, SndBuf}];
        _ ->
            []
    end,

    SockOpts = RB ++ SB,
    case SockOpts of
        [] ->
            ok;
        _ ->
            inet:setopts(Socket, SockOpts)
    end.

%% Parse the version into major, minor, micro digits, ignoring any release
%% candidate suffix
parse_vsn(Str) ->
    Toks = string:tokens(Str, "."),
    Vsns = [begin
                {I,_R} = string:to_integer(T),
                I
            end || T <- Toks],
    list_to_tuple(Vsns).

