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
         format_socketaddrs/1]).

make_peer_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, RiakVSN} = application:get_key(riak_kv, vsn),
    {ok, ReplVSN} = application:get_key(riak_repl, vsn),
    #peer_info{riak_version=RiakVSN, repl_version=ReplVSN, ring=Ring}.

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
    ReqId = erlang:phash2(erlang:now()),
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    Opts = [disable_hooks, {update_last_modified, false}],

    case B of
        <<"_rsid_", Idx/binary>> ->
            {ok, C} = riak:local_client(),
            SC = riak_search_client:new(C),
            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted proxy obj ~p/~p", [B, K]),
                    riak_indexed_doc:remove_entries(C, SC, Idx, K),
                    riak_kv_put_fsm_sup:start_put_fsm(node(),
                                                      [ReqId, Object, 1, 1,
                                                       ?REPL_FSM_TIMEOUT,
                                                       self(), Opts]),
                    reap(ReqId, B, K);
                false ->
                    lager:debug("Incoming proxy obj ~p/~p", [B, K]),
                    IdxDoc = riak_object:get_value(Object),
                    riak_indexed_doc:remove_entries(C, SC, Idx, K),
                    Postings = riak_indexed_doc:postings(IdxDoc),
                    SC:index_terms(Postings),
                    riak_kv_put_fsm_sup:start_put_fsm(node(),
                                                      [ReqId, Object, 1, 1,
                                                       ?REPL_FSM_TIMEOUT, self(),
                                                       Opts])
            end;
        _ ->
            riak_kv_put_fsm_sup:start_put_fsm(node(),
                                              [ReqId, Object, 1, 1,
                                               ?REPL_FSM_TIMEOUT, self(),
                                               Opts]),
            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted KV obj ~p/~p", [B, K]),
                    reap(ReqId, B, K);
                false ->
                    lager:debug("Incoming KV obj ~p/~p", [B, K]),
                    ok
            end
    end.

reap(ReqId, B, K) ->
    riak_kv_get_fsm_sup:start_get_fsm(node(),
                                      [ReqId, B, K, 1, ?REPL_FSM_TIMEOUT,
                                       self()]).

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

%% Parse the version into major, minor, micro digits, ignoring any release
%% candidate suffix
parse_vsn(Str) ->
    Toks = string:tokens(Str, "."),
    Vsns = [begin
                {I,_R} = string:to_integer(T),
                I
            end || T <- Toks],
    list_to_tuple(Vsns).

