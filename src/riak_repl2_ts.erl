%% Riak EnterpriseDS
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_ts).

-export([postcommit/3, get_identity_hashes/1]).

-include("riak_repl.hrl").

%% Realtime replication hook for Timeseries. The basic ideas are
%% copied from `riak_repl2_rt'

postcommit({PK, Val}, Bucket, BucketProps) when not is_list(Val) ->
    postcommit({PK, [Val]}, Bucket, BucketProps);
postcommit(PartitionBatch, Bucket, BucketProps) ->
    maybe_postcommit(PartitionBatch, Bucket, proplists:get_value(repl, BucketProps, both)).

%% If `repl' is `false' or `fullsync', we skip realtime
maybe_postcommit(_PartitionBatch, _Bucket, false) ->
    ok;
maybe_postcommit(_PartitionBatch, _Bucket, fullsync) ->
    ok;
maybe_postcommit({PartIdx, Vals}=PartitionBatch, Bucket, _ReplProp) ->
    lager:debug("Timeseries batch sent to repl~n    PartIdx~p => ~p...", [PartIdx, hd(Vals)]),
    Meta = set_bucket_meta(Bucket),

    %% `w3' is the earliest wire protocol version to handle timeseries
    %% data properly
    BinObj = riak_repl_util:to_wire(w3, PartitionBatch),

    %% When a node starts shutting down, a process
    %% `riak_repl2_rtq_proxy' is registered to forward realtime
    %% updates to another node for delivery to the remote cluster. Try
    %% that first to avoid race conditions with unregister() during
    %% shutdown
    case whereis(riak_repl2_rtq_proxy) of
        undefined ->
            riak_repl2_rtq:push(length(Vals), BinObj, Meta);
        _ ->
            %% we're shutting down and repl is stopped or stopping...
            riak_repl2_rtq_proxy:push(length(Vals), BinObj, Meta)
    end.

%% Bucket properties will be checked at the remote cluster for
%% compatibility. Legacy ("default") buckets would be a single
%% argument to this instead of a tuple for bucket types, but we don't
%% support timeseries data in legacy buckets.
set_bucket_meta({Type, _}) ->
    %% By expressing an invalid bucket type property hash, we can
    %% force the remote cluster to reject this data unless it
    %% understands how to compare DDL hashes
    PropsHash = -1,

    M = orddict:new(),
    M1 = orddict:store(?BT_META_TYPED_BUCKET, true, M),
    M2 = orddict:store(?BT_META_TYPE, Type, M1),
    M3 = orddict:store(?BT_META_EXTRA_VALIDATION,
                       [{ts_ddl_hashes, get_identity_hashes(Type)}], M2),
    orddict:store(?BT_META_PROPS_HASH, PropsHash, M3).

get_identity_hashes(Table) ->
    Mod = riak_ql_ddl:make_module_name(Table),
    case catch Mod:get_identity_hashes() of
        {_, {undef, _}} ->
            %% Module doesn't know about identity hashes, doesn't
            %% exist, etc
            lager:warning("DDL module for ~p does not exist or is not "
                          "compiled with identity hashes", [Table]),
            [-1];
        HashList ->
            HashList
    end.
