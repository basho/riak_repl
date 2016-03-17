%% Riak EnterpriseDS
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_ts).

-export([postcommit/3]).

-include("riak_repl.hrl").

%% Realtime replication hook for Timeseries
postcommit({PK, Val}, Bucket, BucketProps) when not is_list(Val) ->
    postcommit({PK, [Val]}, Bucket, BucketProps);
postcommit(PartitionBatch, Bucket, BucketProps) ->
    maybe_postcommit(PartitionBatch, Bucket, proplists:get_value(repl, BucketProps, both)).

maybe_postcommit(_PartitionBatch, _Bucket, NoRT) when NoRT == false orelse
                                                      NoRT == fullsync ->
    ok;
maybe_postcommit({PK, Vals}=PartitionBatch, Bucket, _RT) ->
    lager:debug("Timeseries batch sent to repl~n    PartIdx~p => ~p...", [PK, hd(Vals)]),
    Meta = set_bucket_meta(Bucket),
    BinObj = riak_repl_util:to_wire(w3, PartitionBatch),

    %% try the proxy first, avoids race conditions with unregister()
    %% during shutdown
    case whereis(riak_repl2_rtq_proxy) of
        undefined ->
            riak_repl2_rtq:push(length(Vals), BinObj, Meta);
        _ ->
            %% we're shutting down and repl is stopped or stopping...
            riak_repl2_rtq_proxy:push(length(Vals), BinObj, Meta)
    end.

set_bucket_meta({Type, _}) ->
    M = orddict:new(),
    PropsHash = riak_repl_bucket_type_util:property_hash(Type),
    M1 = orddict:store(?BT_META_TYPED_BUCKET, true, M),
    M2 = orddict:store(?BT_META_TYPE, Type, M1),
    orddict:store(?BT_META_PROPS_HASH, PropsHash, M2).
