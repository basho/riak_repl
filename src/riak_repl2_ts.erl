%% Riak EnterpriseDS
%% Copyright (c) 2016 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_ts).

-export([postcommit/2]).

-include("riak_repl.hrl").

%% Realtime replication hook for Timeseries
postcommit({PK, Vals}=PartitionBatch, Bucket) when is_list(Vals) ->
    lager:debug("Timeseries batch sent to repl~n    PartIdx~p => ~p...", [PK, hd(Vals)]),
    Meta = set_bucket_meta(Bucket),
    BinObj = riak_repl_util:to_wire(w3, {Bucket, PartitionBatch}),

    %% try the proxy first, avoids race conditions with unregister()
    %% during shutdown
    case whereis(riak_repl2_rtq_proxy) of
        undefined ->
            riak_repl2_rtq:push(length(Vals), BinObj, Meta);
        _ ->
            %% we're shutting down and repl is stopped or stopping...
            riak_repl2_rtq_proxy:push(length(Vals), BinObj, Meta)
    end;
postcommit({PK, Val}=PartitionRecord, Bucket) ->
    lager:debug("Timeseries object sent to repl~n    PartIdx~p => ~p", [PK, Val]),

    Meta = set_bucket_meta(Bucket),
    BinObj = riak_repl_util:to_wire(w3, PartitionRecord),

    %% try the proxy first, avoids race conditions with unregister()
    %% during shutdown
    case whereis(riak_repl2_rtq_proxy) of
        undefined ->
            riak_repl2_rtq:push(1, BinObj, Meta);
        _ ->
            %% we're shutting down and repl is stopped or stopping...
            riak_repl2_rtq_proxy:push(1, BinObj, Meta)
    end.

set_bucket_meta({Type, _}) ->
    M = orddict:new(),
    PropsHash = riak_repl_bucket_type_util:property_hash(Type),
    M1 = orddict:store(?BT_META_TYPED_BUCKET, true, M),
    M2 = orddict:store(?BT_META_TYPE, Type, M1),
    orddict:store(?BT_META_PROPS_HASH, PropsHash, M2).
