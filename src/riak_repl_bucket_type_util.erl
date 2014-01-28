%% Riak EnterpriseDS
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_bucket_type_util).

%% @doc Utility functions for interacting with bucket types

-export([bucket_props_match/1,
         bucket_props_match/2,
         get_bucket_type_props/1,
         is_bucket_typed/1,
         prop_get/3]).

-include("riak_repl.hrl").

-define(BUCKET_TYPES_PROPS, [consistent, datatype, n_val, allow_mult, last_write_wins]).
-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-spec bucket_props_match(proplists:proplist()) -> boolean().
bucket_props_match(Props) ->
    case is_bucket_typed(Props) of
        true ->
            BucketType = prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Props),
            LocalTypeHash = riak_core_bucket_type:property_hash(get_bucket_type_props(BucketType)),
            LocalTypeHash =:= prop_get(?BT_META_PROPS_HASH, undefined, Props);
        false ->
            %% This is not a typed bucket. Check if the remote
            %% side is also untyped.
            undefined =:= prop_get(?BT_META_PROPS_HASH, undefined, Props)
    end.

-spec bucket_props_match(riak_object:type(), [integer()]) -> boolean().
bucket_props_match(Type, []) ->
    LocalTypeHash = riak_core_bucket_type:property_hash(get_bucket_type_props(Type)),
    LocalTypeHash =:= undefined;
bucket_props_match(Type, [RemoteBucketTypeHash]) ->
    LocalTypeHash = riak_core_bucket_type:property_hash(get_bucket_type_props(Type)),
    LocalTypeHash =:= RemoteBucketTypeHash;
bucket_props_match(_Type, _HashList) ->
    %% Do not replicate if the hash list from the riak_object contains
    %% more than 1 value because the local bucket type hash can never
    %% match more than a single value. Must wait for the object to be
    %% resolved and updated on the source side before replicating.
    false.

-spec get_bucket_type_props(undefined | {error, no_type} | binary()) ->
                                   proplists:proplist() | undefined.
get_bucket_type_props(undefined) ->
    undefined;
get_bucket_type_props({error, no_type}) ->
    undefined;
get_bucket_type_props(BucketType) ->
    case riak_core_bucket_type:get(BucketType) of
        undefined ->
            undefined;
        {error, _T} ->
            undefined;
        Props ->
            Props
    end.

-spec is_bucket_typed({error, no_type} | proplists:proplist()) -> boolean().
is_bucket_typed({error, no_type}) ->
    false;
is_bucket_typed(Props) ->
    lager:info("Props: ~p", [Props]),
    prop_get(?BT_META_TYPED_BUCKET, false, Props).

-spec prop_get(binary(), term(), {error, no_type} | proplists:proplist()) -> term().
prop_get(_Key, Default, {error, no_type}) ->
    Default;
prop_get(Key, Default, Props) ->
    case lists:keyfind(Key, 1, Props) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.
