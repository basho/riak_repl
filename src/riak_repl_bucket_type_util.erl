%% Riak EnterpriseDS
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_bucket_type_util).

%% @doc Utility functions for interacting with bucket types

-export([bucket_props_match/1,
         bucket_props_match/2,
         is_bucket_typed/1,
         prop_get/3,
         property_hash/1]).

-include("riak_repl.hrl").

-define(DEFAULT_BUCKET_TYPE, <<"default">>).

-spec bucket_props_match(proplists:proplist()) -> boolean().
bucket_props_match(Props) ->
    case is_bucket_typed(Props) of
        true ->
            Type = prop_get(?BT_META_TYPE, ?DEFAULT_BUCKET_TYPE, Props),
            RemoteHash = prop_get(?BT_META_PROPS_HASH, undefined, Props),
            Extra = prop_get(?BT_META_EXTRA_VALIDATION, [], Props),
            compare_bucket_types(Type, RemoteHash, Extra);
        false ->
            %% This is not a typed bucket. Check if the remote
            %% side is also untyped.
            undefined =:= prop_get(?BT_META_PROPS_HASH, undefined, Props)
    end.

%% If the source cluster gave us an invalid hash value (-1) this means
%% that we need to perform comparisons other than the legacy 2.0
%% static bucket type property hash
compare_bucket_types(Type, ?INVALID_BT_HASH, Extra) ->
    lists:filtermap(fun(P) -> test_prop_extra(P, Type) end, Extra) /= [];
compare_bucket_types(Type, RemoteHash, _Extra) ->
    property_hash(Type)  =:= RemoteHash.

test_prop_extra({ts_ddl_hashes, RemoteHashes}, Type) ->
    LocalHashes = riak_repl2_ts:get_identity_hashes(Type),
    %% We just need one hash out of each list to match
    any_item_matches(RemoteHashes, LocalHashes);
test_prop_extra(UnknownProp, Type) ->
    lager:info("repl does not know what to do to compare ~p for ~p",
               [UnknownProp, Type]),
    %% We don't know what to do with unknown properties, so to be safe
    %% default to a failed comparison
    false.

any_item_matches(L1, L2) ->
    not ordsets:is_disjoint(ordsets:from_list(L1), ordsets:from_list(L2)).

-spec bucket_props_match(binary(), integer()) -> boolean().
bucket_props_match(Type, RemoteBucketTypeHash) ->
   property_hash(Type) =:= RemoteBucketTypeHash.

-spec is_bucket_typed({error, no_type} | proplists:proplist()) -> boolean().
is_bucket_typed({error, no_type}) ->
    false;
is_bucket_typed(Props) ->
    prop_get(?BT_META_TYPED_BUCKET, false, Props).

-spec prop_get(atom() | binary(), term(), {error, no_type} | proplists:proplist()) -> term().
prop_get(_Key, Default, {error, no_type}) ->
    Default;
prop_get(Key, Default, Props) ->
    case lists:keyfind(Key, 1, Props) of
        {Key, Value} ->
            Value;
        false ->
            Default
    end.

-spec property_hash(binary()) -> undefined | integer().
property_hash(undefined) ->
    undefined;
property_hash(Type) ->
    Defaults = riak_core_capability:get(
            {riak_repl, default_bucket_props_hash},
            [consistent, datatype, n_val, allow_mult, last_write_wins]),
    riak_core_bucket_type:property_hash(Type, Defaults).
