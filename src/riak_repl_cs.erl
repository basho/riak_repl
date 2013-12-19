%% Copyright (c) 2012 Basho Technologies, Inc.
%% This repl hook skips Riak CS block buckets

-module(riak_repl_cs).

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([send_realtime/2, send/2, recv/1]).

-define(BLOCK_BUCKET_PREFIX, "0b:").
-define(USER_BUCKET, <<"moss.users">>).
-define(ACCESS_BUCKET, <<"moss.access">>).
-define(STORAGE_BUCKET, <<"moss.storage">>).
-define(BUCKETS_BUCKET, <<"moss.buckets">>).

%% For fullsync, we don't want to ever replicate tombstones or blocks or
%% storage or access.
%% Depending on app.config, we may or may not want to replicate
%% user and bucket objects.
-spec send(riak_object:riak_object(), riak_client:riak_client()) ->
    ok | cancel.
send(Object, _RiakClient) ->
    bool_to_ok_or_cancel(not skip_common_object(Object)).

-spec recv(riak_object:riak_object()) -> ok | cancel.
recv(_Object) ->
    ok.

%% For realtime, we don't want to ever replicate tombstones or
%% storage or access.
%% Depending on app.config, we may or may not want to replicate
%% user and bucket objects.
-spec send_realtime(riak_object:riak_object(), riak_client:riak_client()) ->
    ok | cancel.
send_realtime(Object, _RiakClient) ->
    Skip = skip_common_object(Object) orelse
           skip_block_object(Object),
    bool_to_ok_or_cancel(not Skip).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec skip_common_object(riak_object:riak_object()) -> boolean().
skip_common_object(Object) ->
    riak_kv_util:is_x_deleted(Object) orelse
    skip_common_bucket(riak_object:bucket(Object)).

-spec skip_common_bucket(binary()) -> boolean().
skip_common_bucket(Bucket) ->
    storage_bucket(Bucket) orelse
    access_bucket(Bucket) orelse
    skip_user_bucket(Bucket) orelse
    skip_buckets_bucket(Bucket).

-spec skip_block_object(riak_object:riak_object()) -> boolean().
skip_block_object(Object) ->
    Bucket = riak_object:bucket(Object),
    block_bucket(Bucket).

-spec block_bucket(binary()) -> boolean().
block_bucket(<<?BLOCK_BUCKET_PREFIX, _Rest/binary>>) ->
    true;
block_bucket(_Bucket) ->
    false.

-spec storage_bucket(binary()) -> boolean().
storage_bucket(?STORAGE_BUCKET) ->
    true;
storage_bucket(_Bucket) ->
    false.

-spec access_bucket(binary()) -> boolean().
access_bucket(?ACCESS_BUCKET) ->
    true;
access_bucket(_Bucket) ->
    false.

-spec skip_user_bucket(binary()) -> boolean().
skip_user_bucket(Bucket) ->
    ReplicateUsers = app_helper:get_env(riak_repl, replicate_cs_user_objects,
                       true),
    handle_should_replicate_users_bucket(ReplicateUsers, Bucket).

-spec handle_should_replicate_users_bucket(boolean(), binary()) ->
    boolean().
handle_should_replicate_users_bucket(true, _Bucket) ->
    %% if we _should_ replicate user objects (the true pattern above),
    %% then we don't need to even check if this is a user object
    false;
handle_should_replicate_users_bucket(false, Bucket) ->
    user_bucket(Bucket).

-spec skip_buckets_bucket(binary()) -> boolean().
skip_buckets_bucket(Bucket) ->
    ReplicateBuckets = app_helper:get_env(riak_repl, replicate_cs_bucket_objects,
                       true),
    handle_should_replicate_buckets_bucket(ReplicateBuckets, Bucket).

-spec handle_should_replicate_buckets_bucket(boolean(), binary()) ->
    boolean().
handle_should_replicate_buckets_bucket(true, _Bucket) ->
    %% if we _should_ replicate bucket objects (the true pattern above),
    %% then we don't need to even check if this is a bucket object
    false;
handle_should_replicate_buckets_bucket(false, Bucket) ->
    buckets_bucket(Bucket).

%% @private
%% @doc Should this bucket name be treated as being the users bucket
%% for the sake of replication.
-spec user_bucket(binary()) -> boolean().
user_bucket(?USER_BUCKET) ->
    true;
user_bucket(_Bucket) ->
    false.

-spec buckets_bucket(binary()) -> boolean().
buckets_bucket(?BUCKETS_BUCKET) ->
    true;
buckets_bucket(_Bucket) ->
    false.

-spec bool_to_ok_or_cancel(boolean) -> ok | cancel.
bool_to_ok_or_cancel(true) ->
    ok;
bool_to_ok_or_cancel(false) ->
    cancel.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

do_repl_blocks_fullsync_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"0b:foo">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).

dont_repl_blocks_realtime_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"0b:foo">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_access_objects_fullsync_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.access">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))).

dont_repl_access_objects_realtime_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.access">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_storage_objects_fullsync_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.storage">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))).

dont_repl_storage_objects_realtime_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.storage">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_tombstoned_object_fullsync_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"anything">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assertNot(ok_or_cancel_to_bool(send(Object3, Client))).

dont_repl_tombstoned_object_realtime_test() ->
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"anything">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    M = dict:from_list([{<<"X-Riak-Deleted">>, true}]),
    Object2 = riak_object:update_metadata(Object, M),
    Object3 = riak_object:apply_updates(Object2),
    ?assertNot(ok_or_cancel_to_bool(send(Object3, Client))).

do_repl_user_object_fullsync_test() ->
    application:set_env(riak_repl, replicate_cs_user_objects, true),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.users">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).

dont_repl_user_object_fullsync_test() ->
    application:set_env(riak_repl, replicate_cs_user_objects, false),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.users">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))).

do_repl_user_object_realtime_test() ->
    application:set_env(riak_repl, replicate_cs_user_objects, true),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.users">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_user_object_realtime_test() ->
    application:set_env(riak_repl, replicate_cs_user_objects, false),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.users">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

do_repl_bucket_object_fullsync_test() ->
    application:set_env(riak_repl, replicate_cs_bucket_objects, true),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.buckets">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).

dont_repl_bucket_object_fullsync_test() ->
    application:set_env(riak_repl, replicate_cs_bucket_objects, false),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.buckets">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send(Object, Client))).

do_repl_bucket_object_realtime_test() ->
    application:set_env(riak_repl, replicate_cs_bucket_objects, true),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.buckets">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))).

dont_repl_bucket_object_realtime_test() ->
    application:set_env(riak_repl, replicate_cs_bucket_objects, false),
    %% the riak client isn't even used
    Client = fake_client,
    Bucket = <<"moss.buckets">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assertNot(ok_or_cancel_to_bool(send_realtime(Object, Client))).

%% ===================================================================
%% EUnit helpers
%% ===================================================================

ok_or_cancel_to_bool(ok) ->
    true;
ok_or_cancel_to_bool(cancel) ->
    false.

-endif.
