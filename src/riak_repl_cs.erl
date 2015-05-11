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
    bool_to_ok_or_cancel(replicate_object(
                           riak_object:bucket(Object),
                           riak_kv_util:is_x_deleted(Object),
                           fullsync)).

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
    bool_to_ok_or_cancel(replicate_object(
                           riak_object:bucket(Object),
                           riak_kv_util:is_x_deleted(Object),
                           realtime)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

replicate_object(<<?BLOCK_BUCKET_PREFIX, _Rest/binary>>, IsTombstone, FSorRT) ->
    case {IsTombstone, FSorRT} of
        {false, fullsync} -> true;
        {false, realtime} -> false;
        {true, _} ->
            app_helper:get_env(riak_repl, replicate_block_tombstone, true)
    end;
replicate_object(_, true, _) -> false;
replicate_object(?STORAGE_BUCKET, _, _) -> false;
replicate_object(?ACCESS_BUCKET, _, _) -> false;
replicate_object(?USER_BUCKET, _, _) ->
    app_helper:get_env(riak_repl, replicate_cs_user_objects, true);
replicate_object(?BUCKETS_BUCKET, _, _) ->
    app_helper:get_env(riak_repl, replicate_cs_buckets_objects, true);
replicate_object(_, _, _) -> true.


-spec bool_to_ok_or_cancel(boolean()) -> ok | cancel.
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


do_repl_gc_object_realtime_test() ->
    Client = fake_client,
    Bucket = <<"riak-cs-gc">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))).

do_repl_gc_object_fullsync_test() ->
    Client = fake_client,
    Bucket = <<"riak-cs-gc">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).

do_repl_mb_weight_realtime_test() ->
    Client = fake_client,
    Bucket = <<"riak-cs-multibag">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send_realtime(Object, Client))).

do_repl_mb_weight_fullsync_test() ->
    Client = fake_client,
    Bucket = <<"riak-cs-multibag">>,
    Object = riak_object:new(Bucket, <<"key">>, <<"val">>),
    ?assert(ok_or_cancel_to_bool(send(Object, Client))).


%% ===================================================================
%% EUnit helpers
%% ===================================================================

ok_or_cancel_to_bool(ok) ->
    true;
ok_or_cancel_to_bool(cancel) ->
    false.

-endif.
