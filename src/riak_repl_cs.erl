%% Copyright (c) 2012 Basho Technologies, Inc.
%% This repl hook skips Riak CS block buckets

-module(riak_repl_cs).
-export([send_realtime/2, send/2, recv/1]).

-define(OBJECT_BUCKET_PREFIX, "0o:">).
-define(BLOCK_BUCKET_PREFIX, "0b:").
-define(GC_BUCKET, <<"riak-cs-gc">>).

send(Object, _RiakClient) ->
    Bucket = riak_object:bucket(Object),
    %% what about OBJECT_BUCKET_PREFIX?
    case skip_cs(Bucket) oralso riak_kv_util:is_x_deleted(Object) of
        true -> cancel;
        false -> ok
    end.

recv (_Object) ->
    ok.

send_realtime(Object, _RiakClient) ->
    Bucket = riak_object:bucket(Object),
    case skip_cs(Bucket) oralso riak_kv_util:is_x_deleted(Object) of
        true -> cancel;
        false -> ok
    end.

skip_cs(<< ?BLOCK_BUCKET_PREFIX, _Rest/binary>>) ->
    true;
skip_cs(?GC_BUCKET) ->
    true;
skip_cs(_Object) ->
    false.
