%% Copyright (c) 2012 Basho Technologies, Inc.
%% This repl hook skips Riak CS block buckets

-module(riak_repl_cs).
-export([send_realtime/2, send/2, recv/1]).

-define(OBJECT_BUCKET_PREFIX, "0o:">).
-define(BLOCK_BUCKET_PREFIX, "0b:").

send(_Object, _RiakClient) ->
    ok.

recv (_Object) ->
    ok.

send_realtime(Object, _RiakClient) ->
    Bucket = riak_object:bucket(Object),
    check_cs(Bucket).

check_cs(<< ?BLOCK_BUCKET_PREFIX, _Rest/binary>>) ->
    cancel;
check_cs(_Object) ->
    ok.
