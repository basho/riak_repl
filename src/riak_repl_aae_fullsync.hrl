-define(MSG_INIT, 1).
-define(MSG_LOCK_TREE, 2).
-define(MSG_UPDATE_TREE, 3).
-define(MSG_GET_AAE_BUCKET, 4).
-define(MSG_GET_AAE_SEGMENT, 5).
-define(MSG_REPLY, 6).
-define(MSG_PUT_OBJ, 7).
-define(MSG_GET_OBJ, 8).
-define(MSG_COMPLETE, 9).
-define(MSG_GET_AAE_BUCKETS, 10).
-define(AAE_FULLSYNC_REPLY_TIMEOUT, 60000). %% 1 minute to hear from remote sink
