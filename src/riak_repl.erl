%% Riak EnterpriseDS
%% Copyright 2007-2009 Basho Technologies, Inc. All Rights Reserved.
-module(riak_repl).
-author('Andy Gross <andy@basho.com>').
-include("riak_repl.hrl").
-export([start/0, stop/0]).
-export([install_hook/0]).
-export([fixup/2]).

start() ->
    riak_core_util:start_app_deps(riak_repl),
    application:start(riak_repl).

%% @spec stop() -> ok
stop() -> 
    application:stop(riak_repl).

install_hook() ->
    riak_core_bucket:append_bucket_defaults([{repl, true}]),
    ok.

fixup(_Bucket, BucketProps) ->
    CleanPostcommit = strip_postcommit(BucketProps),
    case proplists:get_value(repl, BucketProps) of
        true ->
            UpdPostcommit = CleanPostcommit ++ [?REPL_HOOK],

            {ok, lists:keystore(postcommit, 1, BucketProps, 
                    {postcommit, UpdPostcommit})};
        _ ->
            %% Update the bucket properties
            UpdBucketProps = lists:keystore(postcommit, 1, BucketProps, 
                {postcommit, CleanPostcommit}),
            {ok, UpdBucketProps}
    end.

%% Get the postcommit hook from the bucket and strip any
%% existing repl hooks.
strip_postcommit(BucketProps) ->
    %% Get the current postcommit hook
    case proplists:get_value(postcommit, BucketProps, []) of
        X when is_list(X) ->
            CurrentPostcommit=X;
        {struct, _}=X ->
            CurrentPostcommit=[X]
    end,
    
    %% Add repl hook - make sure there are not duplicate entries
    CurrentPostcommit -- [?REPL_HOOK].

