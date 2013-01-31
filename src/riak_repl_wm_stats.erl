%% -------------------------------------------------------------------
%%
%% riak_repl_wm_stats: publishing Riak replication stats via HTTP
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_repl_wm_stats).

-export([
         init/1,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         produce_body/2,
         pretty_print/2,
         jsonify_stats/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(ctx, {}).

init(_) ->
    {ok, #ctx{}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(ReqData, Context) ->
    case wrq:method(ReqData) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], ReqData, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], ReqData, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(ReqData, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     ReqData, Context}.

service_available(ReqData, Ctx) ->
    {true, ReqData, Ctx}.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

produce_body(ReqData, Ctx) ->
    Body = mochijson2:encode({struct,
                              get_stats()
                              }),
    {Body, ReqData, Ctx}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {json_pp:print(binary_to_list(list_to_binary(Json))), RD2, C2}.

get_stats() ->
    RTRemotesStatus = riak_repl_console:rt_remotes_status(),
    FSRemotesStatus = riak_repl_console:fs_remotes_status(),
    Stats1 = riak_repl_stats:get_stats(),
    CMStats = riak_repl_console:cluster_mgr_stats(),
    LeaderStats = riak_repl_console:leader_stats(),
    Servers = riak_repl_console:server_stats(),
    Clients = riak_repl_console:client_stats(),
    Coord = riak_repl_console:coordinator_stats(),
    CoordSrv = riak_repl_console:coordinator_srv_stats(),
    RTQ = [{realtime_queue_stats, riak_repl2_rtq:status()}],
    jsonify_stats(RTRemotesStatus,[]) ++ jsonify_stats(FSRemotesStatus,[]) ++ CMStats ++ Stats1 ++ LeaderStats
        ++ jsonify_stats(Clients, [])
        ++ jsonify_stats(Servers, [])
    ++ RTQ
    ++ jsonify_stats(Coord,[])
    ++ jsonify_stats(CoordSrv,[]).

%%format_stats(Type, [], Acc) ->
%%    [{Type, lists:reverse(Acc)}];
%%format_stats(Type, [{P, M, {status, S}}|T], Acc) ->
%%    format_stats(Type, T, [[{pid, list_to_binary(erlang:pid_to_list(P))},
%%                            M, {status, jsonify_stats(S, [])}]|Acc]).

format_pid(Pid) ->
    list_to_binary(riak_repl_util:safe_pid_to_list(Pid)).

format_pid_stat({Name, Value}) when is_pid(Value) ->
    {Name, format_pid(Value)};
format_pid_stat(Pair) ->
    Pair.

jsonify_stats([], Acc) ->
    lists:flatten(lists:reverse(Acc));

jsonify_stats([{fullsync, Num, _Left}|T], Acc) ->
    jsonify_stats(T, [{"partitions_left", Num} | Acc]);

jsonify_stats([{S,IP,Port}|T], Acc) when is_atom(S) andalso is_list(IP) andalso is_integer(Port) ->
    jsonify_stats(T, [{S,
                       list_to_binary(IP++":"++integer_to_list(Port))}|Acc]);
jsonify_stats([{K,V}|T], Acc) when is_pid(V) ->
    jsonify_stats(T, [{K,list_to_binary(riak_repl_util:safe_pid_to_list(V))}|Acc]);

jsonify_stats([{K,V=[{_,_}|_Tl]}|T], Acc) when is_list(V) ->
    NewV = jsonify_stats(V,[]),
    jsonify_stats(T, [{K,NewV}|Acc]);

jsonify_stats([{K,V}|T], Acc) when is_atom(K)
        andalso (K == server_stats orelse K == client_stats) ->
    case V of
        [{Pid, Mq, {status, Stats}}] ->
            FormattedPid = format_pid(Pid),
            NewStats = {status, lists:map(fun format_pid_stat/1, Stats)},
            ServerPid = [{server_pid, FormattedPid}],
            jsonify_stats([NewStats | T], [Mq | [ServerPid | Acc]]);
        [] ->
            jsonify_stats(T, Acc)
    end;
jsonify_stats([{K,V}|T], Acc) when is_list(V) ->
    jsonify_stats(T, [{K,list_to_binary(V)}|Acc]);

jsonify_stats([{K,V}|T], Acc) ->
    jsonify_stats(T, [{K,V}|Acc]).

-ifdef(TEST).

jsonify_stats_test_() ->
    [{"fullsync partitions left",
      fun() ->
              Stats = [{fullsync,63,left},
                       {partition,251195593916248939066258330623111144003363405824},
                       {partition_start,0.88},{stage_start,0.88}],
              Expected = [{"partitions_left",63},
                          {partition,251195593916248939066258330623111144003363405824},
                          {partition_start,0.88}, {stage_start,0.88}],
              ?assertEqual(Expected, jsonify_stats(Stats, []))
      end},
     {"legacy server_stats and client_stats",
      fun() ->
              DummyPid = self(),
              Stats = [{server_stats,
                        [{DummyPid,
                          {message_queue_len,0},
                          {status,
                           [{node,'dev1@127.0.0.1'},
                            {site,"foo"},
                            {strategy,riak_repl_keylist_server},
                            {fullsync_worker,DummyPid},
                            {queue_pid,DummyPid},
                            {dropped_count,0},
                            {queue_length,0},
                            {queue_byte_size,0},
                            {queue_max_size,104857600},
                            {queue_percentage,0},
                            {queue_pending,0},
                            {queue_max_pending,5},
                            {state,wait_for_partition}]}}]},
                       {sources,[]}],
              FormattedPid = format_pid(DummyPid),
              Expected =
                  [{server_pid,FormattedPid},
                   {message_queue_len,0},
                   {status,[{node,'dev1@127.0.0.1'},
                            {site,<<"foo">>},
                            {strategy,riak_repl_keylist_server},
                            {fullsync_worker,FormattedPid},
                            {queue_pid,FormattedPid},
                            {dropped_count,0},
                            {queue_length,0},
                            {queue_byte_size,0},
                            {queue_max_size,104857600},
                            {queue_percentage,0},
                            {queue_pending,0},
                            {queue_max_pending,5},
                            {state,wait_for_partition}]},
                   {sources,<<>>}],
                  ?assertEqual(Expected, jsonify_stats(Stats, []))
      end}].

-endif.
