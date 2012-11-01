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
         pretty_print/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

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
    Stats1 = riak_repl_stats:get_stats(),
    LeaderStats = riak_repl_console:leader_stats(),
    [{server_stats, Servers}] = riak_repl_console:server_stats(),
    [{client_stats, Clients}] = riak_repl_console:client_stats(),
    Stats1 ++ LeaderStats ++ format_stats(client_stats, Clients, []) ++
        format_stats(server_stats, Servers, []).
    

format_stats(Type, [], Acc) ->
    [{Type, lists:reverse(Acc)}];
format_stats(Type, [{P, M, {status, S}}|T], Acc) ->
    format_stats(Type, T, [[{pid, list_to_binary(erlang:pid_to_list(P))},
                            M, {status, jsonify_stats(S, [])}]|Acc]).
jsonify_stats([], Acc) ->
    lists:flatten(lists:reverse(Acc));
jsonify_stats([{K,V}|T], Acc) when is_pid(V) ->
    jsonify_stats(T, [{K,list_to_binary(erlang:pid_to_list(V))}|Acc]);
jsonify_stats([{K,V}|T], Acc) when is_list(V) ->
    jsonify_stats(T, [{K,list_to_binary(V)}|Acc]);
jsonify_stats([{S,IP,Port}|T], Acc) when is_atom(S) andalso is_list(IP) andalso is_integer(Port) ->
    jsonify_stats(T, [{S,
                       list_to_binary(IP++":"++integer_to_list(Port))}|Acc]);
jsonify_stats([{S,{A,B,C,D},Port}|T], Acc) when is_atom(S) andalso is_integer(Port) ->
    jsonify_stats(T, [{S,
                       iolist_to_binary(io_lib:format("~b.~b.~b.~b:~b",[A,B,C,D,Port]))}|Acc]);
jsonify_stats([{K,V}|T], Acc) ->
    jsonify_stats(T, [{K,V}|Acc]).
    
