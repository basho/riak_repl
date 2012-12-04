%% -------------------------------------------------------------------
%%
%% riak_repl2_fsscheduler
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_repl2_fsscheduler).

-define(MAX_TEMP, 100.0).
-define(MIN_TEMP, 1.0).
-define(MAX_ITERATIONS, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([schedule/7]).

%% Return a schedule of new work, based on existing work-in-progress
%% and the two ownership views of the candidate partitions.
%%
%% The length of the combined work will not exceed TotalMax.
%% No source node will appear in the combined work more than SourceMax times.
%% No sink node will  appear in the combined work more than SinkMax times.
%%
schedule(CurrentWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax) ->
    anneal(CurrentWork, [], Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax, ?MAX_TEMP).


%% Anneal until we reach the lower temperature bound
anneal(_Cur, Solution, _Partitions, _Sources, _Sinks, _SourceMax, _SinkMax, _TotalMax, Temp) when Temp < ?MIN_TEMP ->
    Solution;
anneal(Cur, Solution, Partitions, Sources, Sinks, SourceMax, SinkMax, TotalMax, Temp) ->
    %% try 
    annealTemp(Cur, Solution, Partitions, Sources, Sinks, SourceMax, SinkMax, TotalMax, Temp, ?MAX_ITERATIONS),
    %% reduce temperature and try again
    T = 0.90 * Temp,
    anneal(Cur, Solution, Partitions, Sources, Sinks, SourceMax, SinkMax, TotalMax, T).

%% Run Iterations number of iterations at this temperature
annealTemp(_Cur, Solution, _Partitions, _Sources, _Sinks, _SourceMax, _SinkMax, _TotalMax, _Temp, 0) ->
    Solution;
annealTemp(Cur, Solution, Partitions, Sources, Sinks, SourceMax, SinkMax, TotalMax, Temp, Iterations) ->
    annealTemp(Cur, Solution, Partitions, Sources, Sinks, SourceMax, SinkMax, TotalMax, Temp, Iterations-1).


%%%%%%%%%%%%%%%%%%%%%
%% Eunit Tests
%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

empty_schedule_test() ->
    CurrentWork = [],
    Partitions = [],
    SourceOwners = [],
    SinkOwners = [],
    SourceMax = 1,
    SinkMax = 1,
    TotalMax = 1,
    Work = schedule(CurrentWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == []).

schedule_1_test() ->
    PriorWork = [],
    Partitions = partitions(),
    SourceOwners = source_owners(),
    SinkOwners = sink_owners(),
    SourceMax = 1,
    SinkMax = 1,
    TotalMax = 2000,
    Work = schedule(PriorWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == []).


partitions() ->
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].

source_owners() ->
    [{'sink1', [1,4,7,10]},
     {'sink2', [2,5,8,11]},
     {'sink3', [3,6,9]}
    ].

sink_owners() ->
    [{'source1', [1,3,5,7]},
     {'source2', [2,4,6,8]},
     {'source3', [9,10,11]}
    ].

-endif.






























