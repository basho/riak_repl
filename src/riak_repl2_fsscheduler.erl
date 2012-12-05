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

-record(constraints, {
          max_sources,
          max_sinks,
          max_total,
          sources,
          sinks
    }).

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
    Constraints = #constraints{max_sources=SourceMax, max_sinks=SinkMax, max_total=TotalMax,
                              sources=SourceOwners, sinks=SinkOwners },
    anneal(CurrentWork, [], Partitions, Constraints, ?MAX_TEMP).

%% Anneal until we reach the lower temperature bound
anneal(_Cur, Solution, _Partitions, _Constraints, Temp) when Temp < ?MIN_TEMP ->
    %% cool enough. we're done.
    Solution;
anneal(Cur, Solution, Partitions, Constraints, Temp) ->
    %% anneal at temperature for some iterations
    annealTemp(Cur, Solution, Partitions, Constraints, Temp, ?MAX_ITERATIONS),
    %% reduce temperature and try again
    T = 0.90 * Temp,
    anneal(Cur, Solution, Partitions, Constraints, T).

%% Run Iterations number of iterations at this temperature
annealTemp(_Cur, Solution, _Partitions, _Constraints, _Temp, 0) ->
    Solution;
annealTemp(Cur, {OldSolution, OldScore}=Best, Partitions, Contraints, Temp, Iterations) ->
    {NewSolution, NewScore} = randomize_schedule(Cur, OldSolution, Partitions, Contraints, Temp),
    NewBest = case NewScore > OldScore of
                  true -> %% improved
                      {NewSolution, NewScore};
                  false -> %% no improvement
                      Best
              end,
    annealTemp(Cur, NewBest, Partitions, Contraints, Temp, Iterations-1).

%% Return a new valid schedule.
%% higher temperatures are more likely to add new partitions
%% lower temperatures will share between removes and swaps
randomize_schedule(Cur, Solution, Partitions, Constraints, Temp) ->
    NotEmpty = Solution =/= [],
    NotMaxed = (length(Cur) + length(Solution)) < Constraints#constraints.max_total,
    New = case (Temp/?MAX_TEMP) > random:uniform() and NotMaxed of
              true -> %% try adding a new partition
                  Candidates = filter(Cur, Solution, Partitions),
                  add_partition(Solution, Candidates);
              false -> %% try remove or swap
                  case random:uniform() > 0.5 and NotEmpty of
                      true -> %% remove a partition
                          remove_partition(Solution);
                      false -> %% swap a partition with a new one
                          Candidates = filter(Cur, Solution, Partitions),
                          swap_partition(Solution, Candidates)
                  end
          end,
    Score = case is_valid(Cur, New, Constraints) of
                true ->
                    score(Cur, New);
                false ->
                    0
            end,
    {New, Score}.

%% Return a list of candidates selected from Partitions
%% that are not in Cur or Solution
filter(Cur, Solution, Partitions) ->
    [Candidate || Candidate <- Partitions,
                  not lists:member(Candidate, Cur),
                  not lists:member(Candidate, Solution)].

%% Add a random partition, from the list of candidates, to the solution
add_partition(Solution, Candidates) ->
    Partition = lists:nth(random:uniform(length(Candidates)), Candidates),
    [Partition, Solution].

remove_partition(Solution) ->
    N = random:uniform(length(Solution)),
    remove(N, Solution).

remove(_, []) -> [];
remove(1, [_|T]) -> T;
remove(N, [H|T]) -> [H | remove(N-1, T)].

swap_partition(Solution, Candidates) ->
    N = random:uniform(length(Candidates)),
    M = random:uniform(length(Solution)),
    Partition = lists:nth(N),
    replace(M, Partition, Solution).
 
replace(_, _, []) -> [];
replace(1, P, [_|T]) -> [P|T];
replace(N, P, [H|T]) -> [H|replace(N-1, P, T)].

is_valid(_CurWork, _NewWork, _Constraints) ->
    false.

%% Score is zero if it's not legal, otherwise it's the length of (Cur ++ New).
score(Cur, New) ->
    length(Cur) + length(New).


%%%%%%%%%%%%%%%%%%%%%
%% Eunit Tests
%%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).

score_test() ->
    ?assert(score([1,2,3,4], [1,2]) == 6),
    ?assert(score([1,2,3,4], []) == 4).

replace_test() ->
    A = [1, 2, 3, 4],
    B = replace(1, 9, A),
    C = replace(2, 9, A),
    D = replace(3, 9, A),
    E = replace(4, 9, A),
    ?assert(B == [9, 2, 3, 4]),
    ?assert(C == [1, 9, 3, 4]),
    ?assert(D == [1, 2, 9, 4]),
    ?assert(E == [1, 2, 3, 9]).

remove_test() ->
    A = [1, 2, 3, 4],
    B = remove(1, A),
    C = remove(2, A),
    D = remove(3, A),
    E = remove(4, A),
    ?assert(B == [2, 3, 4]),
    ?assert(C == [1, 3, 4]),
    ?assert(D == [1, 2, 4]),
    ?assert(E == [1, 2, 3]).

randomize_schedule_test() ->
    Cur = [],
    Solution = [],
    Partitions = [],
    Temp = 100.0,
    Constraints = #constraints{max_sources=1, max_sinks=1, max_total=20,
                               sources=[], sinks=[] },
    {New, Score} = randomize_schedule(Cur, Solution, Partitions, Constraints, Temp),
    ?assert(Score == 0),
    ?assert(New == []).

annealTemp_test() ->
    Cur = [],
    Solution = [],
    Partitions = [],
    Temp = 100.0,
    Constraints = #constraints{max_sources=1, max_sinks=1, max_total=20,
                               sources=[], sinks=[] },
    Work = annealTemp(Cur, Solution, Partitions, Constraints, Temp, ?MAX_ITERATIONS),
    ?assert(Work == []).

anneal_test() ->
    Constraints = #constraints{max_sources=1, max_sinks=1, max_total=20,
                               sources=[], sinks=[] },
    CurrentWork = [],
    Partitions = [],
    Work = anneal(CurrentWork, [], Partitions, Constraints, ?MAX_TEMP),
    ?assert(Work == []).

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






























