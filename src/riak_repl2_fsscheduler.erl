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
          sinks,
          max_possible %% theoretical max number of partitions that can be scheduled
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
%% Owners: [{node(), [partition()]}]
schedule(CurrentWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax) ->
    SourceDict = mk_dict(SourceOwners),
    SinkDict = mk_dict(SinkOwners),
%%    ?debugFmt("SourceDict: ~p", [SourceDict]),
%%    ?debugFmt("SinkDict: ~p", [SinkDict]),
    MaxPossible = erlang:min(length(SourceOwners) * SourceMax,
                             length(SinkOwners) * SinkMax),
    Constraints = #constraints{max_sources=SourceMax, max_sinks=SinkMax, max_total=TotalMax,
                              sources=SourceDict, sinks=SinkDict,
                              max_possible=MaxPossible },
    {Iterations, Schedule} = anneal(CurrentWork, {[],0}, Partitions, Constraints, ?MAX_TEMP, 0),
    ?debugFmt("Added Schedule after ~p iterations = ~p", [Iterations, Schedule]),
    Schedule.

%% create an ordered dictionary that maps Key:Partition to Value:Owner node
mk_dict(Owners) ->
    lists:foldl(fun({Node,Partitions}, Dict1) ->
                        lists:foldl(fun(Partition, Dict) ->
                                            orddict:store(Partition, Node, Dict)
                                    end,
                                    Dict1,
                                    Partitions)
                end,
                orddict:new(),
                Owners).

%% Anneal until we reach the lower temperature bound
anneal(_Cur, {Solution,_Score}=_Best, _Partitions, _Constraints, Temp, Iterations) when Temp < ?MIN_TEMP ->
    %% cool enough. we're done.
    {Iterations, Solution};
anneal(Cur, OldSolution, Partitions, Constraints, Temp, Iterations) ->
    %% anneal at temperature for some iterations
    {NumIterations, NewSolution} = annealTemp(Cur, OldSolution, Partitions, Constraints, Temp, ?MAX_ITERATIONS),
    %% reduce temperature and try again
    T = 0.90 * Temp,
    anneal(Cur, NewSolution, Partitions, Constraints, T, Iterations+NumIterations).

%% Run Iterations number of iterations at this temperature.
%% stop early on goal completion
annealTemp(_Cur, Solution, _Partitions, _Constraints, _Temp, 0) ->
    %% ran max iterrations at this temp
    {?MAX_ITERATIONS, Solution};
annealTemp(Cur, {OldSolution, OldScore}=Best, Partitions, Constraints, Temp, Iterations) ->
    SolutionLength = length(Cur) + length(OldSolution),
    
    case (SolutionLength >= Constraints#constraints.max_total) or 
        (SolutionLength >= Constraints#constraints.max_possible) of
        true ->
            {?MAX_ITERATIONS-Iterations, Best};
        false ->
            {NewSolution, NewScore} = randomize_schedule(Cur, OldSolution, Partitions, Constraints, Temp),
            NewBest = case NewScore - OldScore of
                          Delta when Delta >= 0 ->
                              %% improvement
                              {NewSolution, NewScore};
                          Delta when Delta == 0 ->
                              %% a swap might be an improvement that allows a new add.
                              %% allow a swap at higher temps?
                              case ( random:uniform() > 0.5) of
                                  true ->
                                      {NewSolution, NewScore};
                                  false ->
                                      Best
                              end;
                          Delta when Delta < 0 ->
                              Best
                      end,
            annealTemp(Cur, NewBest, Partitions, Constraints, Temp, Iterations-1)
    end.

%% Return a new valid schedule.
%% higher temperatures are more likely to add new partitions
%% lower temperatures will share between removes and swaps
randomize_schedule(Cur, Solution, Partitions, Constraints, Temp) ->
    NotMaxed = (length(Cur) + length(Solution)) < Constraints#constraints.max_total,
    Random = random:uniform(),
    TemperatureRatio = Temp / ?MAX_TEMP,
    %% candidates are partitions that are not already in the solution
    Candidates = filter(Cur, Solution, Partitions),

    {New, Changed} =
        case (TemperatureRatio > Random) and NotMaxed of
            true -> %% try adding a new partition
%%                ?debugFmt("~p Add partition from candidates ~p ~p", [Temp, Candidates, Solution]),
                add_partition(Solution, Candidates);
            false -> %% try swap
%%                ?debugFmt("~p Swap partition from candidates ~p ~p", [Temp, Candidates, Solution]),
                swap_partition(Solution, Candidates)
        end,

    Score = case is_valid(Cur, New, Changed, Constraints) of
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
%% and return the new solution and the added partition.
add_partition(Solution, []) -> {Solution,[]};
add_partition([], Candidates) ->
    Partition = lists:nth(random:uniform(length(Candidates)), Candidates),
    {[Partition],[Partition]};
add_partition(Solution, Candidates) ->
    Partition = lists:nth(random:uniform(length(Candidates)), Candidates),
    {[Partition|Solution], [Partition]}.

%% remove_partition([]) -> [];
%% remove_partition(Solution) ->
%%     N = random:uniform(length(Solution)),
%%     remove(N, Solution).

-ifdef(TEST).
remove(_, []) -> [];
remove(1, [_|T]) -> T;
remove(N, [H|T]) -> [H | remove(N-1, T)].
-endif.

swap_partition([], _) -> {[],[]};
swap_partition(Solution, []) -> {Solution,[]};
swap_partition(Solution, Candidates) ->
    N = random:uniform(length(Candidates)),
    Added = lists:nth(N, Candidates),
    M = random:uniform(length(Solution)),
    Removed = lists:nth(M, Solution),
    {replace(M, Added, Solution), [Added,Removed]}.
 
replace(_, _, []) -> [];
replace(1, P, [_|T]) -> [P|T];
replace(N, P, [H|T]) -> [H|replace(N-1, P, T)].

%% A valid solution honors the constraints, which include the current work.
%% Changed is just the list of partitions that changed in the new schedule (added and removed)
is_valid(_CurWork, _NewWork, []=_Changed, _Constraints) -> true;
is_valid(CurWork, NewWork, Changed, Constraints) ->
    %% test Acc first for fail-fast, but heck the list:length is never > 2
    lists:foldl(fun(P,Acc) -> Acc and (is_valid_p(CurWork, NewWork, P, Constraints)) end,
                true,
                Changed).

is_valid_p(CurWork, NewWork, Partition, Constraints) ->
    Sources = Constraints#constraints.sources,
    Sinks = Constraints#constraints.sinks,
    SourceOwner = owner_of(Partition, Sources),
    SinkOwner = owner_of(Partition, Sinks),
    SourceLoad = commitments(SourceOwner, Sources, CurWork) + commitments(SourceOwner, Sources, NewWork),
    SinkLoad = commitments(SinkOwner, Sinks, CurWork) + commitments(SinkOwner, Sinks, NewWork),
    ((SourceLoad =< Constraints#constraints.max_sources)
     and (SinkLoad =< Constraints#constraints.max_sinks)
     and ((length(CurWork) + length(NewWork)) =< Constraints#constraints.max_total)).

owner_of(Partition, Ownership) ->
    case orddict:find(Partition, Ownership) of
        {ok, Owner} ->
            Owner;
        error ->
            %% impossible
            throw(partition_not_found_impossible)
    end.

%% Return how many times this Owner is committed to a partition in the work schedule
commitments(_Owner, _Ownership, []) -> 0;
commitments(Owner, Ownership, Work) ->
    lists:foldl(fun(Partition, Count) ->
                        case owner_of(Partition, Ownership) of
                            Owner ->
                                Count + 1;
                            _ ->
                                Count
                        end
                end,
                0,
                Work).

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
    F = replace(1, 9, []),
    ?assert(B == [9, 2, 3, 4]),
    ?assert(C == [1, 9, 3, 4]),
    ?assert(D == [1, 2, 9, 4]),
    ?assert(E == [1, 2, 3, 9]),
    ?assert(F == []).

remove_test() ->
    A = [5, 6, 7, 8],
    B = remove(1, A),
    C = remove(2, A),
    D = remove(3, A),
    E = remove(4, A),
    F = remove(1, []),
    ?assert(B == [6, 7, 8]),
    ?assert(C == [5, 7, 8]),
    ?assert(D == [5, 6, 8]),
    ?assert(E == [5, 6, 7]),
    ?assert(F == []).

schedule_1_test() ->
    PriorWork = [],
    Partitions = partitions(),
    SourceOwners = source_owners(),
    SinkOwners = sink_owners(),
    SourceMax = 2,
    SinkMax = 1,
    TotalMax = 10,
    Work = schedule(PriorWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == [k,g,h]).

schedule_2_test() ->
    PriorWork = [],
    Partitions = partitions(),
    SourceOwners = source_owners(),
    SinkOwners = sink_owners(),
    SourceMax = 2,
    SinkMax = 2,
    TotalMax = 10,
    Work = schedule(PriorWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == [h,b,i,j,a,c]).

schedule_3_test() ->
    PriorWork = [],
    Partitions = partitions(),
    SourceOwners = source_owners(),
    SinkOwners = sink_owners(),
    SourceMax = 3,
    SinkMax = 3,
    TotalMax = 2,
    Work = schedule(PriorWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == [f,d]).

schedule_4_test() ->
    PriorWork = [],
    Partitions = partitions(),
    SourceOwners = source_owners(),
    SinkOwners = sink_owners(),
    SourceMax = 3,
    SinkMax = 3,
    TotalMax = 10,
    Work = schedule(PriorWork, Partitions, SourceOwners, SinkOwners, SourceMax, SinkMax, TotalMax),
    ?assert(Work == [k,i,e,c,f,d,h,j,a]).

partitions() ->
    [a, b, c, d, e, f, g, h, i, j, k].

source_owners() ->
    [{'sink1', [a,d,g,j]},
     {'sink2', [b,e,h,k]},
     {'sink3', [c,f,i]}
    ].

sink_owners() ->
    [{'source1', [a,c,e,g]},
     {'source2', [b,d,f,h]},
     {'source3', [i,j,k]}
    ].

-endif.






























