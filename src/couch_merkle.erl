%% -------------------------------------------------------------------
%%
%% couch_merkle
%%
%% Copyright (c) 2009 Cliff Moon.  All Rights Reserved.
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
-module(couch_merkle).
-author('cliff@powerset.com').

-behaviour(gen_server2).

%% API
-export([open/1, open/2,
         equals/2,
         root/1,
         update/3, update_many/2, updatea/3,
         delete/2, deletea/2,
         diff/2,
         close/1,
         tree/1]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% NOTE: Must mirror the record definition in couch_btree.erl.
-record(btree,
    {fd,
    root,
    extract_kv = fun({Key, Value}) -> {Key, Value} end,
    assemble_kv =  fun(Key, Value) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil,
    chunk_threshold = 16#4ff
    }).
    
-record(kv_node, {values}).
-record(kp_node, {children}).

-include("couch_db.hrl").

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-compile(export_all).
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

%%====================================================================
%% API
%%====================================================================

open(Filename) ->
  open(Filename, true).
  
open(Filename, Create) ->
  gen_server2:start_link(?MODULE, [Filename, Create], [{spawn_opt, [{fullsweep_after, 100}]}]).
  
equals(Server1, Server2) ->
  {_, Hash1} = root(Server1),
  {_, Hash2} = root(Server2),
  Hash1 == Hash2.
  
root(Server) ->
  gen_server2:call(Server, root).
  
update(Server, Key, Hash) ->
  gen_server2:call(Server, {update, Key, Hash}).
  
updatea(Server, Key, Hash) ->
  gen_server2:cast(Server, {update, Key, Hash}).

update_many(Server, KHPairs) ->
    gen_server2:call(Server, {update_many, KHPairs}).

delete(Server, Key) ->
  gen_server2:call(Server, {delete, Key}).
  
deletea(Server, Key) ->
  gen_server2:cast(Server, {delete, Key}).
  
diff(Server1, Server2) ->
  Bt1 = tree(Server1),
  Bt2 = tree(Server2),
  handle_diff(Bt1, Bt2).

close(Server) ->
  gen_server2:cast(Server, close).
  
tree(Server) ->
  gen_server2:call(Server, tree).

%% ====================================================================
%% gen_server2 callbacks
%% ====================================================================

init([Filename, Create]) ->
  put(couch_merkle, Filename),
  case {filelib:is_file(Filename),Create} of
    {true, _} -> open_existing(Filename);
    {false, true} -> open_new(Filename);
    {false, false} -> {error, enoent}
  end.


handle_call({update, Key, Hash}, _From, Bt) ->
  Bt2 = handle_update(Key, Hash, Bt),
  {reply, self(), Bt2};

handle_call({update_many, KVList}, _From, Bt) ->
  Bt2 = handle_update(KVList, Bt),
  {reply, self(), Bt2};
  
handle_call({delete, Key}, _From, Bt) ->
  Bt2 = handle_delete(Key, Bt),
  {reply, self(), Bt2};
  
handle_call(tree, _From, Bt) ->
  {reply, Bt, Bt};
  
handle_call(root, _From, Bt = #btree{root=Root}) ->
  {reply, Root, Bt};
  
handle_call(leaves, _From, Bt) ->
  {reply, handle_leaves(Bt), Bt}.



handle_cast({update, Key, Hash}, Bt) ->
  Bt2 = handle_update(Key, Hash, Bt),
  {noreply, Bt2};
  
handle_cast({delete, Key}, Bt) ->
  Bt2 = handle_delete(Key, Bt),
  {noreply, Bt2};
  
handle_cast(close, Bt) ->
  {stop, normal, Bt}.



handle_info(_Info, State) ->
  {noreply, State}.


terminate(_Reason, #btree{fd=Fd}) ->
  couch_file:close(Fd).


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

open_existing(Filename) ->
  {ok, Fd} = couch_file:open(Filename),
  {ok, #db_header{local_docs_btree_state=HeaderBtree}} = couch_file:read_header(Fd),
  couch_btree:open(HeaderBtree, Fd, [{reduce, fun reduce/2}]).
  
open_new(Filename) ->
  {ok, Fd} = couch_file:open(Filename, [create]),
  Header = #db_header{},
  ok = couch_file:write_header(Fd, Header),
  couch_btree:open(nil, Fd, [{reduce, fun reduce/2}]).

handle_update(Key, Hash, Bt) ->
  {ok, Bt2} = couch_btree:add(Bt, [{Key, Hash}]),
  optional_header_update(Bt, Bt2),
  Bt2.

handle_update(KHList, Bt) ->
    {ok, Bt2} = couch_btree:add(Bt, KHList),    
    optional_header_update(Bt, Bt2),
    Bt2.
  
handle_delete(Key, Bt) ->
  {ok, Bt2} = couch_btree:add_remove(Bt, [], [Key]),
  optional_header_update(Bt, Bt2),
  Bt2.

optional_header_update(Bt, Bt) -> ok;
optional_header_update(#btree{fd=Fd}, Bt2) ->
  ok = couch_file:write_header(Fd, #db_header{local_docs_btree_state=couch_btree:get_state(Bt2)}).

reduce(reduce, KeyValues) -> 
  lists:foldl(fun reduce_op/2, 0, KeyValues);
reduce(rereduce, Reds) -> 
  lists:foldl(fun reduce_op/2, 0, Reds).

reduce_op({_, Hash}, Acc) ->
  Hash bxor Acc;
reduce_op(Hash, Acc) ->
  Hash bxor Acc.

handle_diff(#btree{root=Root1} = Bt1, #btree{root=Root2} = Bt2) ->
  case {Root1, Root2} of
    % trees compare equal
    {nil, nil} -> [];
    {nil, _} -> handle_leaves(Bt2);
    {_, nil} -> handle_leaves(Bt1);
    {{_, Hash}, {_, Hash}} -> [];
      {{Pointer1, _Hash1}, {Pointer2, _Hash2}} ->
      Node1 = couch_btree:get_node(Bt1, Pointer1),
      Node2 = couch_btree:get_node(Bt2, Pointer2),
      {KeysA, KeysB} = key_diff(Node1, Node2, Bt1, Bt2, [], []),
      diff_merge(lists:ukeysort(1, KeysA), lists:ukeysort(1, KeysB))
  end.

handle_leaves(Bt) ->
  {ok, Leaves} = couch_btree:foldl(Bt, fun(V, Acc) ->
      {ok, [V|Acc]}
    end, []),
  lists:reverse(Leaves).

key_diff(_LeafA = #kv_node{values=ValuesA}, _LeafB = #kv_node{values=ValuesB}, 
    TreeA, TreeB, KeysA, KeysB) ->
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, KeysA, KeysB);

key_diff(#kp_node{children=ChildrenA}, #kp_node{children=ChildrenB},
    TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("node differences ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);

key_diff(Leaf = #kv_node{}, #kp_node{children=Children}, TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("leaf node differences ~n"),
  lists:foldl(fun({_,{Ptr,_}}, {AccA, AccB}) ->
      Child = couch_btree:get_node(TreeB, Ptr),
      key_diff(Leaf, Child, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children);

key_diff(#kp_node{children=Children}, Leaf = #kv_node{}, TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("node leaf differences  ~n"),
  lists:foldl(fun({_,{Ptr,_}}, {AccA, AccB}) ->
      Child = couch_btree:get_node(TreeA, Ptr),
      key_diff(Child, Leaf, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children).

node_diff([], [], _TreeA, _TreeB, KeysA, KeysB) -> {KeysA, KeysB};

node_diff([], ChildrenB, _TreeA, TreeB, KeysA, KeysB) ->
    % % error_logger:info_msg("node_diff empty children ~n"),
  {KeysA, lists:foldl(fun({_,{Ptr,_}}, Acc) ->
      Child = couch_btree:get_node(TreeB, Ptr),
      hash_leaves(Child, TreeB, Acc)
    end, KeysB, ChildrenB)};

node_diff(ChildrenA, [], TreeA, _TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("node_diff children empty ~n"),
  {lists:foldl(fun({_,{Ptr,_}}, Acc) ->
      Child = couch_btree:get_node(TreeA, Ptr),
      hash_leaves(Child, TreeA, Acc)
    end, KeysA, ChildrenA), KeysB};

node_diff([{_,{_,Hash}}|ChildrenA], [{_,{_,Hash}}|ChildrenB], TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("equal nodes ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);

node_diff([{_,{PtrA,_}}|ChildrenA], [{_,{PtrB,_}}|ChildrenB], 
    TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("nodes are different ~n"),
  ChildA = couch_btree:get_node(TreeA, PtrA),
  ChildB = couch_btree:get_node(TreeB, PtrB),
  {KeysA1, KeysB1} = key_diff(ChildA, ChildB, TreeA, TreeB, KeysA, KeysB),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA1, KeysB1).

leaf_diff([], [], _, _, KeysA, KeysB) -> {KeysA, KeysB};

leaf_diff([], [{Key,Val}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("leaf_diff empty values {~p, ~p}~n", [Key, Val]),
  leaf_diff([], ValuesB, TreeA, TreeB, KeysA, [{Key, Val}|KeysB]);

leaf_diff([{Key,Val}|ValuesA], [], TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("leaf_diff values empty {~p, ~p}~n", [Key, Val]),
  leaf_diff(ValuesA, [], TreeA, TreeB, [{Key,Val}|KeysA], KeysB);

leaf_diff([{Key,Val}|ValuesA], [{Key,Val}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("leaf_diff equals~n"),
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, KeysA, KeysB);

leaf_diff([{Key,ValA}|ValuesA], [{Key,_ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % % error_logger:info_msg("leaf_diff equal keys, diff vals ~n"),
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, [{Key,ValA}|KeysA], KeysB);

leaf_diff([{KeyA,ValA}|ValuesA], [{KeyB,ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) when KeyA < KeyB ->
  % error_logger:info_msg("leaf_diff complete diff ~p < ~p ~n", [KeyA, KeyB]),
  leaf_diff(ValuesA, [{KeyB,ValB}|ValuesB], TreeA, TreeB, [{KeyA,ValA}|KeysA], KeysB);

leaf_diff([{KeyA,ValA}|ValuesA], [{KeyB,ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) when KeyA > KeyB ->
  % error_logger:info_msg("leaf_diff complete diff ~p > ~p ~n", [KeyA, KeyB]),
  leaf_diff([{KeyA,ValA}|ValuesA], ValuesB, TreeA, TreeB, KeysA, [{KeyB, ValB}|KeysB]).
    
hash_leaves(#kp_node{children=Children}, Tree, Keys) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Child = couch_btree:get_node(Tree, Ptr),
      hash_leaves(Child, Tree, Acc)
    end, Keys, Children);

hash_leaves(#kv_node{values=Values}, _Tree, Keys) -> Keys ++ Values.

diff_merge(ListA, ListB) ->
  diff_merge(ListA, ListB, []).
  
diff_merge([], [], Acc) -> Acc;
diff_merge([], ListB, Acc) -> lists:reverse(Acc) ++ ListB;
diff_merge(ListA, [], Acc) -> lists:reverse(Acc) ++ ListA;
diff_merge([{Key,Hash}|ListA], [{Key,Hash}|ListB], Acc) ->
  diff_merge(ListA, ListB, Acc);
diff_merge([{Key,HashA}|ListA], [{Key,_HashB}|ListB], Acc) ->
  diff_merge(ListA, ListB, [{Key,HashA}|Acc]);
diff_merge([{KeyA,HashA}|ListA], [{KeyB,HashB}|ListB], Acc) when KeyA < KeyB ->
  diff_merge(ListA, [{KeyB,HashB}|ListB], [{KeyA,HashA}|Acc]);
diff_merge([{KeyA,HashA}|ListA], [{KeyB,HashB}|ListB], Acc) when KeyA > KeyB ->
  diff_merge([{KeyA,HashA}|ListA], ListB, [{KeyB,HashB}|Acc]).

-ifdef(TEST).

counterexample_1_test_() ->
    fun() ->
            L1 = [{7495817868,3538506915}],
            L2 = L1 ++ [{0,0}],
            [{0,0}] = lists_to_diff(L1, L2)
    end.

counterexample_2_test_() ->
    %% This test will fail if CHUNK_THRESHOLD in couch_btree.erl is reduced
    %% from 16#4ff down to 16#1ff.
    fun() ->
            %% f(L1).
            %% f(L2).
            %% f(Counter).
            %% Counter = eqc:counterexample().
            %% [{L1, _}, {L2,_,_,_}] = Counter.
            %% io:format("L1 = ~w,\n", [L1]).
            %% io:format("L2 = ~w,\n", [L2]).
            L1 = [{6563270977,-7449584467},{-503947359,2180544135},{4372445702,-72870531},{7404280954,-8800367436},{-8681558892,-40855653},{-5022587429,-9775401170},{-3323502402,-1296633965},{-642890215,530457253},{4237883314,914053890},{-5762691147,-9511274948},{-3691885159,-210552701},{-1539072523,23749962},{-138739462,-2921054928},{-394763102,5042143458},{5120927325,-9148543393},{9693018919,405870372},{4047032260,3782911466},{2306789791,9062130857},{-8129229857,5464247847},{3690332760,8812310750},{-9561981236,9514412071},{3680325041,-4254493324},{1356568505,-7194464766},{-6874117362,-4538002133},{-5976760513,5203425054},{-3163494883,-7091668162},{4739798580,-7674070968},{-3242872077,8483417420},{6644269556,-1530570727},{7331175768,-1015125576}],
            L2 = [{2306789791,9062130857},{5120927325,-9148543393},{-1539072523,23749962},{3680325041,-4254493324},{-8681558892,-40855653},{-5976760513,5203425054},{-5022587429,-9775401170},{-503947359,2180544135},{3690332760,8812310750},{-3163494883,-7091668162},{4047032260,3782911466},{-9561981236,0},{6644269556,-1530570727},{4372445702,-72870531},{-6874117362,-4538002133},{7331175768,-1015125576},{-5762691147,-9511274948},{-642890215,530457253},{-138739462,-2921054928},{4739798580,-7674070968},{-3691885159,-210552701},{4237883314,914053890},{9693018919,405870372},{1356568505,-7194464766},{7404280954,-8800367436},{6563270977,-7449584467},{-3242872077,8483417420},{-394763102,5042143458},{-8129229857,5464247847},{-3323502402,-1296633965}],
            %% Expected 1 diffs (0 ins 1 mod 0 del) but got 30
            [{-9561981236,9514412071}] = lists_to_diff(L1, L2)
    end.

-ifdef(EQC).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

%% EUnit

prop_subsetdiff_test_() ->
    {timeout, 180, fun() -> ?assert(eqc:quickcheck(eqc:numtests(2000, ?QC_OUT(prop_shuffle(100, 10))))) end}.

%% 
%% Example usage (your relative path may differ)
%%
%%  ../../rebar compile eunit
%%  code:add_patha("./.eunit").
%%  % Load the eunit version....
%%  l(couch_merkle).
%%  eqc:quickcheck(couch_merkle:prop_shuffle(80, 1)).
%%
%% MaxLen = Maximum length of the list of keys to put into a tree.
%% MaxDeltas = Max length of the deltas to apply to MaxLen list:
%%    insert, modify, and delete operations.
%%
%% N.B.: This test isn't actually testing a couch_btree delete operation!

prop_shuffle(MaxLen, MaxDeltas) ->
    ?FORALL(
       {L1, Deltas},
       {gen_list(MaxLen), gen_deltas(MaxDeltas)},
       %% Instead of ?LET, use ?FORALL + noshrink() for reporting difference.
       ?FORALL(
          {L2, NumI, NumM, NumD},
          noshrink(gen_2nd_list(L1, Deltas)),
          begin
              File1 = "./hack_diff_foo_1",
              File2 = "./hack_diff_foo_2",
              Diffs = lists_to_diff(File1, L1, File2, L2),
              %% Fudge = 0,
              InsertKeys = [K || {insert, K, _} <- Deltas],
              DupInsertsP = not (lists:usort(InsertKeys) == lists:sort(InsertKeys)),
              if
                  DupInsertsP ->
                      %% Call it true because this should be rare.
                      true;
                  length(Diffs) == (NumI + NumM + NumD) ->
                      true;
                 true ->
                      ?WHENFAIL(
                         io:format("Expected ~p diffs (~p ins ~p mod ~p del) but got ~p\n", [(NumI + NumM + NumD), NumI, NumM, NumD, length(Diffs)]),
                         false)
              end
          end)).

list_to_couch_merkle(File, L) ->
    {ok, T1} = couch_merkle:open(File),
    [T1 = couch_merkle:update(T1, K, H) || {K, H} <- L],
    T1.

%% Example usage: C1 = eqc:counterexample().
%%                couch_merkle:counter_to_diff(C1).

counter_to_diff(CounterExample) ->
    [{L1, _}, {L2, _, _, _}] = CounterExample,
    lists_to_diff(L1, L2).

lists_to_diff(L1, L2) ->
    File1 = "hack_diff_foo_x",
    File2 = "hack_diff_foo_y",
    lists_to_diff(File1, L1, File2, L2).

lists_to_diff(File1, L1, File2, L2) ->
    os:cmd("rm -rf " ++ File1 ++ " " ++ File2),
    T1 = list_to_couch_merkle(File1, L1),
    T2 = list_to_couch_merkle(File2, L2),
    Diffs = couch_merkle:diff(T1, T2),
    couch_merkle:close(T1),
    couch_merkle:close(T2),
    Diffs.    

gen_list(MaxLen) ->
    ?LET(Len, choose(0, MaxLen),
         ?LET(L, vector(Len, noshrink({largeint(), largeint()})),
              begin
                  Ks = [abs(K) || {K, _} <- L],
                  %% Recurse if there are duplicate keys
                  case lists:usort(Ks) == lists:sort(Ks) of
                      true  -> L;
                      false -> gen_list(MaxLen)
                  end
              end)).

gen_deltas(MaxDeltas) ->
    oneof([
           [],
           ?LET(NumDeltas, choose(0, MaxDeltas),
                vector(NumDeltas, gen_delta()))
          ]).

gen_delta() ->
    %% This insert isn't 100% certain to avoid collisions with other
    %% inserts, but we'll see how far it takes us before we have a
    %% list of KVs that has a duplicate key.....
    oneof([{insert, largeint(), largeint()},
           {modify, largeint(), x},
           {delete,          x, x}]).

gen_largeint_prefix(L) ->
    [{largeint(), X} || X <- L].

%% For the sake of laziness, this func will also shuffle the list,
%% which is probably what we want to do 100% of the time anyway.

gen_2nd_list(L, Deltas) ->
    ?LET({IL, IDeltas}, {gen_largeint_prefix(L), gen_largeint_prefix(Deltas)},
         begin
             ShuffleMix = lists:sort(IL ++ IDeltas),
             apply_deltas([Thing || {_Rand, Thing} <- ShuffleMix])
         end).

apply_deltas(L) ->
    apply_deltas(L, [], 0, 0, 0).

apply_deltas([{insert, NewK, NewV}|Tail], Acc, NumI, NumM, NumD) ->
    apply_deltas(Tail, [{NewK, NewV}|Acc], NumI + 1, NumM, NumD);
apply_deltas([{modify, NewV, _}, {K, _OldV}|Tail], Acc, NumI, NumM, NumD) ->
    apply_deltas(Tail, [{K, NewV}|Acc], NumI, NumM + 1, NumD);
apply_deltas([{delete, _, _}, {_K, _OldV}|Tail], Acc, NumI, NumM, NumD) ->
    apply_deltas(Tail, Acc, NumI, NumM, NumD + 1);
apply_deltas([{_, _} = KV|Tail], Acc, NumI, NumM, NumD) ->
    apply_deltas(Tail, [KV|Acc], NumI, NumM, NumD);
apply_deltas([_Delta|Tail], Acc, NumI, NumM, NumD) ->
    apply_deltas(Tail, Acc, NumI, NumM, NumD);
apply_deltas([], Acc, NumI, NumM, NumD) ->
    {Acc, NumI, NumM, NumD}.

nthtail(Num, L) when Num > length(L) ->
    [];
nthtail(Num, L) ->
    lists:nthtail(Num, L).

%% SLF: Older testing stuff, before I decided that the complication of
%% QuickCheck was going to be worthwhile after all.

%% SLF: What a deal, diff seems to be [] (no differences) 100% of the
%%      time for random lists of 150K elements.  And much much faster
%%      than merkerl.erl when using update_many.  When using update on
%%      each single key, (by eyeballing) I dunno which is faster.

hack_diff(ListLen) ->
    hack_diff(ListLen, 0).

hack_diff(ListLen, NumToDel) ->
    os:cmd("rm -rf ./hack_diff_1 ./hack_diff_2"),
    L = [{X, random:uniform(200)} || X <- lists:seq(1, ListLen)],
    _ExceptList = [random:uniform(ListLen) || _ <- lists:seq(1, NumToDel)],
    {ok, T1} = couch_merkle:open("./hack_diff_1"),
    L1 = hack_shuffle(L),
    L2 = lists:nthtail(NumToDel, hack_shuffle(L)),
%%    L2 = hack_shuffle([X || X <- L2a, not lists:member(X, ExceptList)]),
    io:format("Shuffled lists equal: ~p, ", [L1 == L2]),
    [T1 = couch_merkle:update(T1, K, H) || {K, H} <- L1],
    %% T1 = couch_merkle:update_many(T1, L1),
    io:format("T1 updated, "),
    {ok, T2} = couch_merkle:open("./hack_diff_2"),
    [T2 = couch_merkle:update(T2, K, H) || {K, H} <- L2],
    %% T2 = couch_merkle:update_many(T2, L2),
    io:format("T2 updated\n"),
    couch_merkle:diff(T1, T2).

hack_shuffle(L) ->
    L2 = [{random:uniform(100), X} || X <- L],
    [X || {_, X} <- lists:sort(L2)].

-endif.   %% EQC
-endif.   %% TEST

