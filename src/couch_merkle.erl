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

-record(btree,
    {fd,
    root,
    extract_kv = fun({Key, Value}) -> {Key, Value} end,
    assemble_kv =  fun(Key, Value) -> {Key, Value} end,
    less = fun(A, B) -> A < B end,
    reduce = nil
    }).
    
-record(kv_node, {values}).
-record(kp_node, {children}).

-include("couch_db.hrl").

-ifdef(EUNIT).
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
