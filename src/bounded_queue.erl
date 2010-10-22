%% -------------------------------------------------------------------
%%
%% bounded_queue:  a size-bounded FIFO
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
%% @doc A simple size-bounded FIFO queue.

-module(bounded_queue).
-author('Andy Gross <andy@basho.com>').

-export([new/1, 
         in/2, 
         out/1, 
         byte_size/1, 
         len/1,
         dropped_count/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(bq, {
          m=0 :: non_neg_integer(),  %% maximum size of queue, in bytes.
          s=0 :: non_neg_integer(),  %% current size of queue, in bytes.
          q=queue:new() :: queue(),  %% underlying queue.
          d=0 :: non_neg_integer()}  %% dropped item count
       ).

-type(bounded_queue() :: #bq{}).

%% @spec new(MaxSize::non_neg_integer()) -> bounded_queue()
%% @doc  Create a new queue with maximum size in bytes of MaxSize.
-spec new(non_neg_integer()) -> bounded_queue().
new(MaxSize) when is_integer(MaxSize) -> #bq{m=MaxSize, s=0, q=queue:new()}.

%% @spec in(bounded_queue(), binary()) -> bounded_queue()
%% @doc  Add an item to the queue.  
-spec in(bounded_queue(), binary()) -> bounded_queue().
in(BQ=#bq{m=Max, q=Q, d=D}, Item) when is_binary(Item) ->
    ItemSize = size(Item),
    case ItemSize > Max of
        true ->
            BQ#bq{q=queue:from_list([Item]),s=ItemSize,d=queue:len(Q)+D};
        false ->
            make_fit(BQ, Item, ItemSize)
    end.

%% @spec out(bounded_queue()) -> {'value',binary()|'empty', bounded_queue()}
%% @doc  Remove an item from the queue.
-spec out(bounded_queue()) -> {{'value',binary()}|'empty', bounded_queue()}.
out(BQ=#bq{q=Q,s=Size}) ->
    case queue:out(Q) of
        {empty, _} -> {empty, BQ};
        {{value, Item}, NewQ} -> {{value, Item}, BQ#bq{s=Size-size(Item),q=NewQ}}
    end.

%% @spec byte_size(bounded_queue()) -> non_neg_integer()
%% @doc  The size of the queue, in bytes.
-spec byte_size(bounded_queue()) -> non_neg_integer().
byte_size(#bq{s=Size}) -> Size.

%% @spec len(bounded_queue()) ->  non_neg_integer()
%% @doc  The number of items in the queue.
-spec len(bounded_queue()) -> non_neg_integer().
len(#bq{q=Q}) -> queue:len(Q).

%% @spec dropped_count(bounded_queue()) ->  non_neg_integer()
%% @doc  The number of items dropped from the queue due to the size bound.
-spec dropped_count(bounded_queue()) -> non_neg_integer().
dropped_count(#bq{d=D}) -> D.
    

make_fit(BQ=#bq{s=Size,m=Max,d=D}, Item, ItemSize) when (ItemSize+Size>Max) -> 
    {_, NewQ} = out(BQ),
    make_fit(NewQ#bq{d=D+1}, Item, ItemSize);
make_fit(BQ=#bq{q=Q, s=Size}, Item, ItemSize) ->
    BQ#bq{q=queue:in(Item, Q), s=Size+ItemSize}.

   
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

initialization_test() ->
    Q = new(16),
    0 = bounded_queue:len(Q),
    0 = bounded_queue:byte_size(Q),
    0 = bounded_queue:dropped_count(Q),
    Q.

in_test() ->
    Q0 = initialization_test(),
    B = <<1:128/integer>>,
    Q1 = in(Q0, B),
    1 = bounded_queue:len(Q1),
    16 = bounded_queue:byte_size(Q1),
    0 = bounded_queue:dropped_count(Q1),
    Q1.

out_test() ->
    Q0 = in_test(),
    {{value, <<1:128/integer>>}, Q1} = out(Q0),
    {empty, Q2} = out(Q1),
    0 = bounded_queue:len(Q2),
    0 = bounded_queue:byte_size(Q2),
    0 = bounded_queue:dropped_count(Q2),
    Q2.

maxsize_test() ->
    Q0 = out_test(),
    Q1 = in(Q0, <<1:64/integer>>),
    Q2 = in(Q1, <<2:64/integer>>),
    Q3 = in(Q2, <<3:64/integer>>),
    {{value, Item}, Q4} = out(Q3),
    <<2:64/integer>> = Item,
    8 = bounded_queue:byte_size(Q4),
    1 = bounded_queue:len(Q4),
    1 = bounded_queue:dropped_count(Q4),
    Q4.

largeitem_test() ->
    Q0 = initialization_test(),
    Q1 = in(Q0, <<1:256/integer>>),
    32 = bounded_queue:byte_size(Q1),
    1 = bounded_queue:len(Q1),
    0 = bounded_queue:dropped_count(Q1),
    Q1.

-endif.
