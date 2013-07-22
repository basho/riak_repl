-module(stateful).

-export([get/1,set/2,delete/1,start/0,stop/0,reset/0]).
-export([loop/0]).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

set(Key, Value) ->
    gen_server:cast(?MODULE, {set, Key, Value}).

delete(Key) ->
    gen_server:cast(?MODULE, {delete, Key}).

start() ->
    Pid = proc_lib:spawn(?MODULE, loop, []),
    register(?MODULE, Pid),
    {ok, Pid}.

stop() ->
	riak_repl_test_util:kill_and_wait(?MODULE).

reset() ->
    riak_repl_test_util:kill_and_wait(?MODULE),
    start().

loop() ->
    receive
        {'$gen_call', From, {get, Key}} ->
            Val = erlang:get(Key),
            gen_server:reply(From, Val),
            loop();
        {'$gen_cast', {set, Key, Value}} ->
            erlang:put(Key, Value),
            loop();
        {'$gen_cast', {delete, Key}} ->
            erlang:delete(Key),
            loop()
    end.