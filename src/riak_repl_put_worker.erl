-module(riak_repl_put_worker).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, do_put/3]).

-record(state, {
    }).

start_link(_Args) ->
    gen_server:start_link(?MODULE, [], []).

do_put(Pid, Obj, Pool) ->
    gen_server:call(Pid, {put, Obj, Pool}).

init([]) ->
    {ok, #state{}}.

handle_call({put, Obj, Pool}, From, State) ->
    %% unblock the caller
    gen_server:reply(From, ok),
    %% do the put
    riak_repl_util:do_repl_put(Obj),
    %% unblock this worker for more work (or death)
    poolboy:checkin(Pool, self()),
    {noreply, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

