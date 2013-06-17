-module(riak_repl2_rt_spanning).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

init(_) ->
    {ok, digraph:new()}.

handle_call(_Msg, _From, Graph) ->
    {reply, {error, nyi}, Graph}.

handle_cast(stop, Graph) ->
    {stop, normal, Graph};

handle_cast(_Msg, Graph) ->
    {noreply, Graph}.

handle_info(_Msg, Graph) ->
    {noreply, Graph}.

terminate(_Why, _Graph) ->
    ok.

code_change(_Vsn, Graph, _Extra) ->
    {ok, Graph}.

-ifdef(TEST).

functionality_test_() ->
    {setup, fun() ->
        ok
    end,
    fun(ok) ->
        case whereis(?MODULE) of
            undefined ->
                ok;
            Pid ->
                exit(Pid, kill)
        end
    end,
    fun(ok) -> [

        {"start up", fun() ->
            Got = ?MODULE:start_link(),
            ?assertMatch({ok, _Pid}, Got),
            ?assert(is_pid(element(2, Got))),
            unlink(element(2, Got))
        end},

        {"tear down", fun() ->
            Pid = whereis(?MODULE),
            Mon = erlang:monitor(process, Pid),
            ?MODULE:stop(),
            Got = receive
                {'DOWN', Mon, process, Pid, _Why} ->
                    true
            after 1000 ->
                {error, timeout}
            end,
            ?assert(Got)
        end}

    ] end}.

-endif.