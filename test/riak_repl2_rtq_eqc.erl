%%
%% EQC test for RTQ
%%
%% Consumers modelled as public ETS tables

-module(riak_repl2_rtq_eqc).
%-define(EQC,true).
-undef(EQC).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {items=0, %% Number of items created
                cs=[]}).

%% Test consumer record
-record(tc, {name,    %% Name
             trec,    %% ETS table for received items
             tack}).  %% ETS table for acked items

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, {_State, _StateData}, Res} = run_commands(?MODULE,Cmds),
                ?WHENFAIL(begin
                              io:format(user, "Test Failed\n~p\n",
                                        [zip(state_names(H),command_names(Cmds))]),
                              io:format(user, "State: ~p\nStateData: ~p\nRes: ~p\n",
                                        [_State, _StateData, Res])
                          end,
                          %% Generate statistics
                          aggregate(zip(state_names(H),command_names(Cmds)), Res == ok))
            end).


%% ====================================================================
%% eqc_fsm callbacks
%% ====================================================================

initial_state() ->
    running.

initial_state_data() ->
    cleanup(),
    riak_repl2_rtq:start_link(),
    unlink(whereis(riak_repl2_rtq)),
    #state{}.

running(S) ->
    [{running, {call, riak_repl2_rtq, push, [make_item(S#state.items)]}},
     {running, {call, ?MODULE, new_consumer, [length(S#state.cs)]}},
     {running, {call, ?MODULE, pull, [element(S#state.cs)]].

next_state_data(_From, _To, S, _Res, {call, _, push, [_Number]}) ->
    S#state{items = #state.items + 1};
next_state_data(_From, _To, S, Res, {call, _, new_consumer, [_Number]}) ->
    S#state{cs = S#state.cs ++ [Res]};
next_state_data(_From, _To, S, _Res, _Call) ->
    S.
precondition(_From, _To, _S, _Call) ->
    true.
postcondition(_From, _To, _S, _Call, _Res) ->
    true.

%% ====================================================================
%% Generator functions
%% ====================================================================

%% ====================================================================
%% Helper functions
%% ====================================================================

cleanup() ->
    case whereis(riak_repl2_rtq) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            MRef = erlang:monitor(process, Pid),
            receive 
                {'DOWN', MRef, _Type, _Object, _Info} ->
                    ok
            after
                5000 ->
                    exit({could_not_kill_riak_repl2_rtq, Pid})
            end
    end.

make_item(Items) ->
    list_to_binary("i" ++ integer_to_list(Items+1)).

make_consumer(Number) ->
    list_to_atom("c" ++ integer_to_list(Number+1)).

new_consumer(Number) ->
    #tc{name = make_consumer(Number),
        trec = ets:new(received_tab, [public, ordered_set]),
        tack = ets:new(acked_tab, [public, ordered_set])}.
        
-endif.
