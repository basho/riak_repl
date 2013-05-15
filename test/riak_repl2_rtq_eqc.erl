%%
%% EQC test for RTQ
%%

-module(riak_repl2_rtq_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("pulse/include/pulse.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(BINARIED_OBJ_SIZE, 39). % 39 = byte_size(term_to_binary([make_ref()])).

-record(state, {rtq, %% pid of queue process
                qseq=0, %% Queue seq number
                tout_no_clients = [], % No clients available to pull
                pcs=[a, b, c, d, e, f, g], %% potential client names
                cs=[], %% connected clients
                max_bytes=0}).

%% Test consumer record
-record(tc, {name,       %% Name
             tout=[],    %% outstanding items in queue
             trec=[],    %% received items
             tack=[]}).    %% acked items


-ifdef(TEST).
rtq_test_() ->
    {timeout, 60,
     fun() ->
                ?assert(eqc:quickcheck(eqc:testing_time(25,
                                                        ?MODULE:prop_main()))),
                ?assert(eqc:quickcheck(eqc:testing_time(25,
                                                        ?MODULE:prop_parallel()))),
                catch(meck:unload(riak_repl_stats))
        end
    }.
-endif.


max_bytes() ->
    ?LET(MaxBytes, nat(), {size, (MaxBytes+1) * ?BINARIED_OBJ_SIZE}).

cleanup() ->
    catch(meck:unload(riak_repl_stats)),
    ok = meck:new(riak_repl_stats, [passthrough]),
    ok = meck:expect(riak_repl_stats, rt_source_errors,
        fun() -> ok end),
    ok = meck:expect(riak_repl_stats, rt_sink_errors,
        fun() -> ok end).

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
        begin
                cleanup(),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                catch(exit(S#state.rtq, kill)),
                aggregate(command_names(Cmds),
                    pretty_commands(?MODULE, Cmds, {H,S,Res}, Res==ok))
        end).

prop_parallel() ->
    ?LET(Repeat, ?SHRINK(1, []),
    ?FORALL(Cmds, parallel_commands(?MODULE),
    ?ALWAYS(Repeat,
        begin
                cleanup(),
                {H, S, Res} = run_parallel_commands(?MODULE,Cmds),
                kill_all_pids({H, S}),
                aggregate(command_names(Cmds),
                    pretty_commands(?MODULE, Cmds, {H,S,Res}, Res==ok))
        end))).

prop_pulse() ->
  ?FORALL(Cmds, parallel_commands(?MODULE),
  ?PULSE(HSR={_, _, R},
    begin
      cleanup(),
      run_parallel_commands(?MODULE, Cmds)
    end,
    %catch(exit((element(2, HSR))#state.rtq, kill)),
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok)))).


pulse_instrument() ->
  [ pulse_instrument(File) || File <-  ["./src/riak_repl2_rtq.erl"] ].

pulse_instrument(File) ->
  Modules = [ application, application_controller, application_master,
              application_starter, gen, gen_event, gen_fsm, gen_server,
              proc_lib, supervisor ],
  ReplaceModules =
    [{Mod, list_to_atom(lists:concat([pulse_, Mod]))}
      || Mod <- Modules],
  {ok, Mod} = compile:file(File, [{d, 'PULSE', true}, {d, 'TEST', true},
                                  {parse_transform, pulse_instrument},
                                  {pulse_replace_module, ReplaceModules}]),
  code:purge(Mod),
  code:load_file(Mod),
  Mod.

kill_all_pids(Pid) when is_pid(Pid) -> exit(Pid, kill);
kill_all_pids([H|T])                -> kill_all_pids(H), kill_all_pids(T);
kill_all_pids(T) when is_tuple(T)   -> kill_all_pids(tuple_to_list(T));
kill_all_pids(_)                    -> ok.

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    #state{}.

%% start the RTQ *and* set the max bytes for the queue
test_init({size, MaxBytes}) ->
    application:set_env(riak_repl, rtq_max_bytes, MaxBytes),
    {ok, Pid} = riak_repl2_rtq:start_test(),
    Pid.


client_name(S) ->
    ?LET(Client, elements(S#state.cs), Client#tc.name).

get_client(Name, S) ->
    lists:keyfind(Name, #tc.name, S#state.cs).

command(#state{rtq=undefined}) ->
        {call, ?MODULE, test_init, [max_bytes()]};
command(S) ->
    frequency(lists:map(fun(Call={call, _, Fun, _}) -> {weight(Fun), Call} end,
        [{call, ?MODULE, push, [make_item(), S#state.rtq]}] ++
        [{call, ?MODULE, new_consumer, [elements(S#state.pcs), S#state.rtq]} ||
          S#state.pcs /= []] ++
        [{call, ?MODULE, rm_consumer, [client_name(S), S#state.rtq]} ||
          S#state.cs /= []] ++
        [{call, ?MODULE, replace_consumer, [client_name(S), S#state.rtq]} ||
          S#state.cs /= []] ++
        [{call, ?MODULE, pull, [client_name(S), S#state.rtq]} ||
          S#state.cs /= []] ++
        [{call, ?MODULE, ack, [
                  ?LET(C, elements(S#state.cs),
                       {C#tc.name, gen_seq(C)}), S#state.rtq]} ||
          S#state.cs /= []] ++
        []
    )).

weight(push) -> 5;
weight(new_consumer) -> 3;
weight(rm_consumer) -> 1;
weight(replace_consumer) -> 1;
weight(pull) -> 8;
weight(ack) -> 6;
weight(_) -> 1.

precondition(S,{call,riak_repl2_rtq,test_init,_}) ->
    S#state.rtq == undefined;
precondition(S,{call,?MODULE,new_consumer, [Name, _]}) ->
    lists:member(Name, S#state.pcs);
precondition(S,{call,?MODULE,pull, [Name, _]}) ->
    lists:keymember(Name, #tc.name, S#state.cs);
precondition(S,{call,?MODULE,ack, [{Name, Seq}, _]}) ->
    case get_client(Name, S) of
        false ->
            false;
        C ->
            lists:keymember(Seq, 1, C#tc.trec)
    end;
precondition(S,{call,?MODULE,rm_consumer, [Name, _]}) ->
    lists:keymember(Name, #tc.name, S#state.cs);
precondition(S,{call,?MODULE,replace_consumer, [Name, _]}) ->
    lists:keymember(Name, #tc.name, S#state.cs);
precondition(_S,{call,_,_,_}) ->
    true.


postcondition(S,{call,?MODULE,pull,[Name, _]},R) ->
    C = get_client(Name, S),
    Tout = C#tc.tout,
    case R of
        none ->
            Tout == [] orelse {not_empty, C#tc.name, Tout};
        {Seq, Size, Item} ->
            case Tout of
                [] ->
                    {unexpected_item, C#tc.name, {Seq, Size, Item}};
                [H|_] ->
                   H == {Seq, Size, Item} orelse {not_match, C#tc.name, H,
                        {Seq, Size, Item}}
            end
    end;
postcondition(S,{call,?MODULE,push,[_Item, Q]},_R) ->
    % guarantee that the queue size never grows above max_bytes
    QBytes = get_rtq_bytes(Q),

    lists:foldl(fun(_TC, Acc) ->
                ((S#state.max_bytes >= QBytes) == Acc)
        end, true, S#state.cs) orelse {queue_too_big, S#state.max_bytes,
                                       QBytes};
    %true;
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,V,{call, _, test_init, [{size, MaxBytes}]}) ->
    S#state{rtq=V, max_bytes=MaxBytes};
next_state(S,_V,{call, _, new_consumer, [Name, _Q]}) ->
    %% IF there's other clients, this client's outstanding messages will
    %% be the longest REC+OUT queue from one of the other consumers.
    %% Otherwise use the tout_no_clients value and wipe from the state.
    Tout = lists:foldl(fun(#tc{trec = Trec, tout=Tout}, Longest) ->
                    Q = trim(Trec ++ Tout, S),
                    case length(Q) > length(Longest) of
                        true ->
                            Q;
                        _ ->
                            Longest
                    end
            end, S#state.tout_no_clients, S#state.cs),
    lager:info("starting client ~p with backlog of ~p~n", [Name, length(Tout)]),
    S#state{cs=[#tc{name=Name, tout=Tout}|S#state.cs],
            tout_no_clients = [],
            pcs=S#state.pcs -- [Name]};
next_state(S,_V,{call, _, rm_consumer, [Name, _Q]}) ->
    delete_client(Name, S#state{pcs=[Name|S#state.pcs]});
next_state(S,_V,{call, _, replace_consumer, [Name, _Q]}) ->
    Client = get_client(Name, S),
    %% anything we didn't ack will be considered dropped by the queue
    NewClient = Client#tc{tack=[],
                          trec=[],
                          tout=trim(Client#tc.trec
                          ++ Client#tc.tout, S)},
    update_client(NewClient, S);
next_state(S0,_V,{call, _, push, [Value, _Q]}) ->
    S = S0#state{qseq = S0#state.qseq+1},
    Item = {S#state.qseq, length(Value), Value},
    case S#state.cs of
        [] ->
            S#state{tout_no_clients=trim(S#state.tout_no_clients ++ [Item], S)};
        _ ->
            Clients = lists:map(fun(TC) ->
                            TC#tc{tout=trim(TC#tc.tout ++ [Item], S)}
                    end, S#state.cs),
            S#state{cs=Clients}
    end;
next_state(S,_V,{call, _, pull, [Name, _Q]}) ->
    Client = get_client(Name, S),
    %lager:info("tout is ~p~n", [Client#tc.tout]),
    {Tout, Trec} = case Client#tc.tout of
        [] ->
            %% nothing to get
            {[], Client#tc.trec};
        [H|T] ->
            {T, Client#tc.trec ++ [H]}
    end,
    %lager:info("trec ~p, tout ~p~n", [Trec, Tout]),
    update_client(Client#tc{tout=Tout, trec=Trec}, S);
next_state(S,_V,{call, _, ack, [{Name,N}, _Q]}) ->
    Client = get_client(Name, S),
    {H, [X|T]} = lists:splitwith(fun({Seq, _, _}) -> Seq /= N end, Client#tc.trec),
    update_client(Client#tc{trec=T,
            tack=Client#tc.tack ++ H ++ [X]}, S);
next_state(S,_V,{call, _, _, _}) ->
    S.

make_item() ->
    ?LAZY([make_ref()]).

push(List, Q) ->
    lager:info("pushed item ~p~n to ~p~n", [List, Q]),
    gen_server:call(Q, {push, length(List), term_to_binary(List)}).

new_consumer(Name, Q) ->
    lager:info("registering ~p to ~p~n", [Name, Q]),
    gen_server:call(Q, {register, Name}).

rm_consumer(Name, Q) ->
    lager:info("unregistering ~p", [Name]),
    gen_server:call(Q, {unregister, Name}).

replace_consumer(Name, Q) ->
    lager:info("replacing ~p", [Name]),
    gen_server:call(Q, {register, Name}).

get_rtq_bytes(Q) ->
    Stats = gen_server:call(Q, status),
    proplists:get_value(bytes, Stats).


pull(Name, Q) ->
    lager:info("~p pulling from ~p~n", [Name, Q]),
    Ref = make_ref(),
    Self = self(),
    F = fun(Item) ->
            Self ! {Ref, Item},
            receive
                {Ref, ok} ->
                    ok
            after
                5 ->
                    lager:info("No pull ack from ~p~n", [Name]),
                    error 
            end
    end,
    gen_server:cast(Q, {pull, Name, F}),
    receive
        {Ref, {Seq, Size, Item}} ->
            lager:info("~p got ~p size ~p seq ~p~n", [Name, Item, Size, Seq]),
            Q ! {Ref, ok},
            %gen_server:cast(Q, {ack, Name, Seq}),
            {Seq, Size, binary_to_term(Item)}
    after
        20 ->
            lager:info("queue empty: ~p~n", [Name]),
            none
    end.

ack({Name, Seq}, Q) ->
    gen_server:call(Q, {ack_sync, Name, Seq}).

delete_client(Name, S) ->
    S#state{cs=lists:keydelete(Name, #tc.name, S#state.cs)}.

update_client(C, S) ->
    S#state{cs=[C|lists:keydelete(C#tc.name, #tc.name, S#state.cs)]}.

gen_seq(#tc{trec = []}) -> no_seq;
gen_seq(C) ->
    ?LET(E, elements(C#tc.trec), element(1, E)).

trim(Q, #state{max_bytes=Max}) ->
    {_Size, NewQ} = lists:foldl(fun({Seq, NumItems, Bin}, {Size, Acc}) ->
                case (?BINARIED_OBJ_SIZE + Size) > Max of
                    true ->
                        {Size, Acc};
                    false ->
                        {Size + ?BINARIED_OBJ_SIZE, [{Seq, NumItems, Bin}|Acc]}
                end
            end, {0, []}, lists:reverse(Q)),
    case Q /= NewQ of
        true ->
            %io:format(user, "trimmed ~p items~n", [length(Q) - length(NewQ)]),
            %io:format(user, "~p -> ~p :: ~p~n", [Q, NewQ, Max]),
            ok;
        false ->
            ok
    end,
    NewQ.

-endif.

