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

% queued item, get it?
-record(qed_item, {seq, num_items, item_list = [], meta = []}).

-ifdef(TEST).
rtq_test_() ->
    {timeout, 80, {setup, fun() -> ok end,
    fun(_) -> catch(meck:unload(riak_repl_stats)) end,
    fun(_) -> [

        {"prop_main", timeout, 40,
            ?_assert(eqc:quickcheck(eqc:testing_time(20,
                                                        ?MODULE:prop_main())))},

        {"prop_parallel", timeout, 40,
            ?_assert(eqc:quickcheck(eqc:testing_time(20,
                                                        ?MODULE:prop_parallel())))}

    ] end}}.
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
    kill_and_wait(riak_repl2_rtq),
    #state{}.

kill_and_wait(undefined) ->
    ok;
kill_and_wait(Atom) when is_atom(Atom) ->
    kill_and_wait(whereis(Atom));
kill_and_wait(Pid) when is_pid(Pid) ->
    unlink(Pid),
    MonRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive
        {'DOWN', MonRef, process, Pid, _} ->
            ok
    end.

%% start the RTQ *and* set the max bytes for the queue
test_init({size, MaxBytes}) ->
    application:set_env(riak_repl, rtq_max_bytes, MaxBytes),
    {ok, Pid} = riak_repl2_rtq:start_link(),
    unlink(Pid),
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
        [{call, ?MODULE, push, [make_item(), routed_clusters(S#state.cs), S#state.rtq]}] ++
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
    {_Drops, RealTout} = lists:splitwith(fun(Qed) ->
        lists:member(C#tc.name, Qed#qed_item.meta)
    end, Tout),
    case R of
        none ->
            RealTout == [] orelse {not_empty, C#tc.name, Tout};
        {Seq, Size, Item} ->
            case RealTout of
                [] ->
                    {unexpected_item, C#tc.name, {Seq, Size, Item}};
                [#qed_item{seq = Seq, num_items = Size, item_list = Item}|_] ->
                    true;
                _ ->
                    {not_match, C#tc.name, hd(Tout), {Seq, Size, Item}}
                   %H == {Seq, Size, Item} orelse {not_match, C#tc.name, H,
                        %{Seq, Size, Item}}
            end
    end;
postcondition(S,{call,?MODULE,push,[_Item, Q]},_R) ->
    % guarantee that the queue size never grows above max_bytes
    QBytes = get_rtq_bytes(Q),

    lists:foldl(fun(_TC, Acc) ->
                ((S#state.max_bytes >= QBytes) == Acc)
        end, true, S#state.cs) orelse {queue_too_big, S#state.max_bytes,
                                       QBytes};
postcondition(S,{call,?MODULE,push,[_Item,_RotuedClusters,Q]},_R) ->
    % same postcondition as call/2, so no duplicate code here!
    postcondition(S,{call,?MODULE,push,[undefined, Q]},undefined);
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,V,{call, _, test_init, [{size, MaxBytes}]}) ->
    S#state{rtq=V, max_bytes=MaxBytes};
next_state(#state{cs = []} = S, _V, {call, _, new_consumer, [Name, _Q]}) ->
    Tc = #tc{name = Name, tout = S#state.tout_no_clients},
    S#state{cs = [Tc], tout_no_clients = [], pcs = S#state.pcs -- [Name]};
next_state(S, _V, {call, _, new_consumer, [Name, _Q]}) ->
    MasterQ = generate_master_q(S),
    TrueMaster = trim(MasterQ, S),
    TC = #tc{name = Name, tout = TrueMaster},
    S#state{cs = [TC|S#state.cs], pcs = S#state.pcs -- [Name]};
next_state(S,_V,{call, _, rm_consumer, [Name, _Q]}) ->
    delete_client(Name, S#state{pcs=[Name|S#state.pcs]});
next_state(S,_V,{call, _, replace_consumer, [Name, _Q]}) ->
    Client = get_client(Name, S),
    %% anything we didn't ack will be considered dropped by the queue
    MasterQ = generate_master_q(S),
    NewClient = Client#tc{tack=[],
                          trec=[],
                          tout=MasterQ},
    update_client(NewClient, S);
next_state(S0,V,{call, M, push, [Value, _Q]}) ->
    next_state(S0,V,{call,M,push,[Value,[],_Q]});
next_state(S0, _V, {call, _, push, [Value, RoutedClusters, _Q]}) ->
    %Item2 = set_meta(Item, routed_clusters, RoutedClusters),
    S = S0#state{qseq = S0#state.qseq+1},
    %Item = {S#state.qseq, length(Value), Value},
    Item = #qed_item{seq = S#state.qseq, num_items = length(Value), item_list = Value, meta = RoutedClusters},
    case S#state.cs of
        [] ->
            S#state{tout_no_clients=trim(S#state.tout_no_clients ++ [Item], S)};
        _ ->
            Clients = lists:map(fun(TC) ->
                Tout2 = TC#tc.tout ++ [Item],
                TC#tc{tout = Tout2}
            end, S#state.cs),
            trim(S#state{cs = Clients})
    end;
next_state(S,_V,{call, _, pull, [Name, _Q]}) ->
    Client = get_client(Name, S),
    %lager:info("tout is ~p~n", [Client#tc.tout]),
    SplitFun = fun(#qed_item{meta = Meta}) ->
        lists:member(Name, Meta)
    end,
    {TrecTail, ToutLeft} = case lists:splitwith(SplitFun, Client#tc.tout) of
            {SkippedOrDeliverd, []} ->
                {SkippedOrDeliverd, []};
            {SkippedOrDeliverd, [Delivered | Left]} ->
                {SkippedOrDeliverd ++ [Delivered], Left}
    end,
    Trec = Client#tc.trec ++ TrecTail,
    update_client(Client#tc{tout = ToutLeft, trec = Trec}, S);
next_state(S,_V,{call, _, ack, [{Name,N}, _Q]}) ->
    Client = get_client(Name, S),
    {H, [X|T]} = lists:splitwith(fun(#qed_item{seq = Seq}) -> Seq /= N end, Client#tc.trec),
    update_client(Client#tc{trec=T,
            tack=Client#tc.tack ++ H ++ [X]}, S);
next_state(S,_V,{call, _, _, _}) ->
    S.

invariant(S = #state{rtq=QPid}) when is_pid(QPid) ->
    MasterQ = generate_master_q(S),
    MasterQSize = length(MasterQ) * ?BINARIED_OBJ_SIZE,
    Bytes =  proplists:get_value(bytes, gen_server:call(S#state.rtq,
                                                        status, infinity)),
    case MasterQSize ==  Bytes of
        true ->
            DumpQ = gen_server:call(QPid, dumpq, infinity),
            compare_queues(MasterQ, DumpQ);
        false ->
            {size_mismatch, {expected, MasterQSize}, {actual, Bytes}}
    end;
invariant(_) ->
    true.

compare_queues(Expected, Actual) ->
    lists:foldl(fun({A, {Seq, Count, Bin, _MD}=B}, true) ->
                        case A#qed_item.seq == Seq andalso
                             A#qed_item.num_items == Count andalso
                             A#qed_item.item_list == binary_to_term(Bin) of
                            true ->
                                true;
                            false ->
                                {queue_mismatch, {expected, A}, {actual, B}}
                        end;
                   (_, Acc) ->
                        Acc
                end, true, lists:zip(Expected, Actual)).

get_first_routable(Client) ->
    #tc{tout = Tout, name = Name} = Client,
    SplitFun = fun(#qed_item{meta = Meta}) ->
        lists:member(Name, Meta)
    end,
    {_Skipped, NewOut} = lists:splitwith(SplitFun, Tout),
    NewOut.



get_queued_items(#state{cs = [], tout_no_clients = Items}) ->
    Items;
get_queued_items(#state{cs = Cs}) ->
    FoldFun = fun
        (#tc{tout = Tout}, Acc) when length(Tout) > length(Acc) ->
            Tout;
        (_, Acc) ->
            Acc
    end,
    lists:foldl(FoldFun, [], Cs).

set_meta(DataList, Key, Value) when is_list(DataList) ->
    set_meta({length(DataList), term_to_binary(DataList)}, Key, Value);
set_meta({A, B}, Key, Value) ->
    set_meta({A, B, []}, Key, Value);
set_meta({A, B, MetaDict}, Key, Value) ->
    MetaDict2 = orddict:store(Key, Value, MetaDict),
    {A, B, MetaDict2}.

routed_clusters([]) ->
    [];
routed_clusters(Consumers) ->
    Names = [C#tc.name || C <- Consumers],
    ?LET(NamesList, list(elements(Names)),
        ordsets:from_list(NamesList)
    ).

make_item() ->
    ?LAZY([make_ref()]).

push(List, Q) ->
    lager:info("pushed item ~p~n to ~p~n", [List, Q]),
    NumItems = length(List),
    Bin = term_to_binary(List),
    riak_repl2_rtq:push(NumItems, Bin).

push(List, RoutedClusters, _Q) ->
    NumItems = length(List),
    Bin = term_to_binary(List),
    riak_repl2_rtq:push(NumItems, Bin, [{routed_clusters, RoutedClusters}]).

new_consumer(Name, Q) ->
    lager:info("registering ~p to ~p~n", [Name, Q]),
    riak_repl2_rtq:register(Name).

rm_consumer(Name, _Q) ->
    lager:info("unregistering ~p", [Name]),
    riak_repl2_rtq:unregister(Name).

replace_consumer(Name, _Q) ->
    lager:info("replacing ~p", [Name]),
    riak_repl2_rtq:register(Name).

get_rtq_bytes(_Q) ->
    Stats = riak_repl2_rtq:status(),
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
    riak_repl2_rtq:pull(Name, F),
    receive
        {Ref, {Seq, Size, Item, Meta}} ->
            lager:info("~p got ~p size ~p seq ~p meta ~p~n", [Name, Item, Size, Seq, Meta]),
            Q ! {Ref, ok},
            {Seq, Size, binary_to_term(Item)};
        {Ref, _Wut} ->
            none
    after
        20 ->
            lager:info("queue empty: ~p~n", [Name]),
            none
    end.

ack({_Name, no_seq}, _Q) ->
    ok;
ack({Name, Seq}, _Q) ->
    riak_repl2_rtq:ack_sync(Name, Seq).

delete_client(Name, S) ->
    S#state{cs=lists:keydelete(Name, #tc.name, S#state.cs)}.

update_client(C, S) ->
    S#state{cs=[C|lists:keydelete(C#tc.name, #tc.name, S#state.cs)]}.

gen_seq(#tc{trec = []}) -> no_seq;
gen_seq(C) ->
    ?LET(E, elements(C#tc.trec), E#qed_item.seq).

generate_master_q(S) ->
    lists:foldl(fun(TC, Acc) ->
        #tc{tout = Tout, trec = Trec} = TC,
        NotDeliverFilter = fun(Qed) ->
            not lists:member(TC#tc.name, Qed#qed_item.meta)
        end,
        Tout2 = lists:filter(NotDeliverFilter, Tout),
        Trec2 = lists:filter(NotDeliverFilter, Trec),
        lists:umerge([Tout2, Trec2, Acc])
    end, S#state.tout_no_clients, S#state.cs).

trim(S) ->
    MasterQ = generate_master_q(S),
    case trim(MasterQ, S) of
        MasterQ ->
            S;
        Trimmed ->
            Dropped = MasterQ -- Trimmed,
            nuke_dropped(Dropped, S)
    end.

nuke_dropped(Dropped, S) ->
    MapFun = fun(TC) ->
        Tout = TC#tc.tout -- Dropped,
        Trec = TC#tc.trec -- Dropped,
        TC#tc{tout = Tout, trec = Trec}
    end,
    Clients = lists:map(MapFun, S#state.cs),
    S#state{cs = Clients}.

trim(Q, #state{max_bytes=Max}) ->
    {_Size, NewQ} = lists:foldl(fun(Item, {Size, Acc}) ->
                case (?BINARIED_OBJ_SIZE + Size) > Max of
                    true ->
                        {Size, Acc};
                    false ->
                        {Size + ?BINARIED_OBJ_SIZE, [Item|Acc]}
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

