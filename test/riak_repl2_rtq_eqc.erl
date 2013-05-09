%%
%% EQC test for RTQ
%%

-module(riak_repl2_rtq_eqc).
-compile(export_all).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-record(state, {rtq,
                items=0, %% Number of items created
                tout_no_clients = [], % No clients available to pull
                pcs=[a, b, c, d, e, f, g],
                cs=[],
                max_bytes=0}).

%% Test consumer record
-record(tc, {name,       %% Name
             tout=[],    %% outstanding items in queue
             trec=[],    %% received items
             tack=[]}).    %% acked items


-ifdef(TEST).
rtq_test_() ->
    {timeout, 30,
     fun() ->
                ?assert(eqc:quickcheck(eqc:testing_time(25,
                                                        ?MODULE:prop_main())))
        end
    }.
-endif.


max_bytes() ->
    ?LET(MaxBytes, nat(), (MaxBytes+1) * 512).

prop_main() ->
    %?FORALL({Cmds, Bytes}, {noshrink(commands(?MODULE)),
    ?FORALL(Cmds, commands(?MODULE),
            aggregate(command_names(Cmds),
                      begin
                      ok = meck:new(riak_repl_stats, [passthrough]),
                      ok = meck:expect(riak_repl_stats, rt_source_errors,
                                       fun() -> ok end),
                      ok = meck:expect(riak_repl_stats, rt_sink_errors,
                                       fun() -> ok end),
                      {H, S, Res} = run_commands(?MODULE,Cmds),
                      meck:unload(riak_repl_stats),
                      catch(exit(S#state.rtq, kill)),
                      pretty_commands(?MODULE, Cmds, {H,S,Res}, Res==ok)
                      end)).

%% ====================================================================
%% eqc_statem callbacks
%% ====================================================================

initial_state() ->
    #state{}.

%% start the RTQ *and* set the max bytes for the queue
test_init(MaxBytes) ->
    application:set_env(riak_repl, rtq_max_bytes, MaxBytes),
    riak_repl2_rtq:start_test().

command(#state{rtq=undefined}) ->
        {call, ?MODULE, test_init, [max_bytes()]};
command(S) ->
    oneof([{call, ?MODULE, push, [make_item(), S#state.rtq]}] ++
          [{call, ?MODULE, new_consumer, [elements(S#state.pcs), S#state.rtq]} ||
            S#state.pcs /= []] ++
          [{call, ?MODULE, rm_consumer, [elements(S#state.cs), S#state.rtq]} ||
            S#state.cs /= []] ++
          [{call, ?MODULE, replace_consumer, [elements(S#state.cs), S#state.rtq]} ||
            S#state.cs /= []] ++
          [{call, ?MODULE, pull, [elements(S#state.cs), S#state.rtq]} ||
            S#state.cs /= []] ++
          [{call, ?MODULE, ack, [
                    ?LET(C, elements(S#state.cs),
                         {C, gen_seq(C)}), S#state.rtq]} ||
            S#state.cs /= []] ++
          []
    ).


precondition(S,{call,riak_repl2_rtq,start_test,_}) ->
    S#state.rtq == undefined;
precondition(S,{call,?MODULE,new_consumer, [Name, _]}) ->
    lists:member(Name, S#state.pcs);
precondition(S,{call,?MODULE,pull, [Name, _]}) ->
    %not lists:member(Name, S#state.pcs);
    lists:member(Name, S#state.pcs);
precondition(_S,{call,_,_,_}) ->
    true.


postcondition(_S,{call,?MODULE,pull,[C, _]},R) ->
    %Client = find_client(Name, S),
    case R of
        none ->
            C#tc.tout == [] orelse {not_empty, C#tc.name, C#tc.tout};
        {_Seq, Size, Item} ->
            hd(C#tc.tout) == {Size, Item} orelse {not_match, C#tc.name, hd(C#tc.tout),
            {Size, Item}}
    end;
postcondition(S,{call,?MODULE,push,[_Item, Q]},_R) ->
    % guarantee that the queue size never grows above max_bytes
    lists:foldl(fun(_TC, Acc) ->
                QBytes = get_rtq_bytes(Q),
                ((S#state.max_bytes =< QBytes) == Acc)
        end, true, S#state.cs);
postcondition(_S,{call,_,_,_},_R) ->
    true.

next_state(S,V,{call, _, test_init, [_B]}) ->
    S#state{rtq={call, erlang, element, [2, V]}};
next_state(S,_V,{call, _, new_consumer, [Name, _Q]}) ->
    %% IF there's other clients, this client's outstanding messages will
    %% be the longest REC+OUT queue from one of the other consumers.
    %% Otherwise use the tout_no_clients value and wipe from the state.
    Tout = lists:foldl(fun(#tc{trec = Trec, tout=Tout}, Longest) ->
                    Q = strip_seq(Trec) ++ Tout,
                    lager:info("Q is ~p~n", [Trec]),
                     %case qbytes(QTab) > MaxBytes 
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
next_state(S,_V,{call, _, rm_consumer, [Client, _Q]}) ->
    delete_client(Client, S#state{pcs=[Client#tc.name|S#state.pcs]});
next_state(S,_V,{call, _, replace_consumer, [Client, _Q]}) ->
    %% anything we didn't ack will be considered dropped by the queue
    NewClient = Client#tc{tack=[],
                          trec=[],
                          tout=strip_seq(Client#tc.trec)
                          ++ Client#tc.tout},
    update_client(NewClient, S);
next_state(S,_V,{call, _, push, [Item, _Q]}) ->
    case S#state.cs of
        [] ->
            S#state{tout_no_clients=S#state.tout_no_clients ++ [Item]};
        _ ->
            Clients = lists:map(fun(TC) ->
                            TC#tc{tout=TC#tc.tout ++ [Item]}
                    end, S#state.cs),
            S#state{cs=Clients}
    end;
next_state(S,V,{call, _, pull, [Client, _Q]}) ->
    %lager:info("tout is ~p~n", [Client#tc.tout]),
    {Tout, Trec} = case Client#tc.tout of
        [] ->
            %% nothing to get
            {[], Client#tc.trec};
        T ->
            {tl(T), Client#tc.trec ++ [V]}
    end,
    %lager:info("trec ~p, tout ~p~n", [Trec, Tout]),
    update_client(Client#tc{tout=Tout, trec=Trec}, S);
next_state(S,_V,{call, _, ack, [{Client,N}, _Q]}) ->
    case Client#tc.trec of
        [] ->
            S;
        _ ->
            {H, T} = lists:split(N, Client#tc.trec),
            update_client(Client#tc{trec=T,
                                    tack=Client#tc.tack ++ H}, S)
    end;
next_state(S,_V,{call, _, _, _}) ->
    S.

make_item() ->
    ?LAZY({1, term_to_binary([make_ref()])}).

push({NumItems, Bin}, Q) ->
    lager:info("pushed item ~p~n to ~p~n", [Bin, Q]),
    gen_server:call(Q, {push, NumItems, Bin}).

new_consumer(Name, Q) ->
    lager:info("registering ~p to ~p~n", [Name, Q]),
    gen_server:call(Q, {register, Name}).

rm_consumer(#tc{name=Name}, Q) ->
    lager:info("unregistering ~p", [Name]),
    gen_server:call(Q, {unregister, Name}).

replace_consumer(#tc{name=Name}, Q) ->
    lager:info("replacing ~p", [Name]),
    gen_server:call(Q, {register, Name}).

get_rtq_bytes(Q) ->
    Stats = gen_server:call(Q, status),
    proplists:get_value(bytes, Stats).


pull(#tc{name=Name}, Q) ->
    lager:info("~p pulling from ~p~n", [Name, Q]),
    Ref = make_ref(),
    Self = self(),
    F = fun(Item) ->
            Self ! {Ref, Item},
            receive
                {Ref, ok} ->
                    ok
            after
                1000 ->
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
            {Seq, Size, Item}
    after
        1000 ->
            lager:info("queue empty: ~p~n", [Name]),
            none
    end.

ack({C=#tc{name=Name}, N}, Q) ->
    case C#tc.trec of
        [] ->
            lager:info("~p has nothing to ACK~n", [Name]),
            none;
        Trec ->
            {H, _} = lists:split(N, Trec),
            {Seq, _, _} = I = lists:last(H),
            lager:info("~p ACKing ~p (~p of ~p)~n", [Name, Seq, N, length(Trec)]),
            gen_server:call(Q, {ack_sync, Name, Seq}),
            I
    end.

delete_client(C, S) ->
    S#state{cs=lists:keydelete(C#tc.name, 2, S#state.cs)}.

update_client(C, S) ->
    S#state{cs=[C|lists:keydelete(C#tc.name, 2, S#state.cs)]}.

strip_seq(Trec) ->
    [{{call, erlang, element, [2, T]}, {call, erlang, element, [3, T]}} ||
        T <- Trec].

gen_seq(C) ->
    case length(C#tc.trec) of
        0 ->
            0;
        N ->
            choose(1, N)
    end.

-endif.

