%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_tcp_client).
-author('Andy Gross <andy@basho.com').
-include("riak_repl.hrl").
-behaviour(gen_fsm).
-export([start_link/3]).
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).
-export([wait_peerinfo/2,
         merkle_recv/2,
         merkle_diff/2,
         merkle_exchange/2]).

-record(state, {
          socket :: port(), 
          sitename :: string(),
          connector_pid :: pid(),
          client :: tuple(),
          my_pi :: #peer_info{},
          partitions=[] :: list(),
          work_dir :: string(),
          merkle_pt :: non_neg_integer(),
          merkle_fp :: term(),
          merkle_their_fn :: string(),
          merkle_their_sz :: non_neg_integer(),
          merkle_our_fn :: string(),
          merkle_ref :: undefined | reference(),
          helper_pid :: undefined | pid(),
          bkey_vclocks=[] :: [{any(),any()}]
         }).

start_link(Socket, SiteName, ConnectorPid) -> 
    gen_fsm:start_link(?MODULE, [Socket, SiteName, ConnectorPid], []).
    

init([Socket, SiteName, ConnectorPid]) ->
    %io:format("~p starting, sock=~p, site=~p, pid=~p~n", 
    %          [?MODULE, Socket, SiteName, self()]),
    gen_tcp:send(Socket, SiteName),
    Props = riak_repl_fsm:common_init(Socket, SiteName),
    State = #state{
      socket=Socket, 
      sitename=SiteName,
      connector_pid=ConnectorPid,
      work_dir=proplists:get_value(work_dir, Props),
      client=proplists:get_value(client, Props),
      my_pi=proplists:get_value(my_pi, Props),
      partitions=proplists:get_value(partitions, Props)},
    send(Socket, {peerinfo, State#state.my_pi}),
    {ok, wait_peerinfo, State}.

wait_peerinfo({redirect, IP, Port}, State=#state{connector_pid=P}) ->
    P ! {redirect, IP, Port},
    {stop, normal, State};
wait_peerinfo({peerinfo, TheirPeerInfo}, State=#state{my_pi=MyPeerInfo, 
                                                      sitename=SiteName}) ->
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            update_site_ips(riak_repl_ring:get_repl_config(
                              TheirPeerInfo#peer_info.ring), SiteName),
            {next_state, merkle_exchange, State};
        false ->
            error_logger:error_msg("Replication (client) - invalid peer_info ~p~n",
                                   [TheirPeerInfo]),
            {stop, normal, State}
    end;
wait_peerinfo({diff_obj, Obj}, State) ->
    riak_repl_util:do_repl_put(Obj),
    {next_state, wait_peerinfo, State}.
merkle_exchange({merkle,Size,Partition},State=#state{work_dir=WorkDir}) ->
    %% Kick off the merkle build in parallel with receiving the remote
    %% file
    OurFn = riak_repl_util:merkle_filename(WorkDir, Partition, ours),
    file:delete(OurFn), % make sure we get a clean copy
    error_logger:info_msg("Full-sync with site ~p (client); hashing "
                          "partition ~p data\n",
                          [State#state.sitename, Partition]),
    {ok, Pid} = riak_repl_merkle_helper:start_link(self()),
    case riak_repl_merkle_helper:make_merkle(Pid, Partition, OurFn) of
        {ok, Ref} ->
            HelperPid = Pid,
            OurFn2 = OurFn;
        {error, Reason} ->
            error_logger:info_msg("Full-sync with site ~p (client); hashing "
                                  "partition ~p data failed: ~p\n",
                                  [State#state.sitename, Partition, Reason]),
            %% No good way to cancel the send in the current protocol
            %% just accept the data and return empty list of diffs
            Ref = undefined,
            HelperPid = undefined,
            OurFn2 = undefined
    end,
    TheirFn = riak_repl_util:merkle_filename(WorkDir, Partition, theirs),
    {ok, FP} = file:open(TheirFn, [write, raw, binary, delayed_write]),
    {next_state, merkle_recv, State#state{merkle_fp=FP, 
                                          merkle_their_fn=TheirFn,
                                          merkle_their_sz=Size, 
                                          merkle_pt=Partition,
                                          merkle_our_fn = OurFn2,
                                          helper_pid = HelperPid,
                                          merkle_ref = Ref}};


merkle_exchange({partition_complete,_Partition}, State) ->
    {next_state, merkle_exchange, State};
merkle_exchange({diff_obj, Obj}, State) ->
    riak_repl_util:do_repl_put(Obj),
    {next_state, merkle_exchange, State}.    

merkle_recv({diff_obj, Obj}, State) ->
    riak_repl_util:do_repl_put(Obj),
    {next_state, merkle_recv, State};   
merkle_recv({merk_chunk, Data}, State=#state{merkle_fp=FP, merkle_their_sz=SZ}) ->
    ok = file:write(FP, Data),
    LeftBytes = SZ - size(Data),
    case LeftBytes of
        0 ->
            ok = file:sync(FP),
            ok = file:close(FP),
            merkle_recv_next(State#state{merkle_fp = undefined});
        _ ->
            {next_state, merkle_recv, State#state{merkle_their_sz=LeftBytes}}
    end;
merkle_recv({Ref, merkle_built}, State=#state{merkle_ref = Ref}) ->
    merkle_recv_next(State#state{merkle_ref = undefined,
                                 helper_pid = undefined});
merkle_recv({Ref, {error, Reason}}, State=#state{merkle_ref = Ref}) ->
    error_logger:info_msg("Full-sync with site ~p (client); hashing "
                          "partition ~p data failed: ~p\n",
                          [State#state.sitename, State#state.merkle_pt, Reason]),
    merkle_recv_next(State#state{merkle_our_fn = undefined,
                                 helper_pid = undefined,
                                 merkle_ref = undefined}).


merkle_diff({diff_obj, Obj}, State) -> % *not* from the merkle diff
    riak_repl_util:do_repl_put(Obj),
    {next_state, merkle_diff, State};
merkle_diff({Ref, {merkle_diff, BkeyVclock}}, State=#state{merkle_ref = Ref}) ->
    {next_state, merkle_diff,
     State#state{bkey_vclocks = [BkeyVclock | State#state.bkey_vclocks]}};
merkle_diff({Ref, {error, Reason}}, State=#state{merkle_ref = Ref}) ->
    send(State#state.socket, {ack, State#state.merkle_pt, []}),
    error_logger:error_msg("Full-sync with site ~p (client); vclock lookup for "
                           "partition ~p failed: ~p. Skipping partition.\n",
                           [State#state.sitename, State#state.merkle_pt,
                            Reason]),
    {next_state, merkle_exchange, State#state{bkey_vclocks=[]}};
merkle_diff({Ref, merkle_done}, State=#state{merkle_ref = Ref}) ->
    send(State#state.socket,
         {ack, State#state.merkle_pt, State#state.bkey_vclocks}),
    {next_state, merkle_exchange, State#state{bkey_vclocks=[]}}.

handle_info({tcp_closed, _Socket}, _StateName, State) ->
    {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, _StateName, State) ->
    {stop, normal, State};
handle_info({tcp, Socket, Data}, StateName, State=#state{socket=Socket}) ->
    R = ?MODULE:StateName(binary_to_term(Data), State),
    inet:setopts(Socket, [{active, once}]),            
    riak_repl_stats:client_bytes_recv(size(Data)),
    R;
%% no-ops
handle_info(_I, StateName, State) ->  {next_state, StateName, State}.
terminate(_Reason, _StateName, _State) ->  ok.
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
handle_event(_Event, StateName, State) -> {next_state, StateName, State}.
handle_sync_event(_Ev, _F, StateName, State) -> {reply, ok, StateName, State}.

send(Socket, Data) when is_binary(Data) -> 
    R = gen_tcp:send(Socket, Data),
    riak_repl_stats:client_bytes_sent(size(Data)),
    R;
send(Socket, Data) ->
    send(Socket, term_to_binary(Data)).

update_site_ips(TheirReplConfig, SiteName) ->
    {ok, OurRing} = riak_core_ring_manager:get_my_ring(),
    MyListeners = dict:fetch(listeners, riak_repl_ring:get_repl_config(OurRing)),
    MyIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- MyListeners]),
    TheirListeners = dict:fetch(listeners, TheirReplConfig),
    TheirIPAddrs = sets:from_list([R#repl_listener.listen_addr || R <- TheirListeners]),
    ToRemove = sets:subtract(MyIPAddrs, TheirIPAddrs),
    ToAdd = sets:subtract(TheirIPAddrs, MyIPAddrs),
    OurRing1 = lists:foldl(fun(E,A) -> riak_repl_ring:del_site_addr(A, SiteName, E) end,
                           OurRing, sets:to_list(ToRemove)),
    OurRing2 = lists:foldl(fun(E,A) -> riak_repl_ring:add_site_addr(A, SiteName, E) end,
                           OurRing1, sets:to_list(ToAdd)),
    MyNewRC = riak_repl_ring:get_repl_config(OurRing2),
    F = fun(InRing, ReplConfig) ->
                {new_ring, riak_repl_ring:set_repl_config(InRing, ReplConfig)}
        end,
    riak_core_ring_manager:ring_trans(F, MyNewRC),
    ok.    


%% Decide when it is time to leave the merkle_recv state and whether
%% to go ahead with the diff (in merkle_diff) or on error, just ack
%% and return to merkle_exchange.    
merkle_recv_next(#state{merkle_ref = undefined, merkle_fp = undefined}=State) ->
    TheirFn = State#state.merkle_their_fn, 
    OurFn = State#state.merkle_our_fn,
    case TheirFn =:= undefined orelse OurFn =:= undefined of
        true ->
            %% Something has gone wrong, just ack the server
            %% as the protocol currently has no way to report errors
            send(State#state.socket, {ack, State#state.merkle_pt, []}),
            {next_state, merkle_exchange, State};
        false ->
            {ok, Pid} = riak_repl_merkle_helper:start_link(self()),
            {ok, Ref} = riak_repl_merkle_helper:diff(Pid, State#state.merkle_pt,
                                                     TheirFn, OurFn),
            {next_state, merkle_diff, State#state{helper_pid=Pid,
                                                  merkle_ref=Ref}}
    end;
merkle_recv_next(State) ->
    {next_state, merkle_recv, State}.
