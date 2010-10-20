%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_tcp_server).
-author('Andy Gross <andy@basho.com').
-include("riak_repl.hrl").
-include_lib("kernel/include/file.hrl").
-behaviour(gen_fsm).
-export([start_link/1,
         set_socket/2,
         start_fullsync/1,
         cancel_fullsync/1,
         status/1, status/2]).
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).
-export([wait_peerinfo/2,
         merkle_send/2,
         merkle_build/2,
         merkle_build_cancelled/2,
         merkle_wait_ack/2,
         connected/2]).
-record(state, 
        {
          socket :: repl_socket(),       %% peer socket
          sitename :: repl_sitename(),   %% repl site identifier
          client :: tuple(),      %% local riak client
          my_pi :: #peer_info{},  %% peer info record 
          merkle_fp :: term(),    %% current merkle filedesc
          work_dir :: string(),   %% working directory for this repl session
          partitions=[] :: cancelled|list(),%% list of local partitions
          helper_pid :: undefined|pid(),            % riak_repl_merkle_helper
                                                    % building merkle tree
          merkle_ref :: undefined|reference(),      % reference from
                                                    % riak_repl_merkle_helper
          merkle_fn :: string(),                    % Filename for merkle tree
          merkle_fd,                                % Merkle file handle
          partition :: undefined|non_neg_integer(), % partition being syncd
          fullsync_ival :: undefined|disabled|non_neg_integer()
         }
       ).

start_link(SiteName) -> 
    gen_fsm:start_link(?MODULE, [SiteName], []).

start_fullsync(Pid) ->
    %% TODO: Make fullsync message tie into event system for consistency
    Pid ! fullsync.

cancel_fullsync(Pid) ->
    gen_fsm:send_event(Pid, cancel_fullsync).
    
status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_fsm:sync_send_all_state_event(Pid, status, Timeout).

set_socket(Pid, Socket) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_socket, Socket}).
    
init([SiteName]) ->
    {ok, wait_peerinfo, #state{sitename=SiteName}}.

maybe_redirect(Socket, PeerInfo) ->
    OurNode = node(),
    case riak_repl_leader:leader_node()  of
        OurNode ->
            send(Socket, {peerinfo, PeerInfo});
        OtherNode -> 
            OtherListener = listener_for_node(OtherNode),
            {Ip, Port} = OtherListener#repl_listener.listen_addr,
            send(Socket, {redirect, Ip, Port}),
            redirect
    end.

listener_for_node(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplConfig = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, ReplConfig),
    NodeListeners = [L || L <- Listeners,
                          L#repl_listener.nodename =:= Node],
    hd(NodeListeners).

wait_peerinfo({peerinfo, TheirPeerInfo}, State=#state{my_pi=MyPeerInfo}) ->
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            case app_helper:get_env(riak_repl, fullsync_on_connect, true) of
                true ->
                    next_state(merkle_send, do_start_fullsync(State));
                false ->
                    schedule_fullsync(State),
                    next_state(connected, State)
            end;
        false ->
            {stop, normal, State}
    end;
wait_peerinfo(cancel_fullsync, State) ->
    {next_state, wait_peerinfo, State}.

merkle_send(cancel_fullsync, State) ->
    next_state(connected, do_cancel_fullsync(State));
merkle_send(timeout, State=#state{partitions=[], sitename=SiteName}) ->
    error_logger:info_msg("Full-sync with site ~p complete.\n", [SiteName]),
    schedule_fullsync(State),
    riak_repl_stats:server_fullsyncs(),
    next_state(connected, State);
merkle_send(timeout, State=#state{partitions=cancelled}) ->
    next_state(connected, State);
merkle_send(timeout, State=#state{sitename=SiteName,
                                  partitions=[Partition|T],
                                  work_dir=WorkDir}) ->
    FileName = riak_repl_util:merkle_filename(WorkDir, Partition, ours),
    file:delete(FileName), % make sure we get a clean copy
    error_logger:info_msg("Full-sync with site ~p; hashing partition ~p data\n",
                          [SiteName, Partition]),
    {ok, Pid} = riak_repl_merkle_helper:start_link(self()),
    case riak_repl_merkle_helper:make_merkle(Pid, Partition, FileName) of
        {ok, Ref} ->
            next_state(merkle_build, State#state{helper_pid = Pid, 
                                                 merkle_ref = Ref,
                                                 merkle_fn = FileName,
                                                 partition = Partition,
                                                 partitions = T});
        {error, Reason} ->
            error_logger:info_msg("Full-sync ~p with ~p skipped: ~p\n",
                                  [Partition, SiteName, Reason]),
            next_state(merkle_send, State#state{helper_pid = undefined,
                                                merkle_ref = undefined,
                                                partition = undefined,
                                                partitions = T})
    end.

merkle_build(cancel_fullsync, State) ->
    %% Cancel will send us a {Ref, {error, cancelled}} event - may
    %% generate exception if the helper has already completed and exited.
    %% Queue up an event for ourselves so we know we can consume
    %% all messages from it and then really clean up.
    catch riak_repl_merkle_helper:cancel(State#state.helper_pid),
    gen_fsm:send_event(self(), finalize_cancel),
    next_state(merkle_build_cancelled, State);
merkle_build({Ref, merkle_built}, State=#state{merkle_ref = Ref}) ->
    MerkleFile = State#state.merkle_fn,
    {ok, FileInfo} = file:read_file_info(MerkleFile),
    FileSize = FileInfo#file_info.size,
    {ok, MerkleFd} = file:open(MerkleFile, [read,raw,binary,read_ahead]),
    file:delete(MerkleFile), % will not be removed until file handle closed
    error_logger:info_msg("Full-sync with site ~p; sending partition ~p data\n",
                          [State#state.sitename, State#state.partition]),
    send(State#state.socket, {merkle, FileSize, State#state.partition}),
    gen_fsm:send_event(self(), next_chunk),
    next_state(merkle_build, State#state{helper_pid = undefined,
                                         merkle_ref = undefined,
                                         merkle_fd = MerkleFd});
merkle_build({Ref, {error, Reason}}, State) when Ref =:= State#state.merkle_ref ->
    error_logger:info_msg("Full-sync with site ~p; partition ~p skipped: ~p\n",
                          [ State#state.sitename, State#state.partition, Reason]),
    next_state(merkle_send, State#state{helper_pid = undefined,
                                        merkle_ref = undefined,
                                        partition = undefined});
merkle_build(next_chunk, State) ->
    MerkleFd = State#state.merkle_fd,
    case file:read(MerkleFd, ?MERKLE_CHUNKSZ) of
        {ok, Data} ->
            send(State#state.socket, {merk_chunk, Data}),
            gen_fsm:send_event(self(), next_chunk),
            next_state(merkle_build, State);
        eof ->
            file:close(MerkleFd),
            next_state(merkle_wait_ack, State#state{merkle_fd = undefined})
    end.

merkle_build_cancelled({Ref, _}, State=#state{merkle_ref = Ref}) ->
    %% message from riak_repl_merkle_helper in its death throws
    next_state(merkle_build_cancelled, State);
merkle_build_cancelled(finalize_cancel, State) ->
    case State#state.merkle_fd of
        undefined ->
            ok;
        Fd ->
            file:close(Fd)
    end,
    NewState = State#state{helper_pid = undefined,
                           merkle_ref = undefined,
                           merkle_fd = undefined},
    next_state(connected, do_cancel_fullsync(NewState)).

merkle_wait_ack(cancel_fullsync, State) ->
    next_state(merkle_wait_ack,  do_cancel_fullsync(State));
merkle_wait_ack({ack, _Partition, []}, State) ->
    next_state(merkle_send, State);
merkle_wait_ack({ack, _Partition, _}, #state{partitions=cancelled}=State) ->
    next_state(merkle_send, State);
merkle_wait_ack({ack,Partition,DiffVClocks}, State=#state{socket=Socket}) ->
    vclock_diff(Partition, DiffVClocks, State),
    send(Socket, {partition_complete, Partition}),
    error_logger:info_msg("Full-sync with site ~p; partition ~p complete\n",
                          [State#state.sitename, State#state.partition]),
    next_state(merkle_send, State).

connected(_E, State) -> next_state(connected, State).

handle_info({tcp_closed, Socket}, _StateName, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, _StateName, State) ->
    {stop, normal, State};
handle_info({tcp, Socket, Data}, StateName, State=#state{socket=Socket}) ->
    R = ?MODULE:StateName(binary_to_term(Data), State),
    inet:setopts(Socket, [{active, once}]),            
    riak_repl_stats:server_bytes_recv(size(Data)),
    R;
handle_info({repl, RObj}, StateName, State=#state{socket=Socket}) ->
    send(Socket, {diff_obj, RObj}),
    next_state(StateName, State);
handle_info(fullsync, connected, State) ->
    next_state(merkle_send, do_start_fullsync(State));
%% no-ops
handle_info(_I, StateName, State) -> next_state(StateName, State).
terminate(_Reason, _StateName, _State) -> ok.
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
handle_event(_E, StateName, State) -> next_state(StateName, State).

handle_sync_event({set_socket,Socket},_F, _StateName,
                  State=#state{sitename=SiteName}) -> 
    case application:get_env(riak_repl, fullsync_interval) of
        {ok, disabled} ->
            FullsyncIval = disabled;
        {ok, FullsyncIvalMins} ->
            FullsyncIval = timer:minutes(FullsyncIvalMins)
    end,
    Props = riak_repl_fsm:common_init(Socket, SiteName),
    NewState = State#state{
      socket=Socket,
      client=proplists:get_value(client, Props),
      my_pi=proplists:get_value(my_pi, Props),
      work_dir=proplists:get_value(work_dir,Props),
      fullsync_ival=FullsyncIval},
    case maybe_redirect(Socket,  NewState#state.my_pi) of
        ok ->
            riak_repl_leader:add_receiver_pid(self()),
            reply(ok, wait_peerinfo, NewState);
        redirect ->
            {stop, normal, ok, NewState}
    end;
handle_sync_event(status,_F,StateName,State) ->
    case StateName of
        SN when SN =:= merkle_send;
                SN =:= merkle_build;
                SN =:= merkle_build_cancelled;
                SN =:= merkle_wait_ack ->
            case State#state.partitions of
                cancelled ->
                    Desc = {fullsync, cancelled};
                _ ->
                    Left = length(State#state.partitions),
                    Desc = {fullsync, Left, left}
                end;
        _ ->
            Desc = StateName
    end,
    reply({status, Desc}, StateName, State).

send(Sock,Data) when is_binary(Data) -> 
    R = gen_tcp:send(Sock,Data),
    riak_repl_stats:server_bytes_sent(size(Data)),
    R;
send(Sock,Data) -> 
    send(Sock, term_to_binary(Data)).

vclock_diff(Partition, DiffVClocks, #state{client=Client, socket=Socket}) ->
    Keys = [K || {K, _V} <- DiffVClocks],
    case riak_repl_fsm:get_vclocks(Partition, Keys) of
        {error, node_not_available} ->
            [];
        {error, Reason} ->
            error_logger:error_msg("~p:getting vclocks for ~p:~p~n",
                                   [?MODULE, Partition, Reason]),
            [];
        OurVClocks ->
            vclock_diff1(DiffVClocks, OurVClocks, Client, Socket, 0)
    end.

vclock_diff1([],_,_,_,Count) -> Count;
vclock_diff1([{K,VC}|T], OurVClocks, Client, Socket, Count) ->
    case proplists:get_value(K, OurVClocks) of
        undefined -> vclock_diff1(T, OurVClocks, Client, Socket, Count);
        VC -> vclock_diff1(T, OurVClocks, Client, Socket, Count);
        OurVClock -> 
            maybe_send(K, OurVClock, VC, Client, Socket),
            vclock_diff1(T, OurVClocks, Client, Socket, Count+1)
    end.

maybe_send(BKey, V1, V2, Client, Socket) ->
    case vclock:descends(V2, V1) of 
        true -> nop; 
        false -> ok = do_send(BKey, Client, Socket) end.
            
do_send({B,K}, Client, Socket) ->
    case Client:get(B, K, 1, ?REPL_FSM_TIMEOUT) of
        {ok, Obj} -> send(Socket, {diff_obj, Obj});
        _ -> ok
    end.

schedule_fullsync(State) ->
    case State#state.fullsync_ival of
        disabled ->
            ok;
        Interval ->
            erlang:send_after(Interval, self(), fullsync)
    end.

do_start_fullsync(State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Partitions = riak_repl_util:get_partitions(Ring),
    Remaining = length(Partitions),
    error_logger:info_msg("Full-sync with site ~p starting; ~p partitions.\n",
                          [State#state.sitename, Remaining]),
    State#state{partitions=Partitions}.

do_cancel_fullsync(State) when is_list(State#state.partitions) ->
    Remaining = length(State#state.partitions),
    error_logger:info_msg("Full-sync with site ~p cancelled; "
                          "~p partitions remaining.\n",
                          [State#state.sitename, Remaining]),
    schedule_fullsync(State),
    State#state{partition = undefined,
                partitions = cancelled};
do_cancel_fullsync(State) ->  % already cancelled
    State.

next_state(merkle_send, State) ->
    {next_state, merkle_send, State, 0};
next_state(StateName, State) ->
    {next_state, StateName, State}.

reply(Reply, merkle_send, State) ->
    {reply, Reply, merkle_send, State, 0};
reply(Reply, StateName, State) ->
    {reply, Reply, StateName, State}.

