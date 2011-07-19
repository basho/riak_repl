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
         pause_fullsync/1,
         resume_fullsync/1,
         status/1, status/2]).
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).
-export([send_peerinfo/2,
         wait_peerinfo/2,
         merkle_send/2,
         merkle_build/2,
         merkle_xfer/2,
         merkle_wait_ack/2,
         merkle_diff/2,
         connected/2]).
-type now() :: {integer(),integer(),integer()}.
-record(state, 
        {
          socket :: repl_socket(),       %% peer socket
          sitename :: repl_sitename(),   %% repl site identifier
          client :: tuple(),      %% local riak client
          my_pi :: #peer_info{},  %% peer info record 
          merkle_fp :: term(),    %% current merkle filedesc
          work_dir :: string(),   %% working directory for this repl session
          partitions=[] :: cancelled|list(),%% list of local partitions
          helper_pid :: undefined|pid(),            % riak_repl_fullsync_helper
                                                    % building merkle tree
          merkle_ref :: undefined|reference(),      % reference from
                                                    % riak_repl_fullsync_helper
          merkle_fn :: string(),                    % Filename for merkle tree
          merkle_fd,                                % Merkle file handle
          partition :: undefined|non_neg_integer(), % partition being syncd
          partition_start :: undefined|now(),       % start time for partition
          stage_start :: undefined|now(),           % start time for stage
          fullsync_ival :: undefined|disabled|non_neg_integer(),
          paused=false :: true|false,               % true if paused
          diff_vclocks=[],
          diff_recv,                                % differences receives from client
          diff_sent,                                % differences sent
          diff_errs,                                % errors retrieving different keys
          q :: undefined | bounded_queue:bounded_queue(),
          pending :: undefined | non_neg_integer(),
          max_pending :: undefined | pos_integer(),
          election_timeout :: undefined | reference() % reference for the election timeout
         }
       ).

start_link(SiteName) -> 
    gen_fsm:start_link(?MODULE, [SiteName], []).

start_fullsync(Pid) ->
    %% TODO: Make fullsync message tie into event system for consistency
    Pid ! fullsync.

cancel_fullsync(Pid) ->
    gen_fsm:send_event(Pid, cancel_fullsync).
    
pause_fullsync(Pid) ->
    gen_fsm:send_all_state_event(Pid, pause_fullsync).

resume_fullsync(Pid) ->
    gen_fsm:send_all_state_event(Pid, resume_fullsync).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_fsm:sync_send_all_state_event(Pid, status, Timeout).

set_socket(Pid, Socket) ->
    gen_fsm:sync_send_all_state_event(Pid, {set_socket, Socket}).
    
init([SiteName]) ->
    {ok, send_peerinfo, #state{sitename=SiteName}}.

listener_for_node(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplConfig = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, ReplConfig),
    NodeListeners = [L || L <- Listeners,
                          L#repl_listener.nodename =:= Node],
    hd(NodeListeners).

send_peerinfo(timeout, #state{socket=Socket, sitename=SiteName} = State) ->
    OurNode = node(),
    case riak_repl_leader:leader_node()  of
        undefined -> % leader not elected yet
            %% check again in 5 seconds
            erlang:send_after(5000, self(), election_wait),
            {next_state, send_peerinfo, State};
        OurNode ->
            erlang:cancel_timer(State#state.election_timeout),
            %% this switches the socket into active mode
            Props = riak_repl_fsm:common_init(Socket),
            PI = proplists:get_value(my_pi, Props),
            send(Socket, {peerinfo, PI}),
            {ok, WorkDir} = riak_repl_fsm:work_dir(Socket, SiteName),
            riak_repl_leader:add_receiver_pid(self()),
            {next_state, wait_peerinfo, State#state{work_dir = WorkDir,
                    client=proplists:get_value(client, Props),
                    election_timeout=undefined,
                    my_pi=PI}};
        OtherNode -> 
            OtherListener = listener_for_node(OtherNode),
            {Ip, Port} = OtherListener#repl_listener.listen_addr,
            send(Socket, {redirect, Ip, Port}),
            {stop, normal, State}
    end.

wait_peerinfo({peerinfo, TheirPeerInfo}, State) ->
    %% Forward compatibility with post-0.14.0 - will allow protocol negotiation
    %% rather than setting capabilities based on version
    Capability = riak_repl_util:capability_from_vsn(TheirPeerInfo),
    wait_peerinfo({peerinfo, TheirPeerInfo, Capability}, State);
wait_peerinfo({peerinfo, TheirPeerInfo, Capability},
              State=#state{my_pi=MyPeerInfo}) when is_list(Capability) ->
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
            %% Set up bounded queue if remote supports it
            case proplists:get_bool(bounded_queue, Capability) of
                true ->
                    QSize = app_helper:get_env(riak_repl,queue_size,
                                               ?REPL_DEFAULT_QUEUE_SIZE),
                    MaxPending = app_helper:get_env(riak_repl,server_max_pending,
                                                    ?REPL_DEFAULT_MAX_PENDING),
                    State1 = State#state{q = bounded_queue:new(QSize),
                                         max_pending = MaxPending,
                                         pending = 0};
                false ->
                    State1 = State
            end,
                
            case app_helper:get_env(riak_repl, fullsync_on_connect, true) of
                true ->
                    next_state(merkle_send, do_start_fullsync(State1));
                false ->
                    schedule_fullsync(State1),
                    next_state(connected, State1)
            end;
        false ->
            {stop, normal, State}
    end;
wait_peerinfo(cancel_fullsync, State) ->
    {next_state, wait_peerinfo, State}.

merkle_send(cancel_fullsync, State) ->
    next_state(merkle_send, do_cancel_fullsync(State));
merkle_send(timeout, State=#state{partitions=[], sitename=SiteName}) ->
    lager:info("Full-sync with site ~p completed.", [SiteName]),
    schedule_fullsync(State),
    riak_repl_stats:server_fullsyncs(),
    next_state(connected, State);
merkle_send(timeout, State=#state{partitions=cancelled}) ->
    lager:info("Full-sync with site ~p cancelled.",
                          [State#state.sitename]),
    schedule_fullsync(State),
    next_state(connected, State#state{partition_start = undefined,
                                      stage_start = undefined});
merkle_send(timeout, State=#state{paused=true}) ->
    %% If pause requested while previous partition was fullsyncing
    %% and there are partitions left, drop into connected state.
    %% Check after partitions=[] clause to make sure a fullsync completes
    %% if pause was on the last partition.
    lager:info("Full-sync with site ~p paused. ~p partitions pending.",
                          [State#state.sitename, length(State#state.partitions)]),
    next_state(connected, State);
merkle_send(timeout, State=#state{sitename=SiteName,
                                  partitions=[Partition|T],
                                  work_dir=WorkDir}) ->
    FileName = riak_repl_util:merkle_filename(WorkDir, Partition, ours),
    file:delete(FileName), % make sure we get a clean copy
    lager:info("Full-sync with site ~p; hashing partition ~p data",
                          [SiteName, Partition]),
    Now = now(),
    {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, Ref} = riak_repl_fullsync_helper:make_merkle(Pid, Partition, FileName),
    next_state(merkle_build, State#state{helper_pid = Pid, 
                                         merkle_ref = Ref,
                                         merkle_fn = FileName,
                                         partition = Partition,
                                         partition_start = Now,
                                         stage_start = Now,
                                         partitions = T}).

merkle_build(cancel_fullsync, State) ->
    next_state(merkle_build, do_cancel_fullsync(State));
merkle_build({Ref, merkle_built}, State=#state{merkle_ref = Ref,
                                               partitions = cancelled}) ->
    %% Partition sync was cancelled before transferring any data
    %% to the client, go back to the idle state.
    next_state(merkle_send, State#state{helper_pid = undefined,
                                        merkle_ref = undefined});
merkle_build({Ref, merkle_built}, State=#state{merkle_ref = Ref}) ->
    MerkleFile = State#state.merkle_fn,
    {ok, FileInfo} = file:read_file_info(MerkleFile),
    FileSize = FileInfo#file_info.size,
    {ok, MerkleFd} = file:open(MerkleFile, [read,raw,binary,read_ahead]),
    file:delete(MerkleFile), % will not be removed until file handle closed
    lager:info("Full-sync with site ~p; sending partition"
                          " ~p data (built in ~p secs)",
                          [State#state.sitename, State#state.partition,
                           elapsed_secs(State#state.stage_start)]),
    Now = now(),
    send(State#state.socket, {merkle, FileSize, State#state.partition}),
    next_state(merkle_xfer, State#state{helper_pid = undefined,
                                        merkle_ref = undefined,
                                        stage_start = Now,
                                        merkle_fd = MerkleFd});
merkle_build({Ref, {error, Reason}}, State) when Ref =:= State#state.merkle_ref ->
    lager:info("Full-sync with site ~p; partition ~p skipped: ~p",
                          [ State#state.sitename, State#state.partition, Reason]),
    next_state(merkle_send, State#state{helper_pid = undefined,
                                        merkle_ref = undefined,
                                        partition = undefined}).

merkle_xfer(cancel_fullsync, State) ->
    %% Even on cancel, keep sending the file.  The client reads until it has
    %% enough bytes, so stopping sending would leave it in
    %% riak_repl_tcp_client:merkle_recv.
    next_state(merkle_xfer,  do_cancel_fullsync(State));
merkle_xfer(timeout, State) ->
    MerkleFd = State#state.merkle_fd,
    case file:read(MerkleFd, ?MERKLE_CHUNKSZ) of
        {ok, Data} ->
            send(State#state.socket, {merk_chunk, Data}),
            next_state(merkle_xfer, State);
        eof ->
            file:close(MerkleFd),
            lager:info("Full-sync with site ~p; awaiting partition"
                                  " ~p diffs (sent in ~p secs)",
                                  [State#state.sitename, State#state.partition,
                                   elapsed_secs(State#state.stage_start)]),
            Now = now(),
            next_state(merkle_wait_ack, State#state{merkle_fd = undefined,
                                                    stage_start = Now})
    end.

merkle_wait_ack(cancel_fullsync, State) ->
    next_state(merkle_wait_ack,  do_cancel_fullsync(State));
merkle_wait_ack({ack,Partition,DiffVClocks}, 
                State=#state{partition=Partition}) ->
    next_state(merkle_diff, State#state{diff_vclocks=DiffVClocks,
                                        stage_start = now(),
                                        diff_sent = 0,
                                        diff_recv = 0,
                                        diff_errs = 0}).

merkle_diff(cancel_fullsync, State) ->
    next_state(merkle_diff, do_cancel_fullsync(State));
merkle_diff(timeout, #state{partitions=cancelled}=State) ->
    %% abandon the diff if the fullsync has been cancelled
    send(State#state.socket, {partition_complete, State#state.partition}),
    next_state(merkle_send, State#state{partition = undefined,
                                        diff_vclocks = [],
                                        diff_sent = undefined,
                                        diff_recv = undefined,
                                        diff_errs = undefined,
                                        stage_start = undefined});
merkle_diff(timeout, #state{diff_vclocks=[]}=State) ->
    send(State#state.socket, {partition_complete, State#state.partition}),
    DiffsSent = State#state.diff_sent,
    DiffsRecv = State#state.diff_recv,
    case DiffsRecv of
        N when is_integer(N), N > 0 ->
            Pct = 100 * DiffsSent div DiffsRecv;
        0 ->
            Pct = 0
    end,
    lager:info("Full-sync with site ~p; partition ~p complete (~p secs).\n"
                          "Updated ~p/~p (~p%) keys. ~p errors.",
                          [State#state.sitename, State#state.partition,
                           elapsed_secs(State#state.partition_start),
                           DiffsSent, DiffsRecv, Pct, State#state.diff_errs]),
    next_state(merkle_send, State#state{partition = undefined,
                                        partition_start = undefined,
                                        diff_sent = undefined,
                                        diff_recv = undefined,
                                        diff_errs = undefined,
                                        stage_start = undefined});
merkle_diff(timeout, #state{diff_vclocks=[{{B, K}, ClientVC} | Rest]}=State) ->
    Client = State#state.client,
    Recv = State#state.diff_recv,
    Sent = State#state.diff_sent,
    Errs  = State#state.diff_errs,
    case Client:get(B, K, 1, ?REPL_FSM_TIMEOUT) of
        {ok, RObj} ->
            case maybe_send(RObj, ClientVC, State#state.socket) of
                skipped ->
                    next_state(merkle_diff, State#state{diff_vclocks = Rest,
                                                        diff_recv = Recv + 1});
                _ ->
                    next_state(merkle_diff, State#state{diff_vclocks = Rest,
                                                        diff_recv = Recv + 1,
                                                        diff_sent = Sent + 1})
            end;
        {error, notfound} ->
            next_state(merkle_diff, State#state{diff_vclocks = Rest,
                                                diff_recv = Recv + 1});
        _ ->
            next_state(merkle_diff, State#state{diff_vclocks = Rest,
                                                diff_recv = Recv + 1,
                                                diff_errs = Errs + 1})
    end.

connected(_E, State) -> next_state(connected, State).

handle_info({tcp_closed, Socket}, _StateName, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, _StateName, State) ->
    {stop, normal, State};
handle_info({tcp, Socket, Data}, StateName, State=#state{socket=Socket,
                                                         pending=Pending}) ->
    Msg = binary_to_term(Data),
    Reply = case Msg of
        {q_ack, N} -> drain(StateName, State#state{pending=Pending-N});
        _ -> ?MODULE:StateName(Msg, State)
    end,
    inet:setopts(Socket, [{active, once}]),            
    riak_repl_stats:server_bytes_recv(size(Data)),
    Reply;
handle_info({repl, RObj}, StateName, State) when State#state.q == undefined ->
    send(State#state.socket, term_to_binary({diff_obj, RObj})),
    next_state(StateName, State);
handle_info({repl, RObj}, StateName, State) ->
    drain(StateName, enqueue(term_to_binary({diff_obj, RObj}), State));
handle_info(fullsync, connected, State) ->
    next_state(merkle_send, do_start_fullsync(State));
handle_info(election_timeout, _StateName,
            #state{election_timeout=Timer} =State) when is_reference(Timer) ->
    lager:error("Timed out waiting for a leader to be elected"),
    {stop, normal, State};
handle_info(election_wait, send_peerinfo, State) ->
    ?MODULE:send_peerinfo(timeout, State);
%% no-ops
handle_info(_I, StateName, State) -> next_state(StateName, State).
terminate(_Reason, _StateName, State) -> 
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

handle_event(resume_fullsync, StateName, State) ->
    NewState = State#state{paused = false},
    case fullsync_partitions_pending(NewState) andalso StateName =:= connected of
        true ->
            %% If in connected stated an there are pending partitions, drop back
            %% into merkle_send to resume the sync
            lager:info("Full-sync with site ~p resumed.  "
                                  "~p partitions.",
                                  [State#state.sitename,
                                   length(State#state.partitions)]),
            next_state(merkle_send, NewState);
        _ ->
            %% Otherwise there could have been a pause/resume while other work
            %% was being completed (e.g. during merkle_build).  Stay in the same
            %% state and the FSM will get to the right place.
            lager:info("Full-sync with site ~p resumed.",
                                  [State#state.sitename]),
            next_state(StateName, NewState)
    end;
handle_event(pause_fullsync, StateName, State) ->
    case State#state.partition of
        undefined ->
            lager:info("Full-sync with site ~p paused.",
                                  [State#state.sitename]);
        _ ->
            lager:info("Full-sync with site ~p pausing after next partition.",
                                  [State#state.sitename])
    end,
    next_state(StateName, State#state{paused = true}).

handle_sync_event({set_socket,Socket},_F, _StateName, State) -> 
    case application:get_env(riak_repl, fullsync_interval) of
        {ok, disabled} ->
            FullsyncIval = disabled;
        {ok, FullsyncIvalMins} ->
            FullsyncIval = timer:minutes(FullsyncIvalMins)
    end,
    NewState = State#state{
      socket=Socket,
      fullsync_ival=FullsyncIval,
      election_timeout=erlang:send_after(60000, self(), election_timeout)},
    reply(ok, send_peerinfo, NewState);
handle_sync_event(status,_F,StateName,State=#state{q=Q}) ->
    Desc = 
        [{site, State#state.sitename}] ++
        case State#state.partitions of
            [] ->
                [];
            cancelled ->
                [cancelled];
            Partitions ->
                Left = length(Partitions),
                [{fullsync, Left, left}]
        end ++
        case State#state.paused of
            true ->
                [paused];
            false ->
                []
        end ++
        [{state, StateName}],
    case Q of
        undefined ->
            Desc1 = Desc ++ [{bounded_queue, disabled}];
        _ ->
            Desc1 = Desc ++ 
                [{dropped_count, bounded_queue:dropped_count(Q)},
                 {queue_length, bounded_queue:len(Q)},
                 {queue_byte_size, bounded_queue:byte_size(Q)}]
    end,
    reply({status, Desc1}, StateName, State).

maybe_send(RObj, ClientVC, Socket) ->
    ServerVC = riak_object:vclock(RObj),
    case vclock:descends(ClientVC, ServerVC) of
        true ->
            skipped;
        false ->
            send(Socket, {diff_obj, RObj})
    end.

send(Sock,Data) when is_binary(Data) -> 
    R = gen_tcp:send(Sock,Data),
    riak_repl_stats:server_bytes_sent(size(Data)),
    R;
send(Sock,Data) -> 
    send(Sock, term_to_binary(Data)).

schedule_fullsync(State) ->
    case State#state.fullsync_ival of
        disabled ->
            ok;
        Interval ->
            erlang:send_after(Interval, self(), fullsync)
    end.

enqueue(Msg, State=#state{q=Q}) ->
    State#state{q=bounded_queue:in(Q,Msg)}.

send_diffobj(Msg,State=#state{socket=Socket,pending=Pending}) ->
    send(Socket,Msg),
    State#state{pending=Pending+1}.

drain(StateName,State=#state{q=Q,pending=P,max_pending=M}) when P < M ->
    case bounded_queue:out(Q) of
        {{value, Msg}, NewQ} ->
            drain(StateName, send_diffobj(Msg, State#state{q=NewQ}));
        {empty, NewQ} ->
            next_state(StateName, State#state{q=NewQ})
    end;
drain(StateName,State) ->
    next_state(StateName,State).
    
do_start_fullsync(State) ->
    case fullsync_partitions_pending(State) of
        true ->
            Partitions = State#state.partitions; % resuming from pause
        false ->
            %% last sync completed or was cancelled
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Partitions = riak_repl_util:get_partitions(Ring)
    end,
    Remaining = length(Partitions),
    lager:info("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    State#state{partitions=Partitions}.

do_cancel_fullsync(State) when is_list(State#state.partitions) ->
    Remaining = length(State#state.partitions),
    lager:info("Full-sync with site ~p cancelled; "
                          "~p partitions remaining.",
                          [State#state.sitename, Remaining]),
    State#state{partitions = cancelled};
do_cancel_fullsync(State) ->  % already cancelled
    lager:info("Full-sync with site ~p already cancelled.",
                          [State#state.sitename]),
    State.

%% Returns true if there are any fullsync partitions pending
fullsync_partitions_pending(State) ->
    case State#state.partitions of
        Ps when is_list(Ps), length(Ps) > 0 ->
            true;
        _ ->
            false
    end.
                 

%% Work out the elapsed time in seconds, rounded to centiseconds.
elapsed_secs(Then) ->
    CentiSecs = timer:now_diff(now(), Then) div 10000,
    CentiSecs / 100.0.
%% Make sure merkle_send and merkle_diff get sent timeout messages
%% to process their queued work
next_state(StateName, State) when StateName =:= merkle_send;
                                  StateName =:= merkle_xfer;
                                  StateName =:= merkle_diff ->
    {next_state, StateName, State, 0};
next_state(StateName, State) ->
    {next_state, StateName, State}.

reply(Reply, StateName, State) when StateName =:= merkle_send;
                                    StateName =:= merkle_xfer;
                                    StateName =:= merkle_diff;
                                    StateName =:= send_peerinfo ->
    {reply, Reply, StateName, State, 0};
reply(Reply, StateName, State) ->
    {reply, Reply, StateName, State}.
