%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_syncv1_server).
-behaviour(gen_fsm).

%% @doc This is the legacy fullsync behaviour from riak 1.0 and previous. It
%% is not recommended for use when 1.1 or greater is available on both sides
%% of the replication site.
%%
%% This strategy first builds an on-disk merkle tree for the first partition
%% in the list  which it sends to the client. It then sends the file over the
%% wire to the client. When the client receives this file, it immediately
%% converts it to a keylist (because the merkle tree is not actually reliable,
%% but kept for backwards compatability). Then, the client builds its own
%% keylist for this partition. Once the client has both keylists, it diffs
%% them, and sends the *entire* list of differences in one message back to the
%% server. The difference list includes the vclock on the client side. The
%% server then traverses this difference list, checking the vclock of the key
%% with the one it has locally. If the server's vclock is not an ancestor of
%% the vclock received from the client, it sends the new object over the wire
%% using the same protocol as the realtime replication. Once the list of
%% differences are exhausted, it proceeds onto the next partition, until no
%% more remain, at which point the fullsync is complete, and the connection
%% waits for the next scheduled fullsync to occur.

-include("riak_repl.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([start_link/4, start_fullsync/1, cancel_fullsync/1, pause_fullsync/1,
    resume_fullsync/1]).

%% gen_fsm
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

%% states
-export([wait_for_fullsync/2,
         merkle_send/2,
         merkle_build/2,
         merkle_xfer/2,
         merkle_wait_ack/2,
         merkle_diff/2]).

-record(state, {
        sitename :: repl_sitename(),
        socket :: repl_socket(),
        client,
        work_dir,
        helper_pid,
        partition,
        partition_start,
        partitions = [],
        paused = false,
        merkle_ref,
        merkle_fn,
        merkle_fd,
        stage_start,
        diff_vclocks=[],
        diff_recv,                                % differences receives from client
        diff_sent,                                % differences sent
        diff_errs                                 % errors retrieving different keys
    }).

start_link(SiteName, Socket, WorkDir, Client) ->
    gen_fsm:start_link(?MODULE, [SiteName, Socket, WorkDir, Client], []).

start_fullsync(Pid) ->
    Pid ! start_fullsync.

cancel_fullsync(Pid) ->
    gen_fsm:send_event(Pid, cancel_fullsync).

pause_fullsync(Pid) ->
    gen_fsm:send_all_state_event(Pid, pause_fullsync).

resume_fullsync(Pid) ->
    gen_fsm:send_all_state_event(Pid, resume_fullsync).

init([SiteName, Socket, WorkDir, Client]) ->
    State0 = #state{sitename=SiteName, socket=Socket,
        work_dir=WorkDir, client=Client},
    riak_repl_util:schedule_fullsync(),
    State = case application:get_env({progress, SiteName}) of
        {ok, Partitions} when is_list(Partitions) ->
            lager:info("Resuming incomplete fullsync for ~p, ~p partitions remain",
                [SiteName, length(Partitions)]),
            State0#state{partitions=Partitions};
        _ ->
            State0
    end,
    {ok, wait_for_fullsync, State}.

wait_for_fullsync(cancel_fullsync, State) ->
    next_state(wait_for_fullsync,
        do_cancel_fullsync(State#state{paused=false}));
wait_for_fullsync(start_fullsync, State) ->
    lager:info("Full-sync with ~p starting.", [State#state.sitename]),
    next_state(merkle_send, do_start_fullsync(State)).

merkle_send(cancel_fullsync, State) ->
    next_state(merkle_send, do_cancel_fullsync(State));
merkle_send(timeout, State=#state{partitions=[], sitename=SiteName}) ->
    lager:info("Full-sync with site ~p completed.", [SiteName]),
    %% no longer need to track progress.
    application:unset_env(riak_repl, {progress, SiteName}),
    riak_repl_util:schedule_fullsync(),
    riak_repl_stats:server_fullsyncs(),
    next_state(wait_for_fullsync, State);
merkle_send(timeout, State=#state{partitions=cancelled}) ->
    lager:info("Full-sync with site ~p cancelled.",
                          [State#state.sitename]),
    riak_repl_util:schedule_fullsync(),
    next_state(wait_for_fullsync, State#state{partition_start = undefined,
            stage_start = undefined});
merkle_send(timeout, State=#state{paused=true}) ->
    %% If pause requested while previous partition was fullsyncing
    %% and there are partitions left, drop into connected state.
    %% Check after partitions=[] clause to make sure a fullsync completes
    %% if pause was on the last partition.
    lager:info("Full-sync with site ~p paused. ~p partitions pending.",
                          [State#state.sitename, length(State#state.partitions)]),
    {next_state, merkle_send, State};
merkle_send(timeout, State=#state{sitename=SiteName,
                                  partitions=[Partition|T],
                                  work_dir=WorkDir}) ->
    %% update the stored progress, in case the fullsync aborts
    application:set_env(riak_repl, {progress, SiteName}, [Partition|T]),
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
    riak_repl_tcp_server:send(State#state.socket, {merkle, FileSize, State#state.partition}),
    next_state(merkle_xfer, State#state{helper_pid = undefined,
                                        merkle_ref = undefined,
                                        stage_start = Now,
                                        merkle_fd = MerkleFd});
merkle_build({Ref, {error, Reason}}, State) when Ref =:= State#state.merkle_ref ->
    lager:info("Full-sync with site ~p; partition ~p skipped: ~p",
        [State#state.sitename, State#state.partition, Reason]),
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
            riak_repl_tcp_server:send(State#state.socket, {merk_chunk, Data}),
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
    riak_repl_tcp_server:send(State#state.socket, {partition_complete, State#state.partition}),
    next_state(merkle_send, State#state{partition = undefined,
                                        diff_vclocks = [],
                                        diff_sent = undefined,
                                        diff_recv = undefined,
                                        diff_errs = undefined,
                                        stage_start = undefined});
merkle_diff(timeout, #state{diff_vclocks=[]}=State) ->
    riak_repl_tcp_server:send(State#state.socket, {partition_complete, State#state.partition}),
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
            case maybe_send(RObj, ClientVC, State#state.socket, Client) of
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

%% gen_fsm callbacks

handle_event(resume_fullsync, StateName, #state{paused=true} = State) ->
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
    next_state(StateName, State#state{paused = true});
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Desc = 
        [{site, State#state.sitename}] ++
        case State#state.partitions of
            [] ->
                [];
            cancelled ->
                [cancelled];
            Partitions ->
                Left = length(Partitions),
                [
                    {fullsync, Left, left},
                    {partition_start,
                        riak_repl_util:elapsed_secs(State#state.partition_start)},
                    {stage_start,
                        riak_repl_util:elapsed_secs(State#state.stage_start)}
                ]
        end ++
        case State#state.paused of
            true ->
                [paused];
            false ->
                []
        end ++
        [{state, StateName}],
    reply(Desc, StateName, State);
handle_sync_event(_Event,_F,StateName,State) ->
    reply(ok, StateName, State).

handle_info(start_fullsync, wait_for_fullsync, State) ->
    gen_fsm:send_event(self(), start_fullsync),
    next_state(wait_for_fullsync, State);
handle_info(_I, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) -> 
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% internal funtions

do_start_fullsync(State) ->
    case fullsync_partitions_pending(State) of
        true ->
            Partitions = State#state.partitions; % resuming from pause
        false ->
            %% last sync completed or was cancelled
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            Partitions0 = riak_repl_util:get_partitions(Ring),
            Partitions = case app_helper:get_env(riak_repl, shuffle_ring, true) of
                true ->
                    %% randomly shuffle the partitions so that if we
                    %% restart, we have a good chance of not re-doing
                    %% partitions we already synced
                    riak_repl_util:shuffle_partitions(Partitions0, now());
                _ ->
                    Partitions0
            end
    end,
    Remaining = length(Partitions),
    lager:info("Full-sync with site ~p starting; ~p partitions.",
                          [State#state.sitename, Remaining]),
    State#state{partitions=Partitions}.

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

do_cancel_fullsync(State) when is_list(State#state.partitions) ->
    %% clear the tracked progress since we're cancelling
    application:unset_env(riak_repl, {progress, State#state.sitename}),
    Remaining = length(State#state.partitions),
    lager:info("Full-sync with site ~p cancelled; "
                          "~p partitions remaining.",
                          [State#state.sitename, Remaining]),
    State#state{partitions = cancelled, paused=false};
do_cancel_fullsync(State) ->  % already cancelled
    lager:info("Full-sync with site ~p already cancelled.",
                          [State#state.sitename]),
    State.

%% Work out the elapsed time in seconds, rounded to centiseconds.
elapsed_secs(Then) ->
    CentiSecs = timer:now_diff(now(), Then) div 10000,
    CentiSecs / 100.0.

maybe_send(RObj, ClientVC, Socket, Client) ->
    ServerVC = riak_object:vclock(RObj),
    case vclock:descends(ClientVC, ServerVC) of
        true ->
            skipped;
        false ->
            case riak_repl_util:repl_helper_send(RObj, Client) of
                cancel ->
                    skipped;
                Objects when is_list(Objects) ->
                    [riak_repl_tcp_server:send(Socket, {diff_obj, O}) || O <- Objects],
                    riak_repl_tcp_server:send(Socket, {diff_obj, RObj})
            end
    end.

%% Returns true if there are any fullsync partitions pending
fullsync_partitions_pending(State) ->
    case State#state.partitions of
        Ps when is_list(Ps), length(Ps) > 0 ->
            true;
        _ ->
            false
    end.

