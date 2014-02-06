%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_syncv1_client).
-behaviour(gen_fsm).

%% @doc This is the client-side module for the riak 1.0 and earlier legacy
%% replication strategy. See the repl_syncv1_server module for an explanation
%% of the protocol.

-include("riak_repl.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([start_link/4]).

%% gen_fsm
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).

%% states
-export([merkle_exchange/2,
         merkle_recv/2,
         merkle_diff/2]).

-record(state, {
        sitename,
        work_dir,
        socket,
        transport,
        merkle_fn,
        merkle_pt,
        merkle_fp,
        their_merkle_fn,
        their_merkle_sz,
        our_kl_fn,
        our_kl_pid,
        our_kl_ref,
        their_kl_fn,
        their_kl_pid,
        their_kl_ref,
        bkey_vclocks = [],
        diff_pid
    }).

start_link(SiteName, Transport, Socket, WorkDir) ->
    gen_fsm:start_link(?MODULE, [SiteName, Transport, Socket, WorkDir], []).

init([SiteName, Transport, Socket, WorkDir]) ->
    {ok, merkle_exchange,
        #state{sitename=SiteName,transport=Transport,
            socket=Socket,work_dir=WorkDir}}.

merkle_exchange({merkle,Size,Partition},State=#state{work_dir=WorkDir}) ->
    %% Kick off the merkle build in parallel with receiving the remote
    %% file
    OurKeyListFn = riak_repl_util:keylist_filename(WorkDir, Partition, ours),
    file:delete(OurKeyListFn), % make sure we get a clean copy
    lager:info("Full-sync with site ~p; hashing "
                          "partition ~p data",
                          [State#state.sitename, Partition]),
    {ok, OurKeyListPid} = riak_repl_fullsync_helper:start_link(self()),
    {ok, OurKeyListRef} = riak_repl_fullsync_helper:make_keylist(OurKeyListPid,
                                                                 Partition,
                                                                 OurKeyListFn),
    TheirMerkleFn = riak_repl_util:merkle_filename(WorkDir, Partition, theirs),
    TheirKeyListFn = riak_repl_util:keylist_filename(WorkDir, Partition, theirs),
    {ok, FP} = file:open(TheirMerkleFn, [write, raw, binary]),
    {next_state, merkle_recv, State#state{merkle_fp=FP, 
                                          their_merkle_fn=TheirMerkleFn,
                                          their_merkle_sz=Size, 
                                          merkle_pt=Partition,
                                          their_kl_fn = TheirKeyListFn,
                                          their_kl_ref = pending,
                                          our_kl_fn = OurKeyListFn,
                                          our_kl_pid = OurKeyListPid,
                                          our_kl_ref = OurKeyListRef}};
merkle_exchange({partition_complete,_Partition}, State) ->
    {next_state, merkle_exchange, State}.

merkle_recv({merk_chunk, Data}, State=#state{merkle_fp=FP, their_merkle_sz=SZ}) ->
    ok = file:write(FP, Data),
    LeftBytes = SZ - size(Data),
    case LeftBytes of
        0 ->
            ok = file:sync(FP),
            ok = file:close(FP),
            {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),
            {ok, Ref} = riak_repl_fullsync_helper:merkle_to_keylist(Pid,
                           State#state.their_merkle_fn, State#state.their_kl_fn),
            {next_state, merkle_recv, State#state{merkle_fp = undefined,
                                                  their_kl_pid = Pid,
                                                  their_kl_ref = Ref}};
        _ ->
            {next_state, merkle_recv, State#state{their_merkle_sz=LeftBytes}}
    end;
merkle_recv({Ref, keylist_built, _}, State=#state{our_kl_ref = Ref}) ->
    merkle_recv_next(State#state{our_kl_ref = undefined,
                                 our_kl_pid = undefined});
merkle_recv({Ref, {error, Reason}}, State=#state{our_kl_ref = Ref}) ->
    lager:info("Full-sync with site ~p; hashing "
                          "partition ~p data failed: ~p",
                          [State#state.sitename, State#state.merkle_pt, Reason]),
    merkle_recv_next(State#state{our_kl_fn = undefined,
                                 our_kl_pid = undefined,
                                 our_kl_ref = undefined});
merkle_recv({Ref, converted}, State=#state{their_kl_ref = Ref}) ->
    file:delete(State#state.their_merkle_fn),
    merkle_recv_next(State#state{their_merkle_fn = undefined,
                                 their_kl_ref = undefined,
                                 their_kl_pid = undefined});
merkle_recv({Ref, {error, Reason}}, State=#state{their_kl_ref = Ref}) ->
    lager:info("Full-sync with site ~p; converting btree "
                          "partition ~p data failed: ~p",
                          [State#state.sitename, State#state.merkle_pt, Reason]),
    merkle_recv_next(State#state{their_kl_fn = undefined,
                                 their_kl_pid = undefined,
                                 their_kl_ref = undefined}).

merkle_diff({Ref, {merkle_diff, BkeyVclock}}, State=#state{our_kl_ref = Ref}) ->
    {next_state, merkle_diff,
     State#state{bkey_vclocks = [BkeyVclock | State#state.bkey_vclocks]}};
merkle_diff({Ref, {error, Reason}}, State=#state{our_kl_ref = Ref}) ->
    _ = riak_repl_tcp_client:send(State#state.transport, State#state.socket,
        {ack, State#state.merkle_pt, []}),
    lager:error("Full-sync with site ~p; vclock lookup for "
                           "partition ~p failed: ~p. Skipping partition.",
                           [State#state.sitename, State#state.merkle_pt,
                            Reason]),
    {next_state, merkle_exchange, 
     cleanup_partition(State#state{bkey_vclocks=[],
                                   diff_pid = undefined,
                                   our_kl_ref = Ref})};
merkle_diff({Ref, diff_done}, State=#state{our_kl_ref = Ref}) ->
    _ = riak_repl_tcp_client:send(State#state.transport, State#state.socket,
         {ack, State#state.merkle_pt, State#state.bkey_vclocks}),
    {next_state, merkle_exchange, 
     cleanup_partition(State#state{bkey_vclocks=[],
                                   diff_pid = undefined,
                                   our_kl_ref = Ref})}.
%% gen_fsm callbacks

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, State) ->
    Desc =
        [{site, State#state.sitename}] ++
        case State#state.merkle_pt of
            undefined ->
                [];
            Partition ->
                [
                    {fullsync, Partition}
                ]
        end ++
        [{state, StateName}],
    {reply, Desc, StateName, State};
handle_sync_event(_Event,_F,StateName,State) ->
    {reply, ok, StateName, State}.

handle_info(_I, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, State) -> 
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% internal functions

%% Decide when it is time to leave the merkle_recv state and whether
%% to go ahead with the diff (in merkle_diff) or on error, just ack
%% and return to merkle_exchange.    
merkle_recv_next(#state{our_kl_ref = undefined, 
                        their_kl_ref = undefined}=State) ->
    TheirFn = State#state.their_kl_fn, 
    OurFn = State#state.our_kl_fn,
    case TheirFn =:= undefined orelse OurFn =:= undefined of
        true ->
            %% Something has gone wrong, just ack the server
            %% as the protocol currently has no way to report errors
            _ = riak_repl_tcp_client:send(State#state.transport,
                State#state.socket, {ack, State#state.merkle_pt, []}),
            {next_state, merkle_exchange, cleanup_partition(State)};
        false ->
            {ok, Pid} = riak_repl_fullsync_helper:start_link(self()),
            {ok, Ref} = riak_repl_fullsync_helper:diff(Pid, State#state.merkle_pt,
                                                     TheirFn, OurFn),
            {next_state, merkle_diff, State#state{diff_pid=Pid,
                                                  our_kl_ref=Ref}}
    end;
merkle_recv_next(State) ->
    {next_state, merkle_recv, State}.

cleanup_partition(State) ->
    case State#state.merkle_fp of
        undefined ->
            ok;
        Fp ->
            file:close(Fp)
    end,
    case State#state.their_merkle_fn of
        undefined ->
            ok;
        TheirMerkleFn ->
            file:delete(TheirMerkleFn)
    end,
    case State#state.their_kl_fn of
        undefined ->
            ok;
        TheirKlFn ->
            file:delete(TheirKlFn)
    end,
    case State#state.our_kl_fn of
        undefined ->
            ok;
        OurKlFn ->
            file:delete(OurKlFn)
    end,
    State#state{merkle_pt = undefined,
                merkle_fp = undefined,
                their_merkle_fn = undefined,
                their_kl_fn = undefined,
                our_kl_fn = undefined}.
