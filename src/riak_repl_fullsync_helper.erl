%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%
%% Fullsync helper process - offloads key/hash generation and comparison
%% from the main TCP server/client processes.
%%
-module(riak_repl_fullsync_helper).
-behaviour(gen_server2).

%% API
-export([start_link/1,
         stop/1,
         make_merkle/3,
         make_keylist/3,
         merkle_to_keylist/3,
         diff/4,
         diff_stream/5,
         itr_new/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% HOFs
-export([merkle_fold/3, keylist_fold/3]).

-include("riak_repl.hrl").
-include("couch_db.hrl").

-record(state, {owner_fsm,
                ref,
                merkle_pid,
                folder_pid,
                kl_fp,
                kl_total,
                filename,
                buf=[],
                size=0}).

-record(diff_state, {fsm,
                     ref,
                     preflist,
                     count = 0,
                     replies = 0,
                     diff_hash = 0,
                     missing = 0,
                     need_vclocks = true,
                     errors = []}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(OwnerFsm) ->
    gen_server2:start_link(?MODULE, [OwnerFsm], []).

stop(Pid) ->
    gen_server2:call(Pid, stop, infinity).

%% Make a couch_btree of key/object hashes.
%%
%% Return {ok, Ref} if build starts successfully, then sends
%% a gen_fsm event {Ref, merkle_built} to the OwnerFsm or
%% a {Ref, {error, Reason}} event on failures
make_merkle(Pid, Partition, Filename) ->
    gen_server2:call(Pid, {make_merkle, Partition, Filename}).

%% Make a sorted file of key/object hashes.
%% 
%% Return {ok, Ref} if build starts successfully, then sends
%% a gen_fsm event {Ref, keylist_built} to the OwnerFsm or
%% a {Ref, {error, Reason}} event on failures
make_keylist(Pid, Partition, Filename) ->
    gen_server2:call(Pid, {make_keylist, Partition, Filename}).
   
%% Convert a couch_btree to a sorted keylist file.
%%
%% Returns {ok, Ref} or {error, Reason}.
%% Sends a gen_fsm event {Ref, converted} on success or
%% {Ref, {error, Reason}} on failure
merkle_to_keylist(Pid, MerkleFn, KeyListFn) ->
    gen_server2:call(Pid, {merkle_to_keylist, MerkleFn, KeyListFn}).
    
%% Computes the difference between two keylist sorted files.
%% Returns {ok, Ref} or {error, Reason}
%% Differences are sent as {Ref, {merkle_diff, {Bkey, Vclock}}}
%% and finally {Ref, diff_done}.  Any errors as {Ref, {error, Reason}}.
diff(Pid, Partition, TheirFn, OurFn) ->
    gen_server2:call(Pid, {diff, Partition, TheirFn, OurFn, -1, true}).

diff_stream(Pid, Partition, TheirFn, OurFn, Count) ->
    gen_server2:call(Pid, {diff, Partition, TheirFn, OurFn, Count, false}).

%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([OwnerFsm]) ->
    process_flag(trap_exit, true),
    {ok, #state{owner_fsm = OwnerFsm}}.

handle_call(stop, _From, State) ->
    case State#state.folder_pid of
        undefined ->
            ok;
        Pid ->
            unlink(Pid),
            exit(Pid, kill)
    end,
    file:close(State#state.kl_fp),
    couch_merkle:close(State#state.merkle_pid),
    file:delete(State#state.filename),
    {stop, normal, ok, State};
handle_call({make_merkle, Partition, FileName}, From, State) ->
    %% Return to caller immediately - under heavy load exceeded the 5s
    %% default timeout.  Do not wish to block the repl server for
    %% that long in any case.
    Ref = make_ref(),
    gen_server2:reply(From, {ok, Ref}),

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    OwnerNode = riak_core_ring:index_owner(Ring, Partition),
    case lists:member(OwnerNode, riak_core_node_watcher:nodes(riak_kv)) of
        true ->
            {ok, DMerkle} = couch_merkle:open(FileName),
            Self = self(),
            Worker = fun() ->
                             riak_kv_vnode:fold({Partition,OwnerNode},
                                                fun ?MODULE:merkle_fold/3, Self),
                             gen_server2:cast(Self, merkle_finish)
                     end,
            FolderPid = spawn_link(Worker),
            NewState = State#state{ref = Ref, 
                                   merkle_pid = DMerkle, 
                                   folder_pid = FolderPid,
                                   filename = FileName},
            {noreply, NewState};
        false ->
            gen_fsm:send_event(State#state.owner_fsm, {Ref, {error, node_not_available}}),
            {stop, normal, State}
    end;
%% request from client of server to write a keylist of hashed key/value to Filename for Partition
handle_call({make_keylist, Partition, Filename}, From, State) ->
    Ref = make_ref(),
    gen_server2:reply(From, {ok, Ref}),

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    OwnerNode = riak_core_ring:index_owner(Ring, Partition),
    case lists:member(OwnerNode, riak_core_node_watcher:nodes(riak_kv)) of
        true ->
            {ok, FP} = file:open(Filename, [raw, write, binary, delayed_write]),
            Self = self(),
            Worker = fun() ->
                             %% Spend as little time on the vnode as possible,
                             %% accept there could be a potentially huge message queue
                             {Self, _, Total} = riak_kv_vnode:fold({Partition,OwnerNode},
                                 fun ?MODULE:keylist_fold/3, {Self, 0, 0}),
                             gen_server2:cast(Self, {kl_finish, Total})
                     end,
            FolderPid = spawn_link(Worker),
            NewState = State#state{ref = Ref, 
                                   folder_pid = FolderPid,
                                   filename = Filename,
                                   kl_fp = FP},
            {noreply, NewState};
        false ->
            gen_fsm:send_event(State#state.owner_fsm, {Ref, {error, node_not_available}}),
            {stop, normal, State}
    end;
%% sent from keylist_fold every 100 key/value hashes
handle_call(keylist_ack, _From, State) ->
    {reply, ok, State};
handle_call({merkle_to_keylist, MerkleFn, KeyListFn}, From, State) ->
    %% Return to the caller immediately, if we are unable to open/
    %% write to files this process will crash and the caller
    %% will discover the problem.
    Ref = make_ref(),
    gen_server2:reply(From, {ok, Ref}),

    %% Iterate over the couch file and write out to the keyfile
    {ok, InFileFd, InFileBtree} = open_couchdb(MerkleFn),
    {ok, OutFile} = file:open(KeyListFn, [binary, write, raw, delayed_write]),
    couch_btree:foldl(InFileBtree, fun({K, V}, _Acc) ->
                                           B = term_to_binary({K, V}),
                                           ok = file:write(OutFile, <<(size(B)):32, B/binary>>),
                                           {ok, ok}
                                   end, ok),
    couch_file:close(InFileFd),
    file:close(OutFile),

    %% Verify the file is really sorted
    case file_sorter:check(KeyListFn) of
        {ok, []} ->
            Msg = converted;
        {ok, Result} ->
            Msg = {error, {unsorted, Result}};
        {error, Reason} ->
            Msg = {error, Reason}
    end,
    gen_fsm:send_event(State#state.owner_fsm, {Ref, Msg}),
    {stop, normal, State};
handle_call({diff, Partition, RemoteFilename, LocalFilename, Count, NeedVClocks}, From, State) ->
    %% Return to the caller immediately, if we are unable to open/
    %% read files this process will crash and the caller
    %% will discover the problem.
    Ref = make_ref(),
    gen_server2:reply(From, {ok, Ref}),
    try
        {ok, RemoteFile} = file:open(RemoteFilename,
            [read, binary, raw, read_ahead]),
        {ok, LocalFile} = file:open(LocalFilename,
            [read, binary, raw, read_ahead]),
        try
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            OwnerNode = riak_core_ring:index_owner(Ring, Partition),
            case lists:member(OwnerNode, riak_core_node_watcher:nodes(riak_kv)) of
                true ->
                    DiffState = diff_keys(itr_new(RemoteFile, remote_reads),
                                        itr_new(LocalFile, local_reads),
                                        #diff_state{fsm = State#state.owner_fsm,
                                                    count=Count,
                                                    replies=Count,
                                                    ref = Ref,
                                                    need_vclocks = NeedVClocks,
                                                    preflist = {Partition, OwnerNode}}),
                    lager:info("Partition ~p: ~p remote / ~p local: ~p missing, ~p differences.",
                                        [Partition, 
                                        erlang:get(remote_reads),
                                        erlang:get(local_reads),
                                        DiffState#diff_state.missing,
                                        DiffState#diff_state.diff_hash]),
                    case DiffState#diff_state.errors of
                        [] ->
                            ok;
                        Errors ->
                            lager:error("Partition ~p: Read Errors.",
                                                [Partition, Errors])
                    end,
                    gen_fsm:send_event(State#state.owner_fsm, {Ref, diff_done});
                false ->
                    gen_fsm:send_event(State#state.owner_fsm, {Ref, {error, node_not_available}})
            end
        after
            file:close(RemoteFile),
            file:close(LocalFile)
        end
    after
        file:delete(RemoteFilename),
        file:delete(LocalFilename)
    end,

    {stop, normal, State}.

handle_cast({merkle, K, H}, State) ->
    PackedKey = pack_key(K),
    NewSize = State#state.size+size(PackedKey)+4,
    NewBuf = [{PackedKey, H}|State#state.buf],
    case NewSize >= ?MERKLE_BUFSZ of 
        true ->
            couch_merkle:update_many(State#state.merkle_pid, NewBuf),
            {noreply, State#state{buf = [], size = 0}};
        false ->
            {noreply, State#state{buf = NewBuf, size = NewSize}}
    end;
%% write Key/Value Hash to file
handle_cast({keylist, Row}, State) ->
    ok = file:write(State#state.kl_fp, <<(size(Row)):32, Row/binary>>),
    {noreply, State};
handle_cast(merkle_finish, State) ->
    couch_merkle:update_many(State#state.merkle_pid, State#state.buf),
    %% Close couch - beware, the close call is a cast so the process
    %% may still be alive for a while.  Add a monitor and directly
    %% receive the message - it should only be for a short time
    %% and nothing else 
    _Mref = erlang:monitor(process, State#state.merkle_pid),
    couch_merkle:close(State#state.merkle_pid),
    {noreply, State};
handle_cast({kl_finish, Count}, State) ->
    %% delayed_write can mean sync/close might not work the first time around
    %% because of a previous error that is only now being reported. In this case,
    %% call close again. See http://www.erlang.org/doc/man/file.html#open-2
    case file:sync(State#state.kl_fp) of
        ok -> ok;
        _ -> file:sync(State#state.kl_fp)
    end,
    case file:close(State#state.kl_fp) of
        ok -> ok;
        _ -> file:close(State#state.kl_fp)
    end,
    gen_server2:cast(self(), kl_sort),
    {noreply, State#state{kl_total=Count}};
handle_cast(kl_sort, State) ->
    Filename = State#state.filename,
    %% we want the GC to stop running, so set a giant heap size
    %% this process is about to die, so this is OK
    lager:info("Sorting keylist ~p", [Filename]),
    erlang:process_flag(min_heap_size, 1000000),
    {ElapsedUsec, ok} = timer:tc(file_sorter, sort, [Filename]),
    lager:info("Sorted ~s of ~p keys in ~.2f seconds",
                          [Filename, State#state.kl_total, ElapsedUsec / 1000000]),
    gen_fsm:send_event(State#state.owner_fsm, {State#state.ref, keylist_built,
        State#state.kl_total}),
    {stop, normal, State}.

handle_info({'EXIT', Pid,  Reason}, State) when Pid =:= State#state.merkle_pid ->
    case Reason of 
        normal ->
            {noreply, State};
        _ ->
            gen_fsm:send_event(State#state.owner_fsm, 
                               {State#state.ref, {error, {merkle_died, Reason}}}),
            {stop, normal, State}
    end;
handle_info({'EXIT', Pid,  Reason}, State) when Pid =:= State#state.folder_pid ->
    case Reason of
        normal ->
            {noreply, State};
        _ ->
            gen_fsm:send_event(State#state.owner_fsm, 
                               {State#state.ref, {error, {folder_died, Reason}}}),
            {stop, normal, State}
    end;
handle_info({'EXIT', _Pid,  _Reason}, State) ->
    %% The calling repl_tcp_server/client has gone away, so should we
    {stop, normal, State};
handle_info({'DOWN', _Mref, process, Pid, Exit}, State=#state{merkle_pid = Pid}) ->
    case Exit of
        normal ->
            Msg = {State#state.ref, merkle_built};
        _ ->
            Msg = {State#state.ref, {error, {merkle_failed, Exit}}}
    end,
    gen_fsm:send_event(State#state.owner_fsm, Msg),        
    {stop, normal, State#state{buf = [], size = 0}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

pack_key(K) ->
    riak_repl_util:binpack_bkey(K).

unpack_key(K) ->
    riak_repl_util:binunpack_bkey(K).

%% Hash an object, making sure the vclock is in sorted order
%% as it varies depending on whether it has been pruned or not
hash_object(RObjBin) ->
    RObj = binary_to_term(RObjBin),
    Vclock = riak_object:vclock(RObj),
    UpdObj = riak_object:set_vclock(RObj, lists:sort(Vclock)),
    erlang:phash2(term_to_binary(UpdObj)).

open_couchdb(Filename) ->
    {ok, Fd} = couch_file:open(Filename),
    {ok, #db_header{local_docs_btree_state=HeaderBtree}} = couch_file:read_header(Fd),
    {ok, Btree} =  couch_btree:open(HeaderBtree, Fd),
    {ok, Fd, Btree}.

itr_new(File, Tag) ->
    erlang:put(Tag, 0),
    case file:read(File, 4) of
        {ok, <<Size:32/unsigned>>} ->
            itr_next(Size, File, Tag);
        _ ->
            file:close(File),
            eof
    end.

itr_next(Size, File, Tag) ->
    case file:read(File, Size + 4) of
        {ok, <<Data:Size/bytes>>} ->
            erlang:put(Tag, erlang:get(Tag) + 1),
            file:close(File),
            {binary_to_term(Data), fun() -> eof end};
        {ok, <<Data:Size/bytes, NextSize:32/unsigned>>} ->
            erlang:put(Tag, erlang:get(Tag) + 1),
            {binary_to_term(Data), fun() -> itr_next(NextSize, File, Tag) end};
        eof ->
            file:close(File),
            eof
    end.

diff_keys(R, L, #diff_state{replies=0, fsm=FSM, ref=Ref, count=Count} = DiffState) 
  when Count /= 0 ->
    gen_fsm:send_event(FSM, {Ref, diff_paused}),
    %% wait for a message telling us to stop, or to continue.
    %% TODO do this more correctly when there's more time.
    receive
        {'$gen_call', From, stop} ->
            gen_server2:reply(From, ok),
            DiffState;
        {Ref, diff_resume} ->
            %% Resuming the diff stream generation
            diff_keys(R, L, DiffState#diff_state{replies=Count})
    end;
diff_keys({{Key, Hash}, RNext}, {{Key, Hash}, LNext}, DiffState) ->
    %% Remote and local keys/hashes match
    diff_keys(RNext(), LNext(), DiffState);
diff_keys({{Key, _}, RNext}, {{Key, _}, LNext}, DiffState) ->
    %% Both keys match, but hashes do not
    diff_keys(RNext(), LNext(), diff_hash(Key, DiffState));
diff_keys({{RKey, _RHash}, RNext}, {{LKey, _LHash}, _LNext} = L, DiffState)
  when RKey < LKey ->
    diff_keys(RNext(), L, missing_key(RKey, DiffState));
diff_keys({{RKey, _RHash}, _RNext} = R, {{LKey, _LHash}, LNext}, DiffState)
  when RKey > LKey ->
    %% Remote is ahead of local list
    %% TODO: This may represent a deleted key...
    diff_keys(R, LNext(), DiffState);
diff_keys({{RKey, _RHash}, RNext}, eof, DiffState) ->
    %% End of local stream; all keys from remote should be processed
    diff_keys(RNext(), eof, missing_key(RKey, DiffState));
diff_keys(eof, _, DiffState) ->
    %% End of remote stream; all remaining keys are local to this side or
    %% deleted ops
    DiffState.

%% Called when the hashes differ with the packed bkey
diff_hash(PBKey, DiffState = #diff_state{need_vclocks=false, fsm=FSM, ref=Ref}) ->
    BKey = unpack_key(PBKey),
    gen_fsm:send_event(FSM, {Ref, {merkle_diff, {BKey, undefined}}}),
    DiffState#diff_state{diff_hash = DiffState#diff_state.diff_hash + 1,
        replies=DiffState#diff_state.replies - 1};
diff_hash(PBKey, DiffState) ->
    UpdDiffHash = DiffState#diff_state.diff_hash + 1,
    BKey = unpack_key(PBKey),
    case catch riak_kv_vnode:get_vclocks(DiffState#diff_state.preflist, 
                                         [BKey]) of
        [{BKey, _Vclock} = BkeyVclock] ->
            Fsm = DiffState#diff_state.fsm,
            Ref = DiffState#diff_state.ref,
            gen_fsm:send_event(Fsm, {Ref, {merkle_diff, BkeyVclock}}),
            DiffState#diff_state{diff_hash = UpdDiffHash,
                replies=DiffState#diff_state.replies - 1};
        Reason ->
            UpdErrors = orddict:update_counter(Reason, 1, DiffState#diff_state.errors),
            DiffState#diff_state{errors = UpdErrors}
    end.
    
%% Called when the key is missing on the local side
missing_key(PBKey, DiffState) ->
    BKey = unpack_key(PBKey),
    Fsm = DiffState#diff_state.fsm,
    Ref = DiffState#diff_state.ref,
    gen_fsm:send_event(Fsm, {Ref, {merkle_diff, {BKey, vclock:fresh()}}}),
    UpdMissing = DiffState#diff_state.missing + 1,
    DiffState#diff_state{missing = UpdMissing,
        replies=DiffState#diff_state.replies - 1}.

%% @private
%%
%% @doc Visting function for building merkle tree.  This function was
%% purposefully created because if you use a lambda then things will
%% go wrong when the MD5 of this module changes. I.e. if the lambda is
%% shipped to another node with a different version of
%% riak_repl_fullsync_helper, even if the code inside the lambda is
%% the same, then a badfun error will occur since the MD5s of the
%% modules are not the same.
%%
%% @see http://www.javalimit.com/2010/05/passing-funs-to-other-erlang-nodes.html
merkle_fold(K, V, Pid) ->
    gen_server2:cast(Pid, {merkle, K, hash_object(V)}),
    Pid.

%% @private
%%
%% @doc Visting function for building keylists. Similar to merkle_fold.
keylist_fold(K, V, {MPid, Count, Total}) ->
    H = hash_object(V),
    Bin = term_to_binary({pack_key(K), H}),
    %% write key/value hash to file
    gen_server2:cast(MPid, {keylist, Bin}),
    case Count of
        100 ->
            %% send keylist_ack to "self" every 100 key/value hashes
            ok = gen_server2:call(MPid, keylist_ack, infinity),
            {MPid, 0, Total+1};
        _ ->
            {MPid, Count+1, Total+1}
    end.
