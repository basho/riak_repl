%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_tcp_client).
-author('Andy Gross <andy@basho.com').
-include("riak_repl.hrl").
-behaviour(gen_fsm).
-export([start_link/1,
         set_listeners/2,
         status/1, status/2]).
-export([async_connect/3]).
-export([init/1, 
         handle_event/3,
         handle_sync_event/4, 
         handle_info/3, 
         terminate/3, 
         code_change/4]).
-export([disconnected/2,
         connecting/2,
         wait_peerinfo/2,
         merkle_recv/2,
         merkle_diff/2,
         merkle_exchange/2]).


-type ip_addr() :: any().
-type ip_port_num() :: non_neg_integer().
-type ip_addr_port_list() :: [{ip_addr(), ip_port_num()}].

-record(state, {
          listeners :: ip_addr_port_list(),
          pending :: ip_addr_port_list(),
          listener :: undefined | {pid()|connected, ip_addr(), ip_port_num()},
          socket :: port(), 
          sitename :: string(),
          client :: tuple(),
          my_pi :: #peer_info{},
          partitions=[] :: list(),
          work_dir :: undefined | string(),
          merkle_pt :: non_neg_integer(),
          merkle_fp :: term(),
          their_merkle_fn :: string(),
          their_merkle_sz :: non_neg_integer(),
          their_kl_fn :: string(),
          their_kl_pid  :: undefined | pid(),
          their_kl_ref :: undefined | pending | reference(),  % pending state until pid created
          our_kl_fn :: string(),
          our_kl_ref :: undefined | reference(),
          our_kl_pid :: undefined | pid(),
          diff_pid :: undefined | pid(),
          bkey_vclocks=[] :: [{any(),any()}],
          count :: non_neg_integer(),
          ack_freq :: pos_integer()
         }).

%% ===================================================================
%% Public API
%% ===================================================================

%% Start a client connection to the remote site - addresses will be
%% provided with set_addresses
start_link(SiteName) -> 
    gen_fsm:start_link(?MODULE, [SiteName], []).
    
%% Update the list of IP Address / Ports for the remote site
set_listeners(Pid, Addresses) ->
    gen_fsm:send_all_state_event(Pid, {set_listeners, Addresses}).

%% Return a {status, [...]} tuple
status(Pid) ->
    status(Pid, infinity).

%% Return a {status, [...] tuple with timeout
status(Pid, Timeout) ->
    gen_fsm:sync_send_all_state_event(Pid, status, Timeout).


%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([SiteName]) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_site(Ring, SiteName) of 
        undefined ->
            %% Do not start
            {stop, {site_not_in_ring, SiteName}};
        Site ->
            set_listeners(self(), Site#repl_site.addrs),
            {ok, await_listeners, #state{sitename = SiteName}}
    end.

%%  await_listeners(Event, State) - only sync_all_state_events

disconnected(try_connect, State) ->
    case State#state.pending of
        [] ->
            %% Start the retry timer
            RetryTimeout = app_helper:get_env(riak_repl, client_retry_timeout, 30000),
            gen_fsm:send_event_after(RetryTimeout, try_connect),
            {next_state, disconnected, State#state{pending = State#state.listeners}};
        [{IPAddr, Port} | Pending] ->
            %% Spawn a process to try and connect to IPAddr/Port
            Pid = proc_lib:spawn_link(?MODULE, async_connect,
                                      [self(), IPAddr, Port]),
            {next_state, connecting, State#state{listener = {Pid, IPAddr, Port},
                                                 pending = Pending}}
    end.

connecting({connected, Socket}, State) ->
    gen_tcp:send(Socket, State#state.sitename),
    Props = riak_repl_fsm:common_init(Socket),

    {_ConnPid, IPAddr, Port} = State#state.listener,
    NewState = State#state{
              listener = {connected, IPAddr, Port},
              socket=Socket, 
              client=proplists:get_value(client, Props),
              my_pi=proplists:get_value(my_pi, Props),
              partitions=proplists:get_value(partitions, Props)},
    send(Socket, {peerinfo, NewState#state.my_pi}),
    riak_repl_stats:client_connects(),
    {next_state, wait_peerinfo, NewState};
connecting({connect_failed, _Reason}, State) ->
    gen_fsm:send_event(self(), try_connect),
    riak_repl_stats:client_connect_errors(),
    {next_state, disconnected, State#state{listener = undefined}}.

wait_peerinfo({redirect, IPAddr, Port}, State) ->
    case lists:member({IPAddr, Port}, State#state.listeners) of
        false ->
            lager:notice("Redirected IP ~p not in listeners ~p",
                                  [{IPAddr, Port}, State#state.listeners]);
        _ ->
            ok
    end,
    riak_repl_stats:client_redirect(),
    disconnect(State#state{pending = [{IPAddr, Port} | State#state.pending]});
wait_peerinfo({peerinfo, TheirPeerInfo}, State) ->
    %% Forward compatibility with post-0.14.0 - will allow protocol negotiation
    %% rather than setting capabilities based on version
    Capability = riak_repl_util:capability_from_vsn(TheirPeerInfo),
    wait_peerinfo({peerinfo, TheirPeerInfo, Capability}, State);
wait_peerinfo({peerinfo, TheirPeerInfo, Capability},
              State=#state{socket = Socket,
                           my_pi=MyPeerInfo,
                           sitename=SiteName}) when is_list(Capability) ->
    case riak_repl_util:validate_peer_info(TheirPeerInfo, MyPeerInfo) of
        true ->
             %% Set up for bounded queue if remote server supports it
            case proplists:get_bool(bounded_queue, Capability) of
                true ->
                    AckFreq = app_helper:get_env(riak_repl,client_ack_frequency,
                                                 ?REPL_DEFAULT_ACK_FREQUENCY),
                    State1 = State#state{count=0, 
                                         ack_freq=AckFreq};
                false ->
                    State1 = State
            end,
           
            {ok, WorkDir} = riak_repl_fsm:work_dir(Socket, SiteName),
            TheirRing = riak_core_ring:upgrade(TheirPeerInfo#peer_info.ring),
            update_site_ips(riak_repl_ring:get_repl_config(TheirRing), SiteName),
            {next_state, merkle_exchange, State1#state{work_dir = WorkDir}};
        false ->
            lager:error("Replication - invalid peer_info ~p",
                                   [TheirPeerInfo]),
            cleanup_and_stop(State)
    end;
wait_peerinfo({diff_obj, Obj}, State) ->
    {next_state, wait_peerinfo, do_repl_put(Obj, State)}.
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
    {ok, FP} = file:open(TheirMerkleFn, [write, raw, binary, delayed_write]),
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
    {next_state, merkle_exchange, State};
merkle_exchange({diff_obj, Obj}, State) ->
    {next_state, merkle_exchange, do_repl_put(Obj, State)}.

merkle_recv({diff_obj, Obj}, State) ->
    {next_state, merkle_recv, do_repl_put(Obj, State)};
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
merkle_recv({Ref, keylist_built}, State=#state{our_kl_ref = Ref}) ->
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

merkle_diff({diff_obj, Obj}, State) -> % *not* from the merkle diff
    {next_state, merkle_diff, do_repl_put(Obj, State)};
merkle_diff({Ref, {merkle_diff, BkeyVclock}}, State=#state{our_kl_ref = Ref}) ->
    {next_state, merkle_diff,
     State#state{bkey_vclocks = [BkeyVclock | State#state.bkey_vclocks]}};
merkle_diff({Ref, {error, Reason}}, State=#state{our_kl_ref = Ref}) ->
    send(State#state.socket, {ack, State#state.merkle_pt, []}),
    lager:error("Full-sync with site ~p; vclock lookup for "
                           "partition ~p failed: ~p. Skipping partition.",
                           [State#state.sitename, State#state.merkle_pt,
                            Reason]),
    {next_state, merkle_exchange, 
     cleanup_partition(State#state{bkey_vclocks=[],
                                   diff_pid = undefined,
                                   our_kl_ref = Ref})};
merkle_diff({Ref, diff_done}, State=#state{our_kl_ref = Ref}) ->
    send(State#state.socket,
         {ack, State#state.merkle_pt, State#state.bkey_vclocks}),
    {next_state, merkle_exchange, 
     cleanup_partition(State#state{bkey_vclocks=[],
                                   diff_pid = undefined,
                                   our_kl_ref = Ref})}.

handle_info({tcp_closed, Socket}, _StateName, #state{socket = Socket} = State) ->
    lager:info("Connection to site ~p closed", [State#state.sitename]),
    cleanup_and_stop(State);
handle_info({tcp_closed, _Socket}, StateName, State) ->
    %% Ignore old sockets - e.g. after a redirect
    {next_state, StateName, State};
handle_info({tcp_error, Socket, Reason}, _StateName,  #state{socket = Socket} = State) ->
    lager:error("Connection to site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    cleanup_and_stop(State);
handle_info({tcp_error, _Socket, _Reason}, StateName, State) ->    
    %% Ignore old sockets errors
    {next_state, StateName, State};
handle_info({tcp, Socket, Data}, StateName, State=#state{socket=Socket}) ->
    R = ?MODULE:StateName(binary_to_term(Data), State),
    inet:setopts(Socket, [{active, once}]),            
    riak_repl_stats:client_bytes_recv(size(Data)),
    R;
%% no-ops
handle_info(_I, StateName, State) ->  {next_state, StateName, State}.
terminate(_Reason, _StateName, State) ->
    %% Clean up the working directory on crash/exit
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [State#state.work_dir])),
    os:cmd(Cmd).

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

handle_event({set_listeners, Listeners}, await_listeners, State) ->
    gen_fsm:send_event(self(), try_connect),
    {next_state, disconnected, State#state{pending = Listeners,
                                           listeners = Listeners}};
handle_event({set_listeners, Listeners}, StateName, State) ->
    %% If in any other state than the initial await_listeners,
    %% check that the the listeners we are connecting/connected to
    %% is in the list.  If not, stop so the supervisor will restart cleanly
    InListeners = case State#state.listener of
                      undefined ->
                          true; % not talking to anybody yet, so ok
                      {_, IpAddr, Port} ->
                          lists:member({IpAddr, Port}, Listeners)
                  end,
    case InListeners of 
        false ->
            %% let the supervisor restart
            cleanup_and_stop(State);
        true ->
            {next_state, StateName, State#state{pending = Listeners,
                                                listeners = Listeners}}
    end.
handle_sync_event(status, _From, StateName, State) ->
    Desc =
        [{site, State#state.sitename}] ++
        case State#state.listener of
            undefined ->
                [{waiting_to_retry, State#state.listeners}];
            {connected, IPAddr, Port} ->
                [{connected, IPAddr, Port}];
            {Pid, IPAddr, Port} ->
                [{connecting, Pid, IPAddr, Port}]
        end ++
        case State#state.merkle_pt of
            undefined ->
                [];
            Partition ->
                [{fullsync, Partition}]
        end ++
        [{state, StateName}],
    {reply, {status, Desc}, StateName, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% Function spawned to do async connect
async_connect(Parent, IPAddr, Port) ->
    Timeout = app_helper:get_env(riak_repl, client_connect_timeout, 15000),
    case gen_tcp:connect(IPAddr, Port, [binary, 
                                        {packet, 4}, 
                                        {keepalive, true},
                                        {nodelay, true}], Timeout) of
        {ok, Socket} ->
            ok = gen_tcp:controlling_process(Socket, Parent),
            gen_fsm:send_event(Parent, {connected, Socket});
        {error, Reason} ->
            %% Send Reason so it shows in traces even if nothing is done with it
            gen_fsm:send_event(Parent, {connect_failed, Reason})
    end.

%% Disconnect from the current server and reset all state
disconnect(State) ->
    catch gen_tcp:close(State#state.socket),
    NewState = cleanup_partition(State),
    remove_workdir(NewState),
    gen_fsm:send_event(self(), try_connect),
    {next_state, disconnected, NewState#state{work_dir = undefined,
                                              socket = undefined,
                                              listener = undefined}}.

%% Cleanup and stop the server - supervisor will restart
%% Easier than tracking the helper processes
cleanup_and_stop(State) ->
    NewState = cleanup_partition(State),
    remove_workdir(NewState),
    {stop, normal, NewState}.
    
%% Remove the work dir
remove_workdir(NewState) ->
    case NewState#state.work_dir of
        undefined ->
            ok;
        WorkDir ->
            Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
            os:cmd(Cmd)
    end.

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
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, MyNewRC),
    ok.    

do_repl_put(Obj, State=#state{ack_freq = undefined}) -> % q_ack not supported
    spawn(riak_repl_util, do_repl_put, [Obj]),
    State;
do_repl_put(Obj, State=#state{count=C, ack_freq=F}) when (C < (F-1)) ->
    spawn(riak_repl_util, do_repl_put, [Obj]),
    State#state{count=C+1};
do_repl_put(Obj, State=#state{socket=S, ack_freq=F}) ->
    send(S, {q_ack, F}),
    spawn(riak_repl_util, do_repl_put, [Obj]),
    State#state{count=0}.
    
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
            send(State#state.socket, {ack, State#state.merkle_pt, []}),
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
