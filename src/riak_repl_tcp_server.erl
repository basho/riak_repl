%% Riak EnterpriseDS
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_repl_tcp_server).

%% @doc This module is responsible for the server-side TCP communication
%% during replication. A seperate instance of this module is started for every
%% replication connection that is established. A handshake with the client is
%% then exchanged and, using that information, certain protocol extensions are
%% enabled and the fullsync strategy is negotiated.
%%
%% This module handles the realtime part of the replication itself, but all
%% the details of the fullsync replication are delegated to the negotiated
%% fullsync worker, which implements its own protocol. Any unrecognized
%% messages received over the TCP connection are sent to the fullsync process.
%%
%% Realtime replication is quite simple. Using a postcommit hook, writes to
%% the cluster are sent to the replication leader, which will then forward
%% the update out to any connected replication sites. An optional protocol
%% extension is to use a bounded queue to throttle the stream of updates.

-include("riak_repl.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, set_socket/2, send/2, status/1, status/2]).
-export([start_fullsync/1, cancel_fullsync/1, pause_fullsync/1,
        resume_fullsync/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        sitename :: repl_sitename(),
        socket :: repl_socket(),
        client :: tuple(),
        q :: undefined | bounded_queue:bounded_queue(),
        max_pending :: undefined | pos_integer(),
        work_dir :: string(),   %% working directory for this repl session
        pending :: undefined | non_neg_integer(),
        my_pi :: #peer_info{},
        their_pi :: #peer_info{},
        fullsync_worker :: pid() | undefined,
        fullsync_strategy :: atom(),
        election_timeout :: undefined | reference() % reference for the election timeout
    }).

start_link(SiteName) ->
    gen_server:start_link(?MODULE, [SiteName], []).

set_socket(Pid, Socket) ->
    gen_server:call(Pid, {set_socket, Socket}).

start_fullsync(Pid) ->
    gen_server:call(Pid, start_fullsync).

cancel_fullsync(Pid) ->
    gen_server:call(Pid, cancel_fullsync).

pause_fullsync(Pid) ->
    gen_server:call(Pid, pause_fullsync).

resume_fullsync(Pid) ->
    gen_server:call(Pid, resume_fullsync).

status(Pid) ->
    status(Pid, infinity).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

init([SiteName]) ->
    %% we need to wait for set_socket to happen
    {ok, #state{sitename=SiteName}}.

handle_call(start_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:start_fullsync(FSW),
    {reply, ok, State};
handle_call(cancel_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:cancel_fullsync(FSW),
    {reply, ok, State};
handle_call(pause_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:pause_fullsync(FSW),
    {reply, ok, State};
handle_call(resume_fullsync, _From, #state{fullsync_worker=FSW,
        fullsync_strategy=Mod} = State) ->
    Mod:resume_fullsync(FSW),
    {reply, ok, State};
handle_call(status, _From, #state{fullsync_worker=FSW, q=Q} = State) ->
    Res = case is_pid(FSW) of
        true -> gen_fsm:sync_send_all_state_event(FSW, status, infinity);
        false -> []
    end,
    Desc = 
        [
            {site, State#state.sitename},
            {strategy, State#state.fullsync_strategy},
            {fullsync_worker, State#state.fullsync_worker}
        ] ++
        case State#state.q of
            undefined ->
                [{bounded_queue, disabled}];
            _ ->
                [{dropped_count, bounded_queue:dropped_count(Q)},
                    {queue_length, bounded_queue:len(Q)},
                    {queue_byte_size, bounded_queue:byte_size(Q)}]
        end,
    {reply, {status, Desc ++ Res}, State};
handle_call({set_socket, Socket}, _From, State) ->
    ok = riak_repl_util:configure_socket(Socket),
    self() ! send_peerinfo,
    Timeout = erlang:send_after(60000, self(), election_timeout),
    {reply, ok, State#state{socket=Socket, election_timeout=Timeout}}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info({repl, RObj}, State) when State#state.q == undefined ->
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        Objects when is_list(Objects) ->
            [send(State#state.socket, term_to_binary({diff_obj, O})) || O <-
                Objects],
            send(State#state.socket, term_to_binary({diff_obj, RObj})),
            {noreply, State};
        cancel ->
            {noreply, State}
    end;
handle_info({repl, RObj}, State) ->
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        [] ->
            %% no additional objects to queue
            drain(enqueue(term_to_binary({diff_obj, RObj}), State));
        Objects when is_list(Objects) ->
            %% enqueue all the objects the hook asked us to send as a list.
            %% They're enqueued together so that they can't be dumped from the
            %% queue piecemeal if it overflows
            NewState = enqueue([term_to_binary({diff_obj, O}) ||
                    O <- Objects ++ [RObj]], State),
            drain(NewState);
        cancel ->
            {noreply, State}
    end;
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({tcp, Socket, Data}, State=#state{socket=Socket}) ->
    Msg = binary_to_term(Data),
    Reply = handle_msg(Msg, State),
    inet:setopts(Socket, [{active, once}]),
    riak_repl_stats:server_bytes_recv(size(Data)),
    Reply;
handle_info(send_peerinfo, State) ->
    send_peerinfo(State);
handle_info(election_timeout, #state{election_timeout=Timer} = State) when is_reference(Timer) ->
    lager:error("Timed out waiting for a leader to be elected"),
    {stop, normal, State};
handle_info(election_wait, State) ->
    send_peerinfo(State);
handle_info(_Event, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW}) ->
    case is_pid(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions

handle_msg({peerinfo, PI}, State) ->
    Capability = riak_repl_util:capability_from_vsn(PI),
    handle_msg({peerinfo, PI, Capability}, State);
handle_msg({peerinfo, TheirPI, Capability}, #state{my_pi=MyPI} = State) ->
    case riak_repl_util:validate_peer_info(TheirPI, MyPI) of
        true ->
            ClientStrats = proplists:get_value(fullsync_strategies, Capability,
                [?LEGACY_STRATEGY]),
            ServerStrats = app_helper:get_env(riak_repl, fullsync_strategies,
                [?LEGACY_STRATEGY]),
            Strategy = riak_repl_util:choose_strategy(ServerStrats, ClientStrats),
            StratMod = riak_repl_util:strategy_module(Strategy, server),
            lager:notice("Using fullsync strategy ~p.", [StratMod]),
            {ok, FullsyncWorker} = StratMod:start_link(State#state.sitename,
                State#state.socket, State#state.work_dir, State#state.client),
            %% Set up bounded queue if remote supports it
            case proplists:get_bool(bounded_queue, Capability) of
                true ->
                    QSize = app_helper:get_env(riak_repl,queue_size,
                                               ?REPL_DEFAULT_QUEUE_SIZE),
                    MaxPending = app_helper:get_env(riak_repl,server_max_pending,
                                                    ?REPL_DEFAULT_MAX_PENDING),
                    State1 = State#state{q = bounded_queue:new(QSize),
                                         fullsync_worker = FullsyncWorker,
                                         fullsync_strategy = StratMod,
                                         max_pending = MaxPending,
                                         pending = 0};
                false ->
                    State1 = State#state{fullsync_worker = FullsyncWorker,
                                         fullsync_strategy = StratMod}
            end,

            case app_helper:get_env(riak_repl, fullsync_on_connect, true) of
                true ->
                    FullsyncWorker ! start_fullsync,
                    lager:notice("Full-sync on connect"),
                    {noreply, State1};
                false ->
                    {noreply, State1}
            end;
        false ->
            lager:error("Invalid peer info, ring sizes do not match."),
            {stop, normal, State}
    end;
handle_msg({q_ack, N}, #state{pending=Pending} = State) ->
    drain(State#state{pending=Pending-N});
handle_msg(Msg, #state{fullsync_worker = FSW} = State) ->
    gen_fsm:send_event(FSW, Msg),
    {noreply, State}.

send_peerinfo(#state{socket=Socket, sitename=SiteName} = State) ->
    OurNode = node(),
    case riak_repl_leader:leader_node()  of
        undefined -> % leader not elected yet
            %% check again in 5 seconds
            erlang:send_after(5000, self(), election_wait),
            {noreply, State};
        OurNode ->
            case riak_repl_leader:add_receiver_pid(self()) of
                ok ->
                    erlang:cancel_timer(State#state.election_timeout),
                    %% this switches the socket into active mode
                    Props = riak_repl_fsm_common:common_init(Socket),
                    PI = proplists:get_value(my_pi, Props),
                    send(Socket, {peerinfo, PI,
                            [bounded_queue, {fullsync_strategies,
                                    app_helper:get_env(riak_repl, fullsync_strategies,
                                        [?LEGACY_STRATEGY])}]}),
                    {ok, WorkDir} = riak_repl_fsm_common:work_dir(Socket, SiteName),
                    {noreply, State#state{work_dir = WorkDir,
                            client=proplists:get_value(client, Props),
                            election_timeout=undefined,
                            my_pi=PI}};
                {error, _Reason} ->
                    %% leader has changed, try again
                    send_peerinfo(State)
            end;
        OtherNode -> 
            OtherListener = listener_for_node(OtherNode),
            {Ip, Port} = OtherListener#repl_listener.listen_addr,
            send(Socket, {redirect, Ip, Port}),
            {stop, normal, State}
    end.

send(Sock, Data) when is_binary(Data) ->
    R = gen_tcp:send(Sock,Data),
    riak_repl_stats:server_bytes_sent(size(Data)),
    R;
send(Sock, Data) ->
    send(Sock, term_to_binary(Data)).

listener_for_node(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ReplConfig = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, ReplConfig),
    NodeListeners = [L || L <- Listeners,
                          L#repl_listener.nodename =:= Node],
    hd(NodeListeners).

drain(State=#state{q=Q,pending=P,max_pending=M}) when P < M ->
    case bounded_queue:out(Q) of
        {{value, Msg}, NewQ} ->
            drain(send_diffobj(Msg, State#state{q=NewQ}));
        {empty, NewQ} ->
            {noreply, State#state{q=NewQ}}
    end;
drain(State) ->
    {noreply, State}.

enqueue(Msg, State=#state{q=Q}) ->
    State#state{q=bounded_queue:in(Q,Msg)}.

send_diffobj(Msgs, State0) when is_list(Msgs) ->
    %% send all the messages in the list
    %% we correctly increment pending, so we should get enough q_acks
    %% to restore pending to be less than max_pending when we're done.
    lists:foldl(fun(Msg, State) ->
                send_diffobj(Msg, State)
        end, State0, Msgs);
send_diffobj(Msg,State=#state{socket=Socket,pending=Pending}) ->
    send(Socket,Msg),
    State#state{pending=Pending+1}.

