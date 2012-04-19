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
-export([start_link/4, set_socket/2, send/3, status/1, status/2]).
-export([start_fullsync/1, cancel_fullsync/1, pause_fullsync/1,
        resume_fullsync/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
        sitename :: repl_sitename(),
        socket :: repl_socket(),
        transport :: 'ranch_tcp' | 'ranch_ssl',
        listener :: pid(),
        client :: tuple(),
        q :: undefined | bounded_queue:bounded_queue(),
        max_pending :: undefined | pos_integer(),
        work_dir :: string(),   %% working directory for this repl session
        pending :: undefined | non_neg_integer(),
        my_pi :: #peer_info{},
        their_pi :: #peer_info{},
        fullsync_worker :: pid() | undefined,
        fullsync_strategy :: atom(),
        election_timeout :: undefined | reference(), % reference for the election timeout
        keepalive_time :: undefined | integer()
    }).

start_link(Listener, Socket, Transport, _Opts) ->
    gen_server:start_link(?MODULE, [Listener, Socket, Transport], []).

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

init([Listener, Socket, Transport]) ->
    self() ! init_ack,
    {ok, #state{socket=Socket, transport=Transport, listener=Listener}}.

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
    {reply, {status, Desc ++ Res}, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info({repl, RObj}, State=#state{transport=Transport, socket=Socket}) when State#state.q == undefined ->
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        Objects when is_list(Objects) ->
            [send(Transport, Socket, term_to_binary({diff_obj, O})) || O <-
                Objects],
            send(Transport, Socket, term_to_binary({diff_obj, RObj})),
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
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("Connection for site ~p closed", [State#state.sitename]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("Connection for site ~p closed unexpectedly: ~p",
        [State#state.sitename, Reason]),
    {stop, normal, State};
handle_info({Proto, Socket, Data},
        State=#state{socket=Socket,transport=Transport}) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    Reply = handle_msg(Msg, State),
    riak_repl_stats:server_bytes_recv(size(Data)),
    case Reply of
        {noreply, NewState} ->
            case NewState#state.keepalive_time of
                Time when is_integer(Time) ->
                    %% set the keepalive timeout
                    {noreply, NewState, Time};
                _ ->
                    Reply
            end;
        _ ->
            Reply
    end;
handle_info(init_ack, State=#state{transport=Transport, socket=Socket}) ->
    %% acknowledge the change of socket ownership
    ok = ranch:accept_ack(State#state.listener),
    ok = Transport:setopts(Socket, [
            binary, 
            {keepalive, true},
            {nodelay, true},
            {packet, 4},
            {reuseaddr, true},
            {active, false}]),
    case Transport:recv(Socket, 0, 60000) of
        {ok, SiteNameBin} ->
            SiteName = binary_to_list(SiteNameBin),
            ok = riak_repl_util:configure_socket(Transport, Socket),
            self() ! send_peerinfo,
            Timeout = erlang:send_after(60000, self(), election_timeout),
            {noreply, State#state{sitename=SiteName, election_timeout=Timeout}};
        {error, Reason} ->
            riak_repl_stats:server_connect_errors(),
            %% debug to avoid DOS logging
            lager:debug("Failed to receive site name banner from replication"
                "client: ~p", [Reason]),
            {stop, normal, State}
    end;
handle_info(send_peerinfo, State) ->
    send_peerinfo(State);
handle_info(election_timeout, #state{election_timeout=Timer} = State) when is_reference(Timer) ->
    lager:error("Timed out waiting for a leader to be elected"),
    {stop, normal, State};
handle_info(election_wait, State) ->
    send_peerinfo(State);
handle_info(timeout, State) ->
    case State#state.keepalive_time of
        Time when is_integer(Time) ->
            %% keepalive timeout fired
            send(State#state.transport, State#state.socket, keepalive),
            {noreply, State, Time};
        _ ->
            {noreply, State}
    end;
handle_info(_Event, State) ->
    {noreply, State}.

terminate(_Reason, #state{fullsync_worker=FSW, work_dir=WorkDir}) ->
    case is_pid(FSW) of
        true ->
            gen_fsm:sync_send_all_state_event(FSW, stop);
        _ ->
            ok
    end,
    %% clean up work dir
    Cmd = lists:flatten(io_lib:format("rm -rf ~s", [WorkDir])),
    os:cmd(Cmd).

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
            lager:info("Using fullsync strategy ~p.", [StratMod]),
            {ok, FullsyncWorker} = StratMod:start_link(State#state.sitename,
                State#state.transport, State#state.socket, State#state.work_dir, State#state.client),
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

            case proplists:get_bool(keepalive, Capability) of
                true ->
                    KeepaliveTime = ?KEEPALIVE_TIME;
                _ ->
                    KeepaliveTime = undefined
            end,

            case app_helper:get_env(riak_repl, fullsync_on_connect, true) of
                true ->
                    FullsyncWorker ! start_fullsync,
                    lager:info("Full-sync on connect"),
                    {noreply, State1#state{keepalive_time=KeepaliveTime}};
                false ->
                    {noreply, State1#state{keepalive_time=KeepaliveTime}}
            end;
        false ->
            lager:error("Invalid peer info, ring sizes do not match."),
            {stop, normal, State}
    end;
handle_msg({q_ack, N}, #state{pending=Pending} = State) ->
    drain(State#state{pending=Pending-N});
handle_msg(keepalive, State) ->
    send(State#state.transport, State#state.socket, keepalive_ack),
    {noreply, State};
handle_msg(keepalive_ack, State) ->
    %% noop
    {noreply, State};
handle_msg(Msg, #state{fullsync_worker = FSW} = State) ->
    gen_fsm:send_event(FSW, Msg),
    {noreply, State}.

send_peerinfo(#state{transport=Transport, socket=Socket, sitename=SiteName} = State) ->
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
                    %% are we configured to upgrade to ssl?
                    case {riak_repl_util:maybe_use_ssl(), Transport:name()} of
                        {B, T} when B == false; T == ssl ->
                            %% if there's no valid ssl config or we've already
                            %% upgraded
                            Props = riak_repl_fsm_common:common_init(Transport, Socket),

                            PI = proplists:get_value(my_pi, Props),
                            send(Transport, Socket, {peerinfo, PI,
                                    [bounded_queue, keepalive, {fullsync_strategies,
                                            app_helper:get_env(riak_repl, fullsync_strategies,
                                                [?LEGACY_STRATEGY])}]}),
                            {ok, WorkDir} = riak_repl_fsm_common:work_dir(Transport, Socket, SiteName),
                            Transport:setopts(Socket, [{active, once}]),
                            {noreply, State#state{work_dir = WorkDir,
                                    client=proplists:get_value(client, Props),
                                    election_timeout=undefined,
                                    my_pi=PI}};
                        {Config, tcp} ->
                            send(Transport, Socket, {peerinfo,
                                    riak_repl_util:make_fake_peer_info(),
                                    [ssl_required]}),
                            %% verify the other side has sent us a fake ring
                            %% with the ssl_required capability
                            {ok, Data} = Transport:recv(Socket, 0, infinity),
                            case binary_to_term(Data) of
                                {peerinfo, _, [ssl_required]} ->
                                    case ssl:ssl_accept(Socket, Config) of
                                        {ok, SSLSocket} ->
                                            send_peerinfo(State#state{socket=SSLSocket,
                                                    transport=ranch_ssl});
                                        {error, Reason} ->
                                            lager:error("Unable to negotiate SSL for "
                                                "replication connection to ~p: ~p",
                                                [SiteName, Reason]),
                                            {stop, normal, State}
                                    end;
                                _ ->
                                    lager:error("Client does not support SSL; "
                                        "closing connection to ~p",
                                        [SiteName]),
                                    %% sleep for a really long time so the
                                    %% client doesn't DOS us with reconnect
                                    %% attempts
                                    timer:sleep(300000),
                                    {stop, normal, State}
                            end
                    end;
                {error, _Reason} ->
                    %% leader has changed, try again
                    send_peerinfo(State)
            end;
        OtherNode ->
            OtherListener = listener_for_node(OtherNode),
            {Ip, Port} = OtherListener#repl_listener.listen_addr,
            send(Transport, Socket, {redirect, Ip, Port}),
            {stop, normal, State}
    end.

send(Transport, Sock, Data) when is_binary(Data) ->
    R = Transport:send(Sock,Data),
    riak_repl_stats:server_bytes_sent(size(Data)),
    R;
send(Transport, Sock, Data) ->
    send(Transport, Sock, term_to_binary(Data)).

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
send_diffobj(Msg,State=#state{transport=Transport,socket=Socket,pending=Pending}) ->
    send(Transport, Socket, Msg),
    State#state{pending=Pending+1}.

