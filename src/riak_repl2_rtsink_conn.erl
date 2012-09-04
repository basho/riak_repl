%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink_conn).

%% @doc Realtime replication sink connection module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%

%% API
-export([start_link/4,
         stop/1,
         status/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {transport,        %% Module for sending
                socket,           %% Socket
                proto,            %% Protocol version negotiated
                max_pending,      %% Maximum number of operations
                active = true,    %% If socket is set active
                deactivated = 0,  %% Count of times deactivated
                source_drops = 0, %% Count of upstream drops
                helper,           %% Helper PID
                seq_ref,          %% Sequence reference for completed/acked
                expect_seq = undefined,%% Next expected sequence number
                acked_seq = undefined, %% Last sequence number acknowledged
                completed = [],   %% Completed sequence numbers that need to be sent
                cont = <<>>       %% Continuation from previous TCP buffer
               }).

start_link(Socket, Transport, Proto, _Args) ->
    gen_server:start_link(?MODULE, [Transport, Socket, Proto], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

status(Pid) ->
    status(Pid, 5000).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

%% Callbacks
init([Transport, Socket, Proto]) ->
    {ok, Helper} = riak_repl2_rtsink_helper:start_link(self()),
    riak_repl2_rt:register_sink(self()),
    Transport:setopts(Socket, [{active, true}]), % pick up errors in tcp_error msg
    MaxPending = app_helper:get_env(riak_repl, rtsink_max_pending, 100),
    {ok, #state{transport = Transport, socket = Socket,
                proto = Proto, max_pending = MaxPending, helper = Helper}}.

handle_call(status, _From, State = #state{transport = T, socket = S, helper = Helper,
                                          active = Active, deactivated = Deactivated,
                                          source_drops = SourceDrops,
                                          expect_seq = ExpSeq, acked_seq = AckedSeq}) ->
    Pending = pending(State),    
    Status = {will_be_remote, self(), [{connected, true},
                                       {transport, T},
                                       {socket, S},
                                       {peer, T:peername(S)},
                                       {helper, Helper},
                                       {active, Active},
                                       {deactivated, Deactivated},
                                       {source_drops, SourceDrops},
                                       {expect_seq, ExpSeq},
                                       {acked_seq, AckedSeq},
                                       {pending, Pending}]},
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

%% Note pattern patch on Ref
handle_cast({ack, Ref, Seq}, State = #state{transport = T, socket = S, 
                                            seq_ref = Ref,
                                            acked_seq = AckedTo,
                                            completed = Completed}) ->
    %% Worker pool has completed the put, check the completed
    %% list and work out where we can ack back to
    case ack_to(AckedTo, ordsets:add_element(Seq, Completed)) of
        {AckedTo, Completed2} ->
            {noreply, State#state{completed = Completed2}};
        {AckTo, Completed2}  ->
            TcpIOL = riak_repl2_rtframe:encode(ack, AckTo),
            T:send(S, TcpIOL),
            %% TODO: break out socket stats collection/reporting to separate process
            riak_repl_stats:client_bytes_sent(iolist_size(TcpIOL)),
            {noreply, State#state{acked_seq = AckTo, completed = Completed2}}
    end;
handle_cast({ack, Ref, Seq}, State) ->
    %% Nothing to send, it's old news.
    lager:debug("Received ack ~p for previous sequence ~p\n", [Seq, Ref]),
    {noreply, State}.

handle_info({tcp, _S, TcpBin}, State= #state{cont = Cont}) ->
    %% TODO: break out socket stats collection/reporting to separate process
    riak_repl_stats:client_bytes_recv(size(TcpBin)),
    recv(<<Cont/binary, TcpBin/binary>>, State);
handle_info({tcp_closed, S}, State = #state{transport = T, socket = S, cont = Cont}) ->
    case size(Cont) of
        0 ->
            ok;
        NumBytes ->
            %%TODO: Add remote name somehow
            lager:warning("Realtime connection from ~p closed with partial receive of ~b bytes\n",
                          [T:peername(S), NumBytes])
    end,
    {stop, normal, State};
handle_info({tcp_error, _S, Reason}, State= #state{transport = T, socket = S, cont = Cont}) ->
    %%TODO: Add remote name somehow
    lager:warning("Realtime connection from ~p network error ~p - ~b bytes pending\n",
                  [T:peername(S), Reason, size(Cont)]),
    {stop, normal, State};
handle_info(reactivate_socket, State = #state{transport = T, socket = S,
                                              max_pending = MaxPending}) ->
    case pending(State) > MaxPending of
        true ->
            {noreply, schedule_reactivate_socket(State#state{active = false})};
        _ ->
            lager:debug("Realtime sink recovered - reactivating transport ~p socket ~p\n",
                        [T, S]),
            T:setopts(S, [{active, true}]), % socket could die, pick it up on tcp_error msgs
            {noreply, State#state{active = true}}
    end.

terminate(_Reason, _State) ->
    %% TODO: Consider trying to do something graceful with poolboy?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Receive TCP data - decode framing and dispatch
recv(TcpBin, State) ->
    case riak_repl2_rtframe:decode(TcpBin) of
        {ok, undefined, Cont} ->
            {noreply, State#state{cont = Cont}};
        {ok, {objects, {Seq, BinObjs}}, Cont} ->
            recv(Cont, do_write_objects(Seq, BinObjs, State));
        {error, Reason} ->
            %% TODO: Log Something bad happened
            {stop, {framing, Reason}, State}
    end.

%% Note match on Seq
do_write_objects(Seq, BinObjs, State = #state{max_pending = MaxPending,
                                              helper = Helper,
                                              seq_ref = Ref,
                                              expect_seq = Seq,
                                              acked_seq = AckedSeq}) ->
    Me = self(),
    DoneFun = fun() -> gen_server:cast(Me, {ack, Ref, Seq}) end,
    riak_repl2_rtsink_helper:write_objects(Helper, BinObjs, DoneFun),
    State2 = case AckedSeq of
                 undefined ->
                     %% Handle first received sequence number
                     State#state{acked_seq = Seq - 1, expect_seq = Seq + 1};
                 _ ->
                     State#state{expect_seq = Seq + 1}
             end,
    %% If the socket is too backed up, take a breather
    %% by setting {active, false} then casting a message to ourselves
    %% to enable it once under the limit
    case pending(State2) > MaxPending of
        true ->
            schedule_reactivate_socket(State2);
        _ ->
            State2
    end;
do_write_objects(Seq, BinObjs, State = #state{expect_seq = ExpSeq,
                                              source_drops = SourceDrops}) ->
    %% Did not get expected sequence.
    %%
    %% If the source dropped (rtq consumer behind tail of queue), there
    %% is no point acknowledging any more if it is unable to resend anyway.
    %% Reset seq ref/completion array and start over at new sequence. 
    %%
    %% If the sequence number wrapped?  don't worry about acks, happens infrequently.
    %%
    NewSeqRef = make_ref(),
    SourceDrops2 = case ExpSeq of
                       undefined -> % no need to tell user about first time through
                           SourceDrops;
                       _ ->
                           SourceDrops + Seq - ExpSeq
                  end,
    do_write_objects(Seq, BinObjs, State#state{seq_ref = NewSeqRef,
                                               expect_seq = Seq,
                                               acked_seq = Seq - 1,
                                               completed = [],
                                               source_drops = SourceDrops2}).
                                               
    
    
    

%% Work out the highest sequence number that can be acked
%% and return it, completed always has one or more elements on first
%% call.
ack_to(Acked, []) ->
    {Acked, []};
ack_to(Acked, [Seq | Completed2] = Completed) -> 
    case Acked + 1 of
        Seq ->
            ack_to(Seq, Completed2);
        _ ->
            {Acked, Completed}
    end.

%% Work out how many requests are pending (not writted yet, may not
%% have been acked)
pending(#state{acked_seq = undefined}) ->
    0; % nothing received yet
pending(#state{expect_seq = ExpSeq, acked_seq = AckedSeq,
               completed = Completed}) ->
    ExpSeq - AckedSeq - length(Completed) - 1.
    

schedule_reactivate_socket(State = #state{transport = T,
                                          socket = S,
                                          active = Active,
                                          deactivated = Deactivated}) ->
    case Active of
        true ->
            lager:debug("Realtime sink overloaded - deactivating transport ~p socket ~p\n",
                        [T, S]),
            T:setopts(S, [{active, false}]),
            self() ! reactivate_socket,
            State#state{active = false, deactivated = Deactivated + 1};
        false ->
            %% already deactivated, try again in 10ms
            erlang:send_after(10, self(), reactivate_socket),
            State#state{active = {false, scheduled}};
        {false, scheduled} ->
            %% have a check scheduled already
            State
    end.
    
