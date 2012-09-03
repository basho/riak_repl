%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink).

%% @doc Realtime replication sink module
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
    ok = Transport:setopts(Socket, [{active, true}]),
    {ok, #state{transport = Transport, socket = Socket,
                proto = Proto, helper = Helper}}.

handle_call(status, _From, State = #state{transport = T, socket = S, helper = Helper,
                                          expect_seq = ExpSeq, acked_seq = AckedSeq,
                                          completed = Completed}) ->
    Pending = case AckedSeq of 
                  undefined -> % nothing sent yet
                      0;
                  _ ->
                      ExpSeq - AckedSeq - length(Completed) - 1
              end,
    
    Status = {will_be_remote, self(), [{connected, true},
                                       {transport, T},
                                       {socket, S},
                                       {peer, T:peername(S)},
                                       {helper, Helper},
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
            T:send(S, riak_repl2_rtframe:encode(ack, AckTo)),
            {noreply, State#state{acked_seq = AckTo, completed = Completed2}}
    end;
handle_cast({ack, Ref, Seq}, State) ->
    %% Nothing to send, it's old news.
    lager:debug("Received ack ~p for previous sequence ~p\n", [Seq, Ref]),
    {noreply, State}.


handle_info({tcp, _S, TcpBin}, State= #state{cont = Cont}) ->
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
    {stop, normal, State}.

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
do_write_objects(Seq, BinObjs, State = #state{helper = Helper, 
                                              seq_ref = Ref,
                                              expect_seq = Seq}) ->
    Me = self(),
    DoneFun = fun() -> gen_server:cast(Me, {ack, Ref, Seq}) end,
    riak_repl2_rtsink_helper:write_objects(Helper, BinObjs, DoneFun),
    update_seq(Seq, State);
do_write_objects(Seq, BinObjs, State = #state{seq_ref = OldSeqRef,
                                              expect_seq = ExpSeq}) ->
    %% Did not get expected sequence.
    %%
    %% If the source dropped (rtq consumer behind tail of queue), there
    %% is no point acknowledging any more if it is unable to resend anyway.
    %% Reset seq ref/completion array and start over at new sequence. 
    %%
    %% If the sequence number wrapped?  don't worry about acks, happens infrequently.
    %%
    NewSeqRef = make_ref(),
    case OldSeqRef of
        undefined -> % no need to tell user about first time through
            ok;
        _ ->
            lager:info("Realtime replication sink detected sequence gap from ~p to ~p,"
                       " ending sequence ref ~p starting ~p\n",
                       [ExpSeq, Seq, OldSeqRef, NewSeqRef])
    end,
    do_write_objects(Seq, BinObjs, State#state{seq_ref = NewSeqRef,
                                               expect_seq = Seq,
                                               acked_seq = Seq - 1,
                                               completed = []}).
                                               
    
    
    

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

update_seq(Seq, State = #state{acked_seq = undefined}) ->
    %% Handle first sequence number
    State#state{acked_seq = Seq - 1, expect_seq = Seq + 1};
update_seq(Seq, State) ->
    State#state{expect_seq = Seq + 1}.

