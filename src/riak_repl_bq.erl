-module(riak_repl_bq).
-behaviour(gen_server).

-export([start_link/0, get_handle/1, status/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_repl.hrl").

-record(state, {
        q,
        client,
        max_size,
        dropped_count=0
        }).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

get_handle(Pid) ->
    gen_server:call(Pid, get_handle).

status(Pid) ->
    try
        gen_server:call(Pid, status, infinity)
    catch
        _:_ ->
            [{queue, too_busy}]
    end.

stop(Pid) ->
    gen_server:call(Pid, stop).

%% gen_server

init([]) ->
    QSize = app_helper:get_env(riak_repl,queue_size,
                               ?REPL_DEFAULT_QUEUE_SIZE),
    {ok, C} = riak:local_client(),
    {ok, Q} = erl_lfq:new(),
    {ok, #state{q = Q,
                client=C,
                max_size=QSize
               }}.


handle_cast(_, State) ->
    {noreply, State}.

handle_call(status, _From, State = #state{q=Q}) ->
    Status = [{queue_pid, self()},
              {queue_msg_queue, process_info(self(), message_queue_len)},
              {dropped_count, State#state.dropped_count},
              {queue_length, erl_lfq:len(Q)},
              {queue_byte_size, erl_lfq:byte_size(Q)},
              {queue_max_size, State#state.max_size},
              {queue_percentage, (erl_lfq:byte_size(Q) * 100) div
               State#state.max_size}
              %{queue_pending, State#state.pending},
              %{queue_max_pending, State#state.max_pending}
             ],
    {reply, Status, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(get_handle, _From, State = #state{q=Q}) ->
    {reply, Q, State}.

handle_info({repl, RObj}, State) ->
    case riak_repl_util:repl_helper_send_realtime(RObj, State#state.client) of
        [] ->
            %% no additional objects to queue
            {noreply, enqueue(term_to_binary({diff_obj, RObj}), State)};
        Objects when is_list(Objects) ->
            %% enqueue all the objects the hook asked us to send as a list.
            %% They're enqueued together so that they can't be dumped from the
            %% queue piecemeal if it overflows
            NewState = enqueue([term_to_binary({diff_obj, O}) ||
                        O <- Objects ++ [RObj]], State),
            {noreply, NewState};
        cancel ->
            {noreply, State}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% internal

enqueue(Msg, State=#state{q=Q, max_size=M}) ->
    %lager:error("getting queue size"),
    case erl_lfq:byte_size(Q) of
        X when X > M  ->
            %lager:error("dropping, size was ~p", [X]),
            State#state{dropped_count=State#state.dropped_count+1};
        _ ->
            %lager:error("enqueueing, size was ~p : ~p", [X, byte_size(Msg)]),
            erl_lfq:in(Q,Msg),
            State
    end.


