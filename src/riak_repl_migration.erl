%% Riak Replication Realtime Migration manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_repl_migration).

-behaviour(gen_server).

%% API
-export([start_link/0,migrate_queue/0, migrate_queue/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {elapsed_sleep,
               caller}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

migrate_queue() ->
     DefaultTimeout = app_helper:get_env(riak_repl, queue_migration_timeout, 5),
    gen_server:call(?SERVER, {wait_for_queue, DefaultTimeout}, infinity).
migrate_queue(Timeout) ->
    %% numeric timeout only... probably need to support infinity
    gen_server:call(?SERVER, {wait_for_queue, Timeout}, infinity).

init([]) ->
    lager:info("Riak replication migration server started"),
    {ok, #state{elapsed_sleep=0}}.

handle_call({wait_for_queue, MaxTimeout}, From, State) ->
    lager:info("Realtime Repl queue migration sleeping"),
    %% TODO: is there a better way to do the next line? just call
    %% handle_info?
    erlang:send_after(100, self(), {sleep, MaxTimeout}),
    {noreply, State#state{caller = From, elapsed_sleep = 0}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({sleep, MaxTimeout}, State = #state{elapsed_sleep = ElapsedSleep}) ->
    case riak_repl2_rtq:all_queues_empty() of
        true ->
            gen_server:reply(State#state.caller, ok),
            lager:info("Queue empty, no replication queue migration required");
        false ->
            case (ElapsedSleep >= MaxTimeout) of
                true ->
                    lager:info("Realtime queue has not completely drained"),
                    queue_handoff(State),
                    gen_server:reply(State#state.caller, ok);
                false ->
                    lager:info("Waiting for realtime repl queue to drain"),
                    erlang:send_after(1000, self(), {sleep, MaxTimeout})
            end
    end,
    NewState = State#state{elapsed_sleep = ElapsedSleep + 1000},
    {noreply, NewState}.

drain_queue(false, Peer) ->
    % would have made this a standard function, but I need a closure for the
    % value Peer
    riak_repl2_rtq:pull_sync(qm,
             fun ({Seq, NumItem, Bin}) ->
                try
                    gen_server:cast({riak_repl2_rtq,Peer}, {push, NumItem, Bin}),
                    %% Note - the next line is casting, not calling.
                    riak_repl2_rtq:ack(qm, Seq)
                catch
                    _:_ ->
                        % probably too much spam in the logs for this warning
                        %lager:warning("Dropped object during replication queue migration"),
                        % is this the correct stat?
                        riak_repl_stats:objects_dropped_no_clients(),
                        riak_repl_stats:rt_source_errors()
                end,
             ok end),
    drain_queue(riak_repl2_rtq:is_empty(qm), Peer);

drain_queue(true, _Peer) ->
   done.

queue_handoff(State) ->
    PeerReplNodes = riak_repl_util:get_peer_repl_nodes(),
    case length(PeerReplNodes) of
        0 -> %% TODO: bump up riak_repl_stats dropped_objects stats
            %% should we have a new stat? riak_repl_stats:objects_dropped_no_leader()
            lager:error("No nodes available to migrate replication data"),
            riak_repl_stats:rt_source_errors(),
            {reply, error, State};
        _N ->
            lager:info("Starting queue migration"),
            riak_repl2_rtq:register(qm),

            Peer = hd(PeerReplNodes),
            lager:info("Migrating replication queue data to ~p", [Peer]),
            drain_queue(riak_repl2_rtq:is_empty(qm), Peer),
            lager:info("Done migrating replication queue"),
            {reply, ok, State}
        end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
