%% Riak EnterpriseDS
%% Copyright 2013 Basho Technologies, Inc. All Rights Reserved.

-module(riak_repl_aae_source).
-behaviour(gen_fsm).

-include("riak_repl_aae_fullsync.hrl").

%% API
-export([start_link/5, start_exchange/1]).

%% FSM states
-export([prepare_exchange/2,
         update_trees/2,
         key_exchange/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         terminate/3, code_change/4]).

-type index() :: non_neg_integer().
-type index_n() :: {index(), pos_integer()}.

-record(state, {cluster,
                transport,
                socket,
                index       :: index(),
                index_n     :: index_n(),
                tree_pid    :: pid(),
                built       :: non_neg_integer(),
                timeout     :: pos_integer()
               }).

%% Per state transition timeout used by certain transitions
-define(DEFAULT_ACTION_TIMEOUT, 300000). %% 5 minutes

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(term(), term(), term(), index(), index_n())
                -> {ok,pid()} | ignore | {error, term()}.
start_link(Cluster, Transport, Socket, Index, IndexN) ->
    gen_fsm:start(?MODULE, [Cluster, Transport, Socket, Index, IndexN], []).

start_exchange(AAESource) ->
    gen_fsm:send_event(AAESource, start_exchange).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Cluster, Transport, Socket, Index, IndexN]) ->
    Timeout = app_helper:get_env(riak_kv,
                                 anti_entropy_timeout,
                                 ?DEFAULT_ACTION_TIMEOUT),

    {ok, TreePid} = riak_kv_vnode:hashtree_pid(Index),

    %% monitor(process, Manager),
    monitor(process, TreePid),

    State = #state{cluster=Cluster,
                   transport=Transport,
                   socket=Socket,
                   index=Index,
                   index_n=IndexN,
                   tree_pid=TreePid,
                   timeout=Timeout,
                   built=0},
    %% lager:debug("Starting exchange: ~p", [LocalVN]),
    {ok, prepare_exchange, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'DOWN', _, _, _, _}, _StateName, State) ->
    %% Either the entropy manager, local hashtree, or remote hashtree has
    %% exited. Stop exchange.
    {stop, something_went_down, State};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% gen_fsm states
%%%===================================================================

%% @doc Initial state. Attempt to acquire all necessary exchange locks.
%%      In order, acquire local concurrency lock, local tree lock,
%%      remote concurrency lock, and remote tree lock. Exchange will
%%      timeout if locks cannot be acquired in a timely manner.
prepare_exchange(start_exchange, State=#state{transport=Transport,
                                              socket=Socket,
                                              index=Partition,
                                              index_n=IndexN}) ->
    TcpOptions = [{keepalive, true},
                  {packet, 4},
                  {active, once},
                  {nodelay, true},
                  {header, 1}],
    ok = Transport:setopts(Socket, TcpOptions),
    case riak_kv_index_hashtree:get_lock(State#state.tree_pid,
                                         fullsync_source) of
        ok ->
            %% TODO: Normal AAE has a timeout in this phase of the
            %%       protocol. Do we want similar for fullsync?
            ok = send_msg(?MSG_INIT, {Partition, IndexN}, State),
            case send_msg(?MSG_LOCK_TREE, State) of
                ok ->
                    update_trees(start_exchange, State);
                Error ->
                    send_exchange_status({remote, Error}, State),
                    {stop, {remote, Error}, State}
            end;
        Error ->
            send_exchange_status(Error, State),
            {stop, normal, State}
    end.

%% @doc Now that locks have been acquired, ask both the local and remote
%%      hashtrees to perform a tree update. If updates do not occur within
%%      a timely manner, the exchange will timeout. Since the trees will
%%      continue to finish the update even after the exchange times out,
%%      a future exchange should eventually make progress.
update_trees(start_exchange, State=#state{tree_pid=TreePid,
                                          index=Partition,
                                          index_n=IndexN}) ->
    %% lager:debug("Sending to ~p", [LocalVN]),
    %% lager:debug("Sending to ~p", [RemoteVN]),

    update_request(TreePid, {Partition, undefined}, IndexN),
    case send_msg(?MSG_UPDATE_TREE, State) of
        ok ->
            update_trees({tree_built, Partition, IndexN}, State);
        not_responsible ->
            update_trees({not_responsible, Partition, IndexN}, State)
    end;

update_trees({not_responsible, VNodeIdx, IndexN}, State) ->
    lager:debug("VNode ~p does not cover preflist ~p", [VNodeIdx, IndexN]),
    send_exchange_status({not_responsible, VNodeIdx, IndexN}, State),
    {stop, not_responsible, State};
update_trees({tree_built, _, _}, State) ->
    Built = State#state.built + 1,
    case Built of
        2 ->
            lager:debug("Moving to key exchange"),
            {next_state, key_exchange, State, 0};
        _ ->
            {next_state, update_trees, State#state{built=Built}}
    end.

%% @doc Now that locks have been acquired and both hashtrees have been updated,
%%      perform a key exchange and trigger read repair for any divergent keys.
key_exchange(timeout, State=#state{cluster=Cluster,
                                   index=Partition,
                                   tree_pid=TreePid,
                                   index_n=IndexN}) ->
    lager:debug("Starting fullsync exchange with ~p for ~p/~p",
                [Cluster, Partition, IndexN]),

    Remote = fun(get_bucket, {L, B}) ->
                     send_msg(?MSG_GET_AAE_BUCKET, {L,B}, State);
                (key_hashes, Segment) ->
                     send_msg(?MSG_GET_AAE_SEGMENT, Segment, State)
             end,

    %% Unclear if we should allow exchange to run indefinitely or enforce
    %% a timeout. The problem is that depending on the number of keys and
    %% key differences, exchange can take arbitrarily long. For now, go with
    %% unbounded exchange, with the ability to cancel exchanges through the
    %% entropy manager if needed.
    AccFun = fun(KeyDiff, Acc) ->
                     lager:info("Keydiff: ~p", [KeyDiff]),
                     case Acc of
                         [] ->
                             [1];
                         [Count] ->
                             [Count+1];
                         Other ->
                             Other
                     end
             end,
    %% TODO: Add stats for AAE
    case riak_kv_index_hashtree:compare(IndexN, Remote, AccFun, TreePid) of
        [] ->
            %% exchange_complete(LocalVN, RemoteVN, IndexN, 0),
            lager:info("Repl'd 0 keys"),
            ok;
        [_Count] ->
            %% exchange_complete(LocalVN, RemoteVN, IndexN, Count),
            lager:info("Repl'd ~b keys during fullsync to ~p of ~p/~p ",
                       [Cluster, Partition, IndexN])
    end,
    {stop, normal, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
update_request(Tree, {Index, _}, IndexN) ->
    as_event(fun() ->
                     case riak_kv_index_hashtree:update(IndexN, Tree) of
                         ok ->
                             {tree_built, Index, IndexN};
                         not_responsible ->
                             {not_responsible, Index, IndexN}
                     end
             end).

%% @private
as_event(F) ->
    Self = self(),
    spawn_link(fun() ->
                       Result = F(),
                       gen_fsm:send_event(Self, Result)
               end),
    ok.

%% @private
%% do_timeout(State=#state{local=LocalVN,
%%                         remote=RemoteVN,
%%                         index_n=IndexN}) ->
%%     lager:info("Timeout during exchange between (local) ~p and (remote) ~p, "
%%                "(preflist) ~p", [LocalVN, RemoteVN, IndexN]),
%%     send_exchange_status({timeout, RemoteVN, IndexN}, State),
%%     {stop, normal, State}.

%% @private
send_exchange_status(Status, State) ->
    throw(Status),
    State.

%% @private
%% next_state_with_timeout(StateName, State) ->
%%     next_state_with_timeout(StateName, State, State#state.timeout).
%% next_state_with_timeout(StateName, State, Timeout) ->
%%     {next_state, StateName, State, Timeout}.

%% exchange_complete({LocalIdx, _}, {RemoteIdx, RemoteNode}, IndexN, Repaired) ->
%%     riak_kv_entropy_info:exchange_complete(LocalIdx, RemoteIdx, IndexN, Repaired),
%%     rpc:call(RemoteNode, riak_kv_entropy_info, exchange_complete,
%%              [RemoteIdx, LocalIdx, IndexN, Repaired]).

send_msg(MsgType, Msg, State=#state{transport=Transport,
                                    socket=Socket}) ->
    Data = term_to_binary(Msg),
    ok = Transport:send(Socket, <<MsgType:8, Data/binary>>),
    get_reply(State).

send_msg(MsgType, State=#state{transport=Transport,
                               socket=Socket}) ->
    ok = Transport:send(Socket, <<MsgType:8>>),
    get_reply(State).

get_reply(#state{transport=Transport, socket=Socket}) ->
    ok = Transport:setopts(Socket, [{active, once}]),
    receive
        {_, Socket, [?MSG_REPLY|Data]} ->
            binary_to_term(Data);
        {Error, Socket} ->
            throw(Error);
        {Error, Socket, Reason} ->
            throw({Error, Reason})
    end.
