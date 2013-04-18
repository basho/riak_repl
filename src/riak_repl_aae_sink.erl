%% @doc
%% This module implements a fullsync "sink" strategy that uses Active Anti-Entropy (AAE).
%% It takes full control of the socket to the source side and implements the entire protocol
%% here. 
%%
-module(riak_repl_aae_sink).
-include("riak_repl.hrl").
-include("riak_repl_aae_fullsync.hrl").

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         handle_sync_event/4, terminate/2, code_change/3]).

%% API
-export([start_link/4, init_sync/1]).

-record(state, {
          clustername,
          socket,
          transport,
          tree_pid      :: pid(),  %% pid of the AAE tree
          partition,
          owner         :: pid()   %% our fssource owner
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ClusterName, Transport, Socket, OwnerPid) ->
    gen_server:start_link(?MODULE, [ClusterName, Transport, Socket, OwnerPid], []).

%% Called after ownership of socket has been given to AAE sink worker
init_sync(AAEWorker) ->
    gen_server:call(AAEWorker, init_sync, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ClusterName, Transport, Socket, OwnerPid]) ->
    lager:info("Starting AAE fullsync sink worker"),
    {ok, #state{clustername=ClusterName, socket=Socket, transport=Transport, owner=OwnerPid}}.

handle_call(init_sync, _From, State=#state{transport=Transport, socket=Socket}) ->
    TcpOptions = [{keepalive, true}, % find out if connection is dead, this end doesn't send
                  {packet, 4},
                  {active, once},
                  {nodelay, true},
                  {header, 1}],
    ok = Transport:setopts(Socket, TcpOptions),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_sync_event(status, _From, StateName, State) ->
    Res = [{state, StateName},
           {partition_syncing, State#state.partition}
          ],
    {reply, Res, StateName, State}.

handle_info({Proto, _Socket, Data}, State=#state{transport=Transport,
                                                 socket=Socket}) when Proto==tcp; Proto==ssl ->
    TcpOptions = [{active, once}], %% reset to receive next tcp message
    ok = Transport:setopts(Socket, TcpOptions),
    case Data of
        [MsgType] ->
            {noreply, process_msg(MsgType, State)};
        [MsgType|<<>>] ->
            {noreply, process_msg(MsgType, State)};
        [MsgType|MsgData] ->
            {noreply, process_msg(MsgType, binary_to_term(MsgData), State)}
    end;

handle_info({'DOWN', _, _, _, _}, State) ->
    {stop, tree_down, State};

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("AAE sink connection to ~p closed", [State#state.clustername]),
    {stop, normal, State};
handle_info({tcp_error, _Socket, Reason}, State) ->
    lager:error("AAE sink connection to ~p closed unexpectedly: ~p",
                [State#state.clustername, Reason]),
    {stop, normal, State};
handle_info({ssl_closed, Socket}, State=#state{socket=Socket}) ->
    lager:info("AAE sink ssl connection to ~p closed", [State#state.clustername]),
    {stop, normal, State};
handle_info({ssl_error, _Socket, Reason}, State) ->
    lager:error("AAE sink ssl connection to ~p closed unexpectedly with: ~p",
                [State#state.clustername, Reason]),
    {stop, normal, State};
handle_info({Error, Socket, Reason},
            State=#state{socket=MySocket}) when Socket == MySocket ->
    lager:info("AAE sink connection to ~p closed unexpectedly: ~p",
               [State#state.clustername, {Socket, Error, Reason}]),
    {stop, {Socket, Error, Reason}, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% replies: ok
process_msg(?MSG_INIT, Partition, State) ->
    %% List of IndexNs to iterate over.
    IndexNs = riak_kv_util:responsible_preflists(Partition),
    {ok, TreePid} = riak_kv_vnode:hashtree_pid(Partition),
    %% monitor the tree and crash if the tree goes away
    monitor(process, TreePid),
    lager:info("Got init for partition ~p with IndexeNs ~p", [Partition, IndexNs]),
    %% tell the reservation coordinator that we are taking this partition.
    riak_repl2_fs_node_reserver:claim_reservation(Partition),
    send_reply(ok, State#state{partition=Partition, tree_pid=TreePid});

process_msg(?MSG_GET_AAE_BUCKET, {Level,BucketNum,IndexN}, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_bucket(IndexN, Level, BucketNum, TreePid),
    lager:info("Got bucket: ~p", [ResponseMsg]),
    send_reply(ResponseMsg, State);

process_msg(?MSG_GET_AAE_SEGMENT, {SegmentNum,IndexN}, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_segment(IndexN, SegmentNum, TreePid),
    lager:info("Got segment: ~p", [ResponseMsg]),
    send_reply(ResponseMsg, State);

%% no reply
process_msg(?MSG_PUT_OBJ, {fs_diff_obj, BObj}, State) ->
    RObj = riak_repl_util:from_wire(BObj),
    B = riak_object:bucket(RObj),
    K = riak_object:key(RObj),
    lager:info("Received put request for ~p:~p", [B,K]),
    %% do the put
    riak_repl_util:do_repl_put(RObj),
    State;

%% replies: ok | not_responsible
process_msg(?MSG_UPDATE_TREE, IndexN, State=#state{tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:update(IndexN, TreePid),
    lager:info("AAE sink update tree indexN ~p got ~p",
               [State#state.partition, ResponseMsg]),
    send_reply(ResponseMsg, State).

%% replies: ok | not_built | already_locked
process_msg(?MSG_LOCK_TREE, State=#state{tree_pid=TreePid}) ->
    %% NOTE: be sure to die if tcp connection dies, to give back lock
    ResponseMsg = riak_kv_index_hashtree:get_lock(TreePid, fullsync_sink),
    lager:info("AAE sink locked tree for partition ~p got ~p",
               [State#state.partition, ResponseMsg]),
    send_reply(ResponseMsg, State);

%% no reply
process_msg(?MSG_COMPLETE, State=#state{owner=Owner}) ->
    lager:info("AAE sink got complete."),
    riak_repl2_fssink:fullsync_complete(Owner),
    State.

%% Send a response back to the aae_source worker

send_reply(Msg, State=#state{socket=Socket, transport=Transport}) ->
    Data = term_to_binary(Msg),
    ok = Transport:send(Socket, <<?MSG_REPLY:8, Data/binary>>),
    lager:info("sent reply ~p", [Msg]),
    State.

