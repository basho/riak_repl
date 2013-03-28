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
         terminate/2, code_change/3]).

%% API
-export([start_link/3, init_sync/1]).

-record(state, {
          clustername,
          socket,
          transport,
          tree_pid,        %% pid of the AAE tree
          partition,
          index_n          %% preflist index
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ClusterName, Transport, Socket) ->
    gen_server:start_link(?MODULE, [ClusterName, Transport, Socket], []).

%% Called after ownership of socket has been given to AAE sink worker
init_sync(AAEWorker) ->
    gen_server:call(AAEWorker, init_sync, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ClusterName, Transport, Socket]) ->
    {ok, #state{clustername=ClusterName, socket=Socket, transport=Transport}}.

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

handle_info({'DOWN', _, _, _, _}, _State) ->
    {stop, tree_down};

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
process_msg(?MSG_INIT, {Partition,IndexN}, State) ->
    {ok, TreePid} = riak_kv_vnode:hashtree_pid(Partition),
    %% monitor the tree and crash if the tree goes away
    monitor(process, TreePid),
    send_reply(ok, State),
    State#state{partition=Partition, index_n=IndexN, tree_pid=TreePid};

process_msg(?MSG_GET_AAE_BUCKET, {Level,BucketNum}, State=#state{index_n=IndexN,
                                                                 tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_bucket(IndexN, Level, BucketNum, TreePid),
    send_reply(ResponseMsg, State);

process_msg(?MSG_GET_AAE_SEGMENT, SegmentNum, State=#state{index_n=IndexN,
                                                           tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:exchange_segment(IndexN, SegmentNum, TreePid),
    send_reply(ResponseMsg, State).

%% replies: ok | not_built | already_locked
process_msg(?MSG_LOCK_TREE, State=#state{tree_pid=TreePid}) ->
    %% NOTE: be sure to die if tcp connection dies, to give back lock
    ResponseMsg = riak_kv_index_hashtree:get_lock(TreePid, fullsync_sink),
    send_reply(ResponseMsg, State);

%% replies: ok | not_responsible
process_msg(?MSG_UPDATE_TREE, State=#state{index_n=IndexN, tree_pid=TreePid}) ->
    ResponseMsg = riak_kv_index_hashtree:update(IndexN, TreePid),
    send_reply(ResponseMsg, State).

%% Send a response back to the aae_source worker

send_reply(Msg, State=#state{socket=Socket, transport=Transport}) ->
    Data = term_to_binary(Msg),
    ok = Transport:send(Socket, <<?MSG_REPLY:8, Data/binary>>),
    State.
