%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_helper).

%% @doc Realtime replication source helper
%%
%% High level responsibility...

-behaviour(gen_server).
%% API
-export([start_link/4,
         stop/1,
         v1_ack/2,
         status/1, status/2, send_heartbeat/1]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {remote,     % remote site name
                transport,  % erlang module to use for transport
                socket,     % socket to pass to transport
                proto,      % protocol version negotiated
                deliver_fun,% Deliver function
                sent_seq,   % last sequence sent
                v1_offset = 0,
                v1_seq_map = [],
                objects = 0}).   % number of objects sent - really number of pulls as could be multiobj

start_link(Remote, Transport, Socket, Version) ->
    gen_server:start_link(?MODULE, [Remote, Transport, Socket, Version], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

%% @doc v1 sinks require fully sequential sequence numbers sent. The outgoing
%% Seq's are munged, and thus must be munged back when the sink replies.
v1_ack(Pid, Seq) ->
    gen_server:cast(Pid, {v1_ack, Seq}).

status(Pid) ->
    status(Pid, app_helper:get_env(riak_repl, riak_repl2_rtsource_helper_status_to, 5000)).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

send_heartbeat(Pid) ->
    %% Cast the heartbeat, do not want to block the rtsource process
    %% as it is responsible for checking heartbeat
    gen_server:cast(Pid, send_heartbeat).

init([Remote, Transport, Socket, Version]) ->
    riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
    Me = self(),
    Deliver = fun(Result) -> gen_server:call(Me, {pull, Result}) end,
    State = #state{remote = Remote, transport = Transport, proto = Version,
                   socket = Socket, deliver_fun = Deliver},
    async_pull(State),
    {ok, State}.

%% @doc BinObjs are in new riak binary object format. If the remote sink
%%      is storing older non-binary objects, then we need to downconvert
%%      the objects before sending. V is the format expected by the sink.
maybe_downconvert_binary_objs(BinObjs, w0) ->
    %% old sink. downconvert
    Objs = riak_repl_util:from_wire(w1, BinObjs),
    riak_repl_util:to_wire(w0, Objs);
maybe_downconvert_binary_objs(BinObjs, w1) ->
    %% great! nothing to do.
    BinObjs.

handle_call({pull, {error, Reason}}, _From, State) ->
    riak_repl_stats:rt_source_errors(),
    {stop, {queue_error, Reason}, ok, State};
handle_call({pull, {Seq, NumObjects, _BinObjs, _Meta} = Entry}, From,
            State = #state{transport = T, socket = S, objects = Objects}) ->
    %% unblock the rtq as fast as possible
    gen_server:reply(From, ok),
    State2 = maybe_send(T, S, Entry, State),
    async_pull(State2),
    {noreply, State2#state{sent_seq = Seq, objects = Objects + NumObjects}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(status, _From, State = 
                #state{sent_seq = SentSeq, objects = Objects}) ->
    {reply, [{sent_seq, SentSeq},
             {objects, Objects}], State}.

handle_cast(send_heartbeat, State = #state{transport = T, socket = S}) ->
    HBIOL = riak_repl2_rtframe:encode(heartbeat, undefined),
    T:send(S, HBIOL),
    {noreply, State};

handle_cast({v1_ack, Seq}, State = #state{v1_seq_map = Map}) ->
    case orddict:find(Seq, Map) of
        error ->
            ok;
        {ok, RealSeq} ->
            riak_repl2_rtq:ack(State#state.remote, RealSeq)
    end,
    Map2 = orddict:erase(Seq, Map),
    {noreply, State#state{v1_seq_map = Map2}};

handle_cast(Msg, State) ->
    lager:info("Realtime source helper received unexpected cast - ~p\n", [Msg]).


handle_info(Msg, State) ->
    lager:info("Realtime source helper received unexpected message - ~p\n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Trigger an async pull from the realtime queue
async_pull(#state{remote = Remote, deliver_fun = Deliver}) ->
    riak_repl2_rtq:pull(Remote, Deliver).

maybe_send(Transport, Socket, QEntry, State) ->
    {_Seq, _NumObjects, _BinObjs, Meta} = QEntry,
    #state{remote = Remote} = State,
    Routed = get_routed(Meta),
    case lists:member(Remote, Routed) of
        true ->
            lager:debug("Did not forward to ~p; destination already in routed list", [Remote]),
            State;
        false ->
            QEntry2 = merge_forwards_and_routed_meta(QEntry, Remote),
            {Encoded, State2} = encode(QEntry2, State),
            lager:debug("Forwarding to ~p with new data: ~p derived from ~p", [State#state.remote, QEntry2, QEntry]),
            Transport:send(Socket, Encoded),
            State2
    end.

encode({Seq, _NumObjs, BinObjs, Meta}, State = #state{proto = Ver}) when Ver < {2,0} ->
    Skips = orddict:fetch(skip_count, Meta),
    Offset = State#state.v1_offset + Skips,
    Seq2 = Seq - Offset,
    V1Map = orddict:store(Seq2, Seq, State#state.v1_seq_map),
    BinObjs2 = maybe_downconvert_binary_objs(BinObjs, w0),
    Encoded = riak_repl2_rtframe:encode(objects, {Seq2, BinObjs2}),
    State2 = State#state{v1_offset = Offset, v1_seq_map = V1Map},
    {Encoded, State2};
encode({Seq, _NumbOjbs, BinObjs, Meta}, State = #state{proto = {2,0}}) ->
    {riak_repl2_rtframe:encode(objects_and_meta, {Seq, BinObjs, Meta}), State}.

get_routed(Meta) ->
    meta_get(routed_clusters, [], Meta).

meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

merge_forwards_and_routed_meta({_, _, _, Meta} = QEntry, Remote) ->
    LocalForwards = meta_get(local_forwards, [Remote], Meta),
    Routed = meta_get(routed_clusters, [], Meta),
    Self = riak_core_connection:symbolic_clustername(),
    Meta2 = orddict:erase(local_forwards, Meta),
    Routed2 = lists:usort(Routed ++ LocalForwards ++ [Self]),
    Meta3 = orddict:store(routed_clusters, Routed2, Meta2),
    setelement(4, QEntry, Meta3).
