%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_helper).

%% @doc Realtime replication source helper
%%
%% High level responsibility...

-behaviour(gen_server).
%% API
-export([start_link/3,
         start_link/4,
         stop/1,
         status/1, status/2]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(dbg(Msg), ?debugMsg(Msg)).
-define(dbg(Fmt,Args),?debugFmt(Fmt, Args)).
-else.
-define(dbg(_Msg),ok).
-define(dbg(_Fmt,_Arg),ok).
-endif.

-record(state, {remote,     % remote site name
                transport,  % erlang module to use for transport
                socket,     % socket to pass to transport
                proto,      % protocol version negotiated
                deliver_fun,% Deliver function
                sent_seq,   % last sequence sent
                objects = 0}).   % number of objects sent - really number of pulls as could be multiobj

start_link(Remote, Transport, Socket) ->
    start_link(Remote, Transport, Socket, {1,0}).

start_link(Remote, Transport, Socket, Version) ->
    gen_server:start_link(?MODULE, [Remote, Transport, Socket, Version], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

status(Pid) ->
    status(Pid, app_helper:get_env(riak_repl, riak_repl2_rtsource_helper_status_to, 5000)).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

init([Remote, Transport, Socket, Version]) ->
    riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
    Me = self(),
    Deliver = fun(Result) -> gen_server:call(Me, {pull, Result}) end,
    State = #state{remote = Remote, transport = Transport, proto = Version,
                   socket = Socket, deliver_fun = Deliver},
    async_pull(State),
    {ok, State}.

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

handle_cast(_Msg, State) ->
    %% TODO: Log unhandled message
    {noreply, State}.

handle_info(_Msg, State) ->
    %% TODO: Log unknown msg
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% Trigger an async pull from the realtime queue
async_pull(#state{remote = Remote, deliver_fun = Deliver}) ->
    riak_repl2_rtq:pull(Remote, Deliver).

maybe_send(Transport, Socket, QEntry, State) ->
    {Seq, _NumObjects, _BinObjs, Meta} = QEntry,
    #state{remote = Remote} = State,
    Routed = get_routed(Meta),
    case lists:member(Remote, Routed) of
        true ->
            ?dbg("Didn't forward to ~p cause I'm lazy: ~p", [Remote, QEntry]),
            % TODO and if we haven't acked actual sends?
            %riak_repl2_rtq:ack(Remote, Seq),
            State;
        false ->
            QEntry2 = fix_meta(QEntry, Remote),
            Encoded = encode(QEntry2, State#state.proto),
            ?dbg("Forwarding to ~p with new data: ~p derived from ~p", [State#state.remote, QEntry2, QEntry]),
            Transport:send(Socket, Encoded),
            State
    end.

encode({Seq, _NumObjs, BinObjs, _Meta}, {1,0}) ->
    riak_repl2_rtframe:encode(objects, {Seq, BinObjs});
encode({Seq, _NumbOjbs, BinObjs, Meta}, {2,0}) ->
    riak_repl2_rtframe:encode(objects_and_meta, {Seq, BinObjs, Meta}).

get_routed(Meta) ->
    meta_get(routed_clusters, [], Meta).

meta_get(Key, Default, Meta) ->
    case orddict:find(Key, Meta) of
        error -> Default;
        {ok, Value} -> Value
    end.

fix_meta({_, _, _, Meta} = QEntry, Remote) ->
    LocalForwards = meta_get(local_forwards, [Remote], Meta),
    Routed = meta_get(routed_clusters, [], Meta),
    Meta2 = orddict:erase(local_forwards, Meta),
    Routed2 = lists:usort(Routed ++ LocalForwards),
    Meta3 = orddict:store(routed_clusters, Routed2, Meta2),
    setelement(4, QEntry, Meta3).