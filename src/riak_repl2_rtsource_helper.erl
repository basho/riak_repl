%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsource_helper).

%% @doc Realtime replication source helper
%%
%% High level responsibility...

-behaviour(gen_server).
%% API
-export([start_link/3,
         stop/1,
         status/1, status/2]).

-define(SERVER, ?MODULE).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-record(state, {remote,     % remote site name
                transport,  % erlang module to use for transport
                socket,     % socket to pass to transport
                deliver_fun,% Deliver function
                sent_seq,   % last sequence sent
                objects = 0}).   % number of objects sent - really number of pulls as could be multiobj

start_link(Remote, Transport, Socket) ->
    gen_server:start_link(?MODULE, [Remote, Transport, Socket], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

status(Pid) ->
    status(Pid, app_helper:get_env(riak_repl, riak_repl2_rtsource_helper_status_to, 5000)).

status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).

init([Remote, Transport, Socket]) ->
    riak_repl2_rtq:register(Remote), % re-register to reset stale deliverfun
    Me = self(),
    Deliver = fun(Result) -> gen_server:call(Me, {pull, Result}) end,
    State = #state{remote = Remote, transport = Transport, 
                   socket = Socket, deliver_fun = Deliver},
    async_pull(State),
    {ok, State}.

handle_call({pull, {error, Reason}}, _From, State) ->
    {stop, {queue_error, Reason}, State};
handle_call({pull, {Seq, NumObjects, BinObjs}}, From,
            State = #state{transport = T, socket = S, objects = Objects}) ->
    %% unblock the rtq as fast as possible
    gen_server:reply(From, ok),
    TcpIOL = riak_repl2_rtframe:encode(objects, {Seq, BinObjs}),
    T:send(S, TcpIOL),
    async_pull(State),
    {noreply, State#state{sent_seq = Seq, objects = Objects + NumObjects}};
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
