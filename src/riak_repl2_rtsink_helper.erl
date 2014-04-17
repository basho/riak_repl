%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtsink_helper).

%% @doc Realtime replication sink module
%%
%% High level responsibility...
%%  consider moving out socket responsibilities to another process
%%  to keep this one responsive (but it would pretty much just do status)
%%

%% API
-export([start_link/1,
         stop/1,
         write_objects/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-behavior(gen_server).

-record(state, {parent, workers}).

start_link(Parent) ->
    gen_server:start_link(?MODULE, [Parent], []).

stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

write_objects(Pid, BinObjs, DoneFun, Ver) ->
    gen_server:cast(Pid, {write_objects, BinObjs, DoneFun, Ver}).

%% Callbacks
init([Parent]) ->
    %% TODO: Share pool between all rtsinks to bound total RT work
    {ok, #state{parent=Parent, workers=[]}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast({unmonitor, Pid},
            #state{workers=Workers} = State0) ->
    State = case lists:keyfind(Pid, 1, Workers) of
        {Pid, MonitorRef} ->
            erlang:demonitor(MonitorRef),
            State0#state{workers=lists:keydelete(Pid, 1, Workers)};
        false ->
            State0
    end,
    {noreply, State};
handle_cast({write_objects, BinObjs, DoneFun, Ver},
            #state{workers=Workers} = State0) ->
    Self = self(),
    WrapperFun = fun() ->
            gen_server:cast(Self, {unmonitor, self()}),
            DoneFun()
    end,
    Worker = poolboy:checkout(riak_repl2_rtsink_pool, true, infinity),
    State = case lists:keyfind(Worker, 1, Workers) of
        false ->
            MonitorRef = monitor(process, Worker),
            State0#state{workers=Workers ++ [{Worker, MonitorRef}]};
        _ ->
            State0
    end,
    ok = riak_repl_fullsync_worker:do_binputs(
            Worker, BinObjs, WrapperFun, riak_repl2_rtsink_pool, Ver),
    {noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, Reason},
            #state{workers=Workers} = State0)
  when Reason == normal; Reason == shutdown ->
    {noreply, State0#state{workers=lists:keydelete(MRef, 2, Workers)}};
handle_info({'DOWN', MRef, process, Pid, Reason},
            #state{workers=Workers} = State0) ->
    lager:debug("Worker ~p died for reason: ~p", [Pid, Reason]),
    %% TODO: Needs graceful way to let rtsink know so it can die
    State = State0#state{workers=lists:keydelete(MRef, 2, Workers)},
    {stop, {worker_died, {Pid, Reason}}, State}.

terminate(_Reason, _State) ->
    %% TODO: Consider trying to do something graceful with poolboy?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
