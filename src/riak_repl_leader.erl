%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

%%===================================================================
%% Replication leader - responsible for receiving objects to be replicated
%% by the postcommit hook.  If the node is acting as the leader all
%% objects will be sent to any connected server sockets, if not the
%% object will be sent to the node acting as leader.
%%
%% riak_repl_leader_helper is used to perform the elections and work
%% around the gen_leader limit of having a fixed list of candidates.
%%===================================================================

-module(riak_repl_leader).
-behaviour(gen_server).

%% API
-export([start_link/0,
         set_candidates/2,
         leader_node/0,
         is_leader/0,
         postcommit/1,
         add_receiver_pid/1,
         rm_receiver_pid/1]).
-export([set_leader/3]).
-export([helper_pid/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Server name has changed for 0.14.0 now that the gen_leader
%% portion has been broken out into riak_repl_leader_helper. 
%% During rolling upgrades old gen_leader messages from pre-0.14
%% would be sent to the gen_server
-define(SERVER, riak_repl_leader_gs). 

-record(state, {helper_pid,  % pid of riak_repl_leader_helper
                i_am_leader=false :: boolean(), % true if the leader
                leader_node=undefined :: undefined | node(), % current leader
                leader_mref=undefined :: undefined | reference(), % monitor
                candidates=[] :: [node()],      % candidate nodes for leader
                workers=[node()] :: [node()],   % workers
                receivers=[] :: [{reference(),pid()}]}). % {Mref,Pid} pairs
     
%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Set the list of candidate nodes for replication leader
set_candidates(Candidates, Workers) ->
    gen_server:cast(?SERVER, {set_candidates, Candidates, Workers}).

%% Return the current leader node
leader_node() ->
    gen_server:call(?SERVER, leader_node).

%% Are we the leader?
is_leader() ->
    gen_server:call(?SERVER, is_leader).

%% Send the object to the leader
postcommit(Object) ->
    gen_server:cast(?SERVER, {repl, Object}).

%% Add the pid of a riak_repl_tcp_sender process.  The pid is monitored
%% and removed from the list when it exits. 
add_receiver_pid(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {add_receiver_pid, Pid}).

rm_receiver_pid(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {rm_receiver_pid, Pid}).

%%%===================================================================
%%% Callback for riak_repl_leader_helper
%%%===================================================================

%% Called by riak_repl_leader_helper whenever a leadership election
%% takes place.
set_leader(LocalPid, LeaderNode, LeaderPid) ->
    gen_server:call(LocalPid, {set_leader_node, LeaderNode, LeaderPid}).

%%%===================================================================
%%% Unit test support for riak_repl_leader_helper
%%%===================================================================

helper_pid() ->
    gen_server:call(?SERVER, helper_pid).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    erlang:send_after(0, self(), update_leader),
    {ok, #state{}}.

handle_call({add_receiver_pid, Pid}, _From, State) when State#state.i_am_leader =:= true ->
    Mref = erlang:monitor(process, Pid),
    UpdReceivers = orddict:store(Mref, Pid, State#state.receivers),
    {reply, ok, State#state{receivers = UpdReceivers}};
handle_call({add_receiver_pid, _Pid}, _From, State) ->
    {reply, {error, not_leader}, State};

handle_call({rm_receiver_pid, Pid}, _From, State = #state{receivers=R0}) when State#state.i_am_leader =:= true ->
    Receivers = case lists:keyfind(Pid, 2, R0) of
        {MRef, Pid} ->
            erlang:demonitor(MRef),
            orddict:erase(MRef, R0);
        false ->
            R0
    end,
    {reply, ok, State#state{receivers = Receivers}};
handle_call({rm_receiver_pid, _Pid}, _From, State) ->
    {reply, {error, not_leader}, State};

handle_call(leader_node, _From, State) ->
    {reply, State#state.leader_node, State};

handle_call(is_leader, _From, State) ->
    {reply, State#state.i_am_leader, State};

handle_call({set_leader_node, LeaderNode, LeaderPid}, _From, State) ->
    case node() of
        LeaderNode ->
            {reply, ok, become_leader(LeaderNode, State)};
        _ ->
            {reply, ok, new_leader(LeaderNode, LeaderPid, State)}
    end;

handle_call(helper_pid, _From, State) ->
    {reply, State#state.helper_pid, State}.

handle_cast({set_candidates, CandidatesIn, WorkersIn}, State) ->
    Candidates = lists:sort(CandidatesIn),
    Workers = lists:sort(WorkersIn),
    case {State#state.candidates, State#state.workers} of
        {Candidates, Workers} -> % no change to candidate list, leave helper alone
            {noreply, State};
        {_OldCandidates, _OldWorkers} ->
            UpdState1 = remonitor_leader(undefined, State),
            UpdState2 = UpdState1#state{candidates=Candidates, 
                                        workers=Workers,
                                        leader_node=undefined},
            leader_change(State#state.i_am_leader, false),
            {noreply, restart_helper(UpdState2)}
    end;
handle_cast({repl, Msg}, State) when State#state.i_am_leader =:= true ->
    case State#state.receivers of
        [] ->
            riak_repl_util:log_dropped_realtime_obj(Msg),
            riak_repl_stats:objects_dropped_no_clients();
        Receivers ->
            [P ! {repl, Msg} || {_Mref, P} <- Receivers],
            riak_repl_stats:objects_sent()
    end,
    {noreply, State};
handle_cast({repl, Msg}, State) when State#state.leader_node =/= undefined ->
    gen_server:cast({?SERVER, State#state.leader_node}, {repl, Msg}),
    riak_repl_stats:objects_forwarded(),
    {noreply, State};
handle_cast({repl, Msg}, State) ->
    %% No leader currently defined - cannot do anything
    riak_repl_util:log_dropped_realtime_obj(Msg),
    riak_repl_stats:objects_dropped_no_leader(),
    {noreply, State}.

handle_info(update_leader, State) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_repl_ring:get_repl_config(Ring) of
        undefined ->
            {noreply, State};
        _ ->
            riak_repl_ring_handler:update_leader(Ring),
            {noreply, State}
    end;
handle_info({'DOWN', Mref, process, _Object, _Info}, % dead riak_repl_leader
            #state{leader_mref=Mref}=State) ->
    case State#state.helper_pid of
        undefined ->
            ok;
        Pid ->
            riak_repl_leader_helper:refresh_leader(Pid)
    end,
    {noreply, State#state{leader_node = undefined, leader_mref = undefined}};
handle_info({'DOWN', Mref, process, _Object, _Info}, State) ->
    %% May be called here with a stale leader_mref, will not matter
    %% as it will not be in the State#state.receivers so will do nothing.
    UpdReceivers = orddict:erase(Mref, State#state.receivers),
    {noreply, State#state{receivers = UpdReceivers}};
handle_info({'EXIT', Pid, killed}, State=#state{helper_pid={killed,Pid}}) ->
    {noreply, maybe_start_helper(State)};
handle_info({'EXIT', Pid, Reason}, State=#state{helper_pid=Pid}) ->
    lager:warning(
      "Replication leader helper exited unexpectedly: ~p",
      [Reason]),
    {noreply, maybe_start_helper(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

become_leader(Leader, State) ->
    case State#state.leader_node of
        Leader ->
            NewState = State,
            lager:info("Re-elected as replication leader");
        _ ->
            riak_repl_stats:elections_elected(),
            riak_repl_stats:elections_leader_changed(),
            leader_change(State#state.i_am_leader, true),
            NewState1 = State#state{i_am_leader = true, leader_node = Leader},
            NewState = remonitor_leader(undefined, NewState1),
            lager:info("Elected as replication leader")
    end,
    NewState.

new_leader(Leader, LeaderPid, State) ->
    This = node(),
    case State#state.leader_node of
        This ->
            %% this node is surrendering leadership
            leader_change(State#state.i_am_leader, false), % will close connections
            riak_repl_stats:elections_leader_changed(),
            lager:info("Replication leadership surrendered to ~p", [Leader]);
        Leader ->
            lager:info("Replication leader kept as ~p", [Leader]),
            ok;
        _NewLeader ->
            riak_repl_stats:elections_leader_changed(),
            lager:info("Replication leader set to ~p", [Leader]),
            ok
    end,
    %% Set up a monitor on the new leader so we can make the helper
    %% check the elected node if it ever goes away.  This handles
    %% the case where all candidate nodes are down and only workers
    %% remain.
    NewState = State#state{i_am_leader = false, leader_node = Leader},
    remonitor_leader(LeaderPid, NewState).

remonitor_leader(LeaderPid, State) ->
    case State#state.leader_mref of
        undefined ->
            ok;
        OldMref ->
            erlang:demonitor(OldMref)
    end,
    case LeaderPid of
        undefined ->
            State#state{leader_mref = undefined};
        _ ->
            State#state{leader_mref = erlang:monitor(process, LeaderPid)}
    end.

%% Restart the helper 
restart_helper(State) ->    
    case State#state.helper_pid of
        undefined -> % no helper running, start one if needed
            maybe_start_helper(State);
        {killed, _OldPid} ->
            %% already been killed - waiting for exit
            State;
        OldPid ->
            %% Tell helper to stop - cannot use gen_server:call
            %% as may be blocked in an election.  The exit will
            %% be caught in handle_info and the helper will be restarted.
            exit(OldPid, kill),
            State#state{helper_pid = {killed, OldPid}}
    end.

maybe_start_helper(State) ->
    %% Start the helper if there are any candidates
    case State#state.candidates of
        [] ->
            Pid = undefined;
        Candidates ->
            {ok, Pid} = riak_repl_leader_helper:start_link(self(), Candidates,
                                                           State#state.workers)
    end,
    State#state{helper_pid = Pid}.

leader_change(A, A) ->
    %% nothing changed
    ok;
leader_change(false, true) ->
    %% we've become the leader
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_client_sup:ensure_sites(Ring);
leader_change(true, false) ->
    %% we've lost the leadership
    RunningSiteProcs = riak_repl_client_sup:running_site_procs(),
    [riak_repl_client_sup:stop_site(SiteName) || 
        {SiteName, _Pid} <- RunningSiteProcs],
    riak_repl_listener:close_all_connections().
