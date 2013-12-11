%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rt).

%% @doc Realtime replication
%%
%% High level responsibility...
%%
-export([start_link/0, status/0, register_sink/1, get_sink_pids/0]).
-export([enable/1, disable/1, enabled/0, start/1, stop/1, started/0]).
-export([ensure_rt/2, register_remote_locator/0, postcommit/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {sinks = []}).

%% API - is there any state? who watches ring events?
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Status for the realtime repl subsystem
status() ->
    gen_server:call(?SERVER, status, infinity).

%% Add realtime repliation to remote, do not enable yet
enable(Remote) ->
    do_ring_trans(fun riak_repl_ring:rt_enable_trans/2, Remote).


%% Delete relatime repliation to remote
disable(Remote) ->
    F = fun(Ring, Remote1) ->
                R2 = case riak_repl_ring:rt_stop_trans(Ring, Remote1) of
                         {new_ring, R1} ->
                             R1;
                         {ignore, _} ->
                             Ring
                     end,
                riak_repl_ring:rt_disable_trans(R2, Remote1)
        end,
    do_ring_trans(F, Remote).

enabled() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_repl_ring:rt_enabled(Ring).

%% Enable
start(Remote) ->
    F = fun(Ring, Remote1) ->
                case lists:member(Remote, riak_repl_ring:rt_enabled(Ring)) of
                    true ->
                        riak_repl_ring:rt_start_trans(Ring, Remote1);
                    _ ->
                        {ignore, {not_enabled, Remote1}}
                end
        end,
    do_ring_trans(F, Remote).

%% Disable realtime replication
stop(Remote) ->
    do_ring_trans(fun riak_repl_ring:rt_stop_trans/2, Remote).

started() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_repl_ring:rt_started(Ring).

%% Ensure the running realtime repl configuration on this node matches
%% the desired configuration in the ring.
ensure_rt(WantEnabled0, WantStarted0) ->
    WantEnabled = lists:usort(WantEnabled0),
    WantStarted = lists:usort(WantStarted0),
    Status = riak_repl2_rtq:status(),
    CStatus = proplists:get_value(consumers, Status, []),
    Enabled = lists:sort([Remote || {Remote, _Stats} <- CStatus]),
    Started = lists:sort([Remote || {Remote, _Pid}  <- riak_repl2_rtsource_conn_sup:enabled()]),

    ToEnable  = WantEnabled -- Enabled,
    ToDisable = Enabled -- WantEnabled,
    ToStart   = WantStarted -- Started,
    ToStop    = Started -- WantStarted,

    %% Set up secret appenv to be master control of
    %% repl across all buckets.  If no sites are enabled,
    %% don't even fire the hook.
    case WantEnabled of
        [] ->
            application:set_env(riak_repl, rtenabled, false);
        _ ->
            application:set_env(riak_repl, rtenabled, true)
    end,

    case ToEnable ++ ToDisable ++ ToStart ++ ToStop of
        [] ->
            [];
        _ ->
            %% Do enables/starts first to capture maximum amount of rtq

            %% Create a registration to begin queuing, rtsource_sup:ensure_started
            %% will bring up an rtsource process that will re-register
            [riak_repl2_rtq:register(Remote) || Remote <- ToEnable],
            [riak_repl2_rtsource_conn_sup:enable(Remote) || Remote <- ToStart],

            %% Stop running sources, re-register to get rid of pending
            %% deliver functions
            [begin
                 riak_repl2_rtsource_conn_sup:disable(Remote),
                 riak_repl2_rtq:register(Remote)
             end || Remote <- ToStop],

            %% Unregister disabled sources, freeing up the queue
            [riak_repl2_rtq:unregister(Remote) || Remote <- ToDisable],

            [{enabled, ToEnable},
             {started, ToStart},
             {stopped, ToStop},
             {disabled, ToDisable}]
    end.

register_remote_locator() ->
    Locator = fun(Name, _Policy) ->
            riak_core_cluster_mgr:get_ipaddrs_of_cluster(Name)
    end,
    ok = riak_core_connection_mgr:register_locator(rt_repl, Locator).

%% Register an active realtime sink (supervised under ranch)
register_sink(Pid) ->
    gen_server:call(?SERVER, {register_sink, Pid}, infinity).

%% Get list of sink pids
%% TODO: Remove this once rtsink_sup is working right
get_sink_pids() ->
    gen_server:call(?SERVER, get_sink_pids, infinity).

%% Realtime replication post-commit hook
postcommit(RObj) ->
    lager:debug("maybe a mutate happened?~n    ~p", [RObj]),
    case riak_repl_util:repl_helper_send_realtime(RObj, riak_client:new(node(), undefined)) of
        %% always put the objects onto the shared queue in the new format; we'll
        %% down-convert if we have to before sending them to the RT sinks (based
        %% on what the RT source and sink negotiated as the common version).
        Objects0 when is_list(Objects0) ->
            Objects = Objects0 ++ [RObj],
            Meta = check_for_typed_bucket(RObj),
            BinObjs = riak_repl_util:to_wire(w1, Objects),
            %% try the proxy first, avoids race conditions with unregister()
            %% during shutdown
            case whereis(riak_repl2_rtq_proxy) of
                undefined ->
                    riak_repl2_rtq:push(length(Objects), BinObjs, Meta);
                _ ->
                    %% we're shutting down and repl is stopped or stopping...
                    riak_repl2_rtq_proxy:push(length(Objects), BinObjs, Meta)
            end;
        cancel -> % repl helper callback requested not to send over realtime
            ok
    end.

%% gen_server callbacks
init([]) ->
     {ok, #state{}}.

handle_call(status, _From, State = #state{sinks = SinkPids}) ->
    Timeout = app_helper:get_env(riak_repl, status_timeout, 5000),
    Sources = [try
                   riak_repl2_rtsource_conn:status(Pid, Timeout)
               catch
                   _:_ ->
                       {Remote, Pid, unavailable}
               end || {Remote, Pid} <- riak_repl2_rtsource_conn_sup:enabled()],
    Sinks = [try
                 riak_repl2_rtsink_conn:status(Pid, Timeout)
             catch
                 _:_ ->
                     {will_be_remote_name, Pid, unavailable}
             end || Pid <- SinkPids],
    Status = [{enabled, enabled()},
              {started, started()},
              {q,       riak_repl2_rtq:status()},
              {sources, Sources},
              {sinks, Sinks}],
    {reply, Status, State};
handle_call({register_sink, SinkPid}, _From, State = #state{sinks = Sinks}) ->
    Sinks2 = [SinkPid | Sinks],
    monitor(process, SinkPid),
    {reply, ok, State#state{sinks = Sinks2}};
handle_call(get_sink_pids, _From, State = #state{sinks = Sinks}) ->
    {reply, Sinks, State}.

handle_cast(_Msg, State) ->
    %% TODO: log unknown message
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SinkPid, _Reason},
            State = #state{sinks = Sinks}) ->
    %%TODO: Check how ranch logs sink process death
    Sinks2 = Sinks -- [SinkPid],
    {noreply, State#state{sinks = Sinks2}};
handle_info(Msg, State) ->
    %%TODO: Log unhandled message - e.g. timed out status result
    lager:warning("unhandled message - e.g. timed out status result: ~p", Msg),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_ring_trans(F, A) ->
    case riak_core_ring_manager:ring_trans(F, A) of
        {ok, _} ->
            ok;
        ER ->
            ER
    end.

check_for_typed_bucket(Obj) ->
    M = orddict:new(),
    case riak_object:bucket(Obj) of
        {_T, _B } ->
            lager:info("found typed bucket, setting meta data"),
            orddict:store(typed_bucket, true, M);
        _B -> 
            orddict:store(typed_bucket, false, M)
    end.
