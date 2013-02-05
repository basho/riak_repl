%% Riak EnterpriseDS
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_pg).

%% @doc Riak CS Proxy-get
%%
%%
-export([start_link/0, status/0]).
-export([enable/1, disable/1, enabled/0, start/1, stop/1, started/0]).
%-export([ensure_rt/2, register_remote_locator/0, register_remote_pg_locator/0, postcommit/1]).
-export([register_remote_locator/0, ensure_pg/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-record(state, {sinks = []}).

%% API - is there any state? who watches ring events?
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

status() ->
    gen_server:call(?SERVER, status, infinity).

enabled() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_repl_ring:pg_enabled(Ring).

started() ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    riak_repl_ring:pg_started(Ring).

enable(Remote) ->
    do_ring_trans(fun riak_repl_ring:pg_enable_trans/2, Remote).

        disable(Remote) ->
            F = fun(Ring, Remote1) ->
                R2 = case riak_repl_ring:pg_stop_trans(Ring, Remote1) of
                    {new_ring, R1} ->
                        R1;
                        {ignore, _} ->
                         Ring
                     end,
                     riak_repl_ring:pg_disable_trans(R2, Remote1)
                 end,
                 do_ring_trans(F, Remote).

                 start(Remote) ->
    F = fun(Ring, Remote1) ->
            lager:info("Foo1"),
                case lists:member(Remote, riak_repl_ring:pg_enabled(Ring)) of
                    true ->
                        lager:info("Foo2"),
                        riak_repl_ring:pg_start_trans(Ring, Remote1);
                    _ ->
                        {ignore, {not_enabled, Remote1}}
                end
        end,
    do_ring_trans(F, Remote).

stop(Remote) ->
    do_ring_trans(fun riak_repl_ring:pg_stop_trans/2, Remote).

ensure_pg(_WantEnabled0, WantStarted0) ->
    ToStart = WantStarted0,
    lager:info("Proxy-get starting ~p", [ToStart]),
    [riak_repl2_pg_block_provider_sup:enable(Remote) || Remote <- ToStart].

%    CStatus = proplists:get_value(consumers, Status, []),
%    WantEnabled = lists:usort(WantEnabled0),
%    WantStarted = lists:usort(WantStarted0),
%    Status = riak_repl2_rtq:status(),
%    CStatus = proplists:get_value(consumers, Status, []),
%    Enabled = lists:sort([Remote || {Remote, _Stats} <- CStatus]),
%    Started = lists:sort([Remote || {Remote, _Pid}  <- riak_repl2_rtsource_conn_sup:enabled()]),
%
%    ToEnable  = WantEnabled -- Enabled,
%    ToDisable = Enabled -- WantEnabled,
%    ToStart   = WantStarted -- Started,
%    ToStop    = Started -- WantStarted,

    %% Set up secret appenv to be master control of
    %% repl across all buckets.  If no sites are enabled,
    %% don't even fire the hook.
%    case WantEnabled of
%        [] ->
%            application:set_env(riak_repl, pgenabled, false);
%        _ ->
%            application:set_env(riak_repl, pgenabled, true)
%    end,

%    case ToEnable ++ ToDisable ++ ToStart ++ ToStop of
%        [] ->
%            [];
%        _ ->
%            %% Do enables/starts first to capture maximum amount of rtq
%
%            %% Create a registration to begin queuing, rtsource_sup:ensure_started 
%            %% will bring up an rtsource process that will re-register
%            [riak_repl2_rtq:register(Remote) || Remote <- ToEnable],
%            [riak_repl2_rtsource_conn_sup:enable(Remote) || Remote <- ToStart],
%
%            %% Stop running sources, re-register to get rid of pending
%            %% deliver functions
%            [begin
%                 riak_repl2_rtsource_conn_sup:disable(Remote),
%                 riak_repl2_rtq:register(Remote)
%             end || Remote <- ToStop],
%
%            %% Unregister disabled sources, freeing up the queue
%            [riak_repl2_rtq:unregister(Remote) || Remote <- ToDisable],
%
%            [{enabled, ToEnable},
%             {started, ToStart},
%             {stopped, ToStop},
%             {disabled, ToDisable}]
%    end.

register_remote_locator() ->
    Locator = fun(Name, _Policy) ->
            riak_core_cluster_mgr:get_ipaddrs_of_cluster(Name)
    end,
    ok = riak_core_connection_mgr:register_locator(proxy_get, Locator).

%% gen_server callbacks
init([]) ->
     {ok, #state{}}.

handle_call(status, _From, State) ->
    Status = {proxy_get, not_implemented},
    {reply, Status, State}.

handle_cast(_Msg, State) ->
    %% TODO: log unknown message
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SinkPid, _Reason}, 
            State = #state{sinks = Sinks}) ->
    %%TODO: Check how ranch logs sink process death
    Sinks2 = Sinks -- [SinkPid],
    {noreply, State#state{sinks = Sinks2}};
handle_info(Msg, State) ->
    lager:warn("unhandled message - e.g. timed out status result: ~p", Msg),
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
