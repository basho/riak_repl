%% Riak Replication Subprotocol Server Dispatch and Client Connections
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%

-module(riak_core_connection_mgr).
-behaviour(gen_server).

-include("riak_core_connection.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% controls retry and backoff.
-define(INITIAL_BACKOFF, 1 * 1000).  %% 1 second initial backoff
-define(MAX_BACKOFF, 5 * 60 * 1000). %% 5 minute maximum backoff

%%-define(TRACE(Stmt),Stmt).
-define(TRACE(Stmt),ok).

-define(SERVER, riak_core_connection_manager).
-define(MAX_LISTENERS, 100).

-type(counter() :: non_neg_integer()).

%% Connection manager strategy (per Jon M.)
%% when a connection request comes in,
%% + call the locator service to get the list of {transport, {address, port}}
%% + create a linked helper process to call riak_core_connection (just once) on the next available
%%   connection (ignore blacklisted ones, they'll get picked up if a repeat is necessary)
%% + on connection it transfers control of the socket back to the connmgr, casts a success message back
%%   to the connection manager and exits normally.
%%   - on success, the connection manager increments successful connects, reset the backoff timeout on
%%     that connection.
%%   - on failure, casts a failure message back to the connection manager (error, timeout etc) the
%%     connection manager marks the {Transport, {Address, Port}} as blacklisted, increases the failure
%%     counter and starts a timer for the backoff time (and updates it for next time). The connection
%%     manager checks for the next non--blacklisted endpoint in the connection request list to launch
%%     a new connection, if the list is empty call the locator service again to get a new list. If all
%%     connections are blacklisted, use send_after message to wake up and retry (perhaps with backoff
%%     time too).

%% End-point status state, updated for failed and successful connection attempts,
%% or by timers that fire to update the backoff time.
%% TODO: add folsom window'd stats
%% handle an EXIT from the helper process if it dies
-record(ep, {addr,                                 %% endpoint {IP, Port}
             nb_curr_connections = 0 :: counter(), %% number of current connections
             nb_success = 0 :: counter(),   %% total successfull connects on this ep
             nb_failures = 0 :: counter(),  %% total failed connects on this ep
             is_black_listed = false :: boolean(), %% true after a failed connection attempt
             backoff_delay=0 :: counter(),  %% incremented on each failure, reset to zero on success
             failures = orddict:new() :: orddict:orddict(), %% failure reasons
             last_fail                      %% time of last failure
             }).

%% connection request record
-record(req, {ref,      % Unique reference for this connection request
              pid,      % Helper pid trying to make connection
              target,   % target to connect to {Type, Name}
              spec,     % client spec
              strategy, % connection strategy
              cur       % current connection endpoint
             }).   % ordered list to try and connect to
              

%% connection manager state:
%% cluster_finder := function that returns the ip address
-record(state, {is_paused = false :: boolean(),
                cluster_finder = fun() -> {error, undefined} end :: cluster_finder_fun(),
                pending = [] :: [#req{}], % pending requests
                %% endpoints :: {module(),ip_addr()} -> ep()
                endpoints = orddict:new() :: orddict:orddict(), %% known endpoints w/status
                locators = orddict:new() :: orddict:orddict(), %% connection locators
                nb_total_succeeded = 0 :: counter(),
                nb_total_failed = 0 :: counter()
               }).

-export([start_link/0,
         resume/0,
         pause/0,
         is_paused/0,
         set_cluster_finder/1,
         get_cluster_finder/0,
         connect/2, connect/3,
         register_locator/2,
         apply_locator/2
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
-export([connection_helper/4, increase_backoff/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() -> {ok, pid()}).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% resume() will begin/resume accepting and establishing new connections, in
%% order to maintain the protocols that have been (or continue to be) registered
%% and unregistered. pause() will not kill any existing connections, but will
%% cease accepting new requests or retrying lost connections.
-spec(resume() -> ok).
resume() ->
    gen_server:cast(?SERVER, resume).

-spec(pause() -> ok).
pause() ->
    gen_server:cast(?SERVER, pause).

%% return paused state
is_paused() ->
    gen_server:call(?SERVER, is_paused).

%% Specify a function that will return the IP/Port of our Cluster Manager.
%% Connection Manager will call this function each time it wants to find the
%% current ClusterManager
-spec(set_cluster_finder(cluster_finder_fun()) -> ok).
set_cluster_finder(Fun) ->
    gen_server:cast(?SERVER, {set_cluster_finder, Fun}).

%% Return the current function that finds the Cluster Manager
get_cluster_finder() ->
    gen_server:call(?SERVER, get_cluster_finder).

%% Register a locator - for the given Name and strategy it returns {ok, [{IP,Port}]}
%% list of endpoints to connect to, in order. The list may be empty.  
%% If the query can never be answered
%% return {error, Reason}.
%% fun(Name
register_locator(Type, Fun) ->
    gen_server:call(?SERVER, {register_locator, Type, Fun}, infinity).

apply_locator(Name, Strategy) ->
    gen_server:call(?SERVER, {apply_locator, Name, Strategy}, infinity).

%% Establish a connection to the remote destination. be persistent about it,
%% but not too annoying to the remote end. Connect by name of cluster or
%% IP address. Use default strategy to find "best" peer for connection.
connect(Target, ClientSpec) ->
    gen_server:call(?SERVER, {connect, Target, ClientSpec, default}).

connect(Target, ClientSpec, Strategy) ->
    gen_server:call(?SERVER, {connect, Target, ClientSpec, Strategy}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{is_paused = true}}.

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

handle_call(get_cluster_finder, _From, State) ->
    {reply, State#state.cluster_finder, State};

%% connect based on address. Return process id of helper
handle_call({connect, Target, ClientSpec, Strategy}, _From, State) ->
    Reference = make_ref(),
    Request = #req{ref = Reference,
                   target = Target,
                   pid = undefined,
                   spec = ClientSpec,
                   strategy = Strategy},
    {reply, {ok, Reference}, start_request(Request, State)};

handle_call({register_locator, Type, Fun}, _From,
            State = #state{locators = Locators}) ->
    {reply, ok, State#state{locators = orddict:store(Type, Fun, Locators)}};

handle_call({apply_locator, Target, Strategy}, _From,
            State = #state{locators = Locators}) ->
    AddrsOrError = locate_endpoints(Target, Strategy, Locators),
    {reply, AddrsOrError, State};

handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.

handle_cast(pause, State) ->
    {noreply, State#state{is_paused = true}};

handle_cast(resume, State) ->
    {noreply, State#state{is_paused = false}};

handle_cast({set_cluster_finder, FinderFun}, State) ->
    {noreply, State#state{cluster_finder=FinderFun}};

%% helper process says it failed to reach an address.
handle_cast({endpoint_failed, Addr, Reason}, State) ->
    %% mark connection as black-listed and start timer for reset
    {noreply, fail_endpoint(Addr, Reason, State)};

%% helper process says the connection succeeded to this address
handle_cast({endpoint_connected, Addr}, State) ->
    {noreply, connect_endpoint(Addr, State)};

%% message from the helper process. It ran out of endpoints to try.
%% it terminated. start a new one.
handle_cast({endpoints_exhausted, Ref}, State) ->
    %% remove the exhausted helper process pid from our list of pending connections.
    {noreply, schedule_retry(1000, Ref, State)}.

%% it is time to remove Addr from the black-listed addresses
handle_info({backoff_timer, Addr}, State = #state{endpoints = EPs}) ->
    case orddict:find(Addr, EPs) of
        {ok, EP} ->
            EP2 = EP#ep{is_black_listed = false},
            {noreply, State#state{endpoints = orddict:store(Addr,EP2,EPs)}};
        error ->
            %% TODO: Should never happen because the Addr came from the EP list.
            {norepy, State}
    end;
handle_info({retry_req, Ref}, State = #state{pending = Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            %% TODO: should never happen
            {noreply, State};
        Req ->
            {noreply, start_request(Req, State)}
    end;
    
%%% All of the connection helpers end here
%% cases:
%% helper succeeded -> update EP stats: BL<-false, backoff_delay<-0
%% helper failed -> updates EP stats: failures++, backoff_delay++
%% other Pid failed -> pass on linked error
handle_info({'EXIT', From, Reason}, State = #state{pending = Pending}) ->
    ?debugFmt("handle_info: From=~p Reason=~p Pending=~p", [From, Reason, Pending]),
    %% Work out which endpoint it was
    case lists:keytake(From, #req.pid, Pending) of
        false ->
            %% Must have been something we were linked to, or linked to us
            exit({linked, From, Reason});
        {value, Req = #req{cur = Cur}, Pending2} ->
            case Reason of
                ok -> % riak_core_connection set up and handlers called
                    {noreply, connect_endpoint(Cur, State#state{pending = Pending2})};

                Reason -> % something bad happened to the connection
                    State2 = fail_endpoint(Cur, Reason, State),
                    {noreply, schedule_retry(1000, Req, State2)}
            end
    end;
handle_info(_Unhandled, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server info: ~p", [_Unhandled])),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Private
%%%===================================================================

%% schedule a retry to occur after Interval milliseconds
schedule_retry(Interval, Ref, State = #state{pending = Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            State;
        Req ->
            erlang:send_after(Interval, self(), {retry_req, Ref}),
            State#state{pending = lists:keystore(Ref, #req.ref, Pending,
                                                 Req#req{pid = undefined,
                                                         cur = undefined})}
    end.

%% Start process to make connection to available endpoints. Return a reference for
%% this connection attempt.
start_request(Req = #req{ref=Ref, target=Target, spec=ClientSpec, strategy=Strategy},
              State) ->
    case locate_endpoints(Target, Strategy, State#state.locators) of
        {ok, []} ->
            %% locators provided no addresses
            gen_server:cast(?SERVER, {conmgr_no_endpoints, Ref}),
            Interval = app_helper:get_env(riak_core, connmgr_no_endpoint_retry, 5000),
            %% schedule a retry
            schedule_retry(Interval, Ref, State);
        {ok, EpAddrs } ->
            AllEps = update_endpoints(EpAddrs, State#state.endpoints),
            TryAddrs = filter_blacklisted_endpoints(EpAddrs, AllEps),
            Pid = spawn_link(
                    fun() -> exit(try connection_helper(Ref, ClientSpec, Strategy, TryAddrs)
                                  catch T:R -> {exception, {T, R}}
                                  end)
                    end),
            State#state{endpoints = AllEps,
                        pending = lists:keystore(Ref, #req.ref, State#state.pending,
                                                 Req#req{pid = Pid,
                                                         cur = undefined})};
        {error, Reason} ->
            fail_request(Reason, Req, State)
    end.

%% increase the backoff delay, but cap at a maximum
increase_backoff(0) ->
    ?INITIAL_BACKOFF;
increase_backoff(Delay) when Delay > ?MAX_BACKOFF ->
    ?MAX_BACKOFF;
increase_backoff(Delay) ->
    2 * Delay.

%% least_connected_eps(ClusterName, State) ->
%%     case orddict:find(ClusterName, State#state.peer_addrs) of
%%         {ok, Addrs} ->
%%             AllEPs = orddict:to_list(State#state.endpoints),
%%             EPs = [X || {{_T,Addr},EP}=X <- AllEPs,      %% all endpoints where
%%                         lists:member(Addr,Addrs),        %%  Addr is in remote cluster
%%                         EP#ep.is_black_listed == false], %%  and not black-listed
%%             lists:sort(fun({_A,A},{_B,B}) -> A#ep.nb_curr_connections > B#ep.nb_curr_connections end,
%%                        EPs);
%%         error ->
%%             %% nothing found for this cluster :-(
%%             lager:error("No known end points for cluster: ~p", [ClusterName]),
%%             []
%%     end.

%% A spawned process that will walk down the list of endpoints and try them
%% all until exhausting the list.
connection_helper(Ref, _Protocol, _Strategy, []) ->
    %% exhausted the list of endpoints. let server start new helper process
    gen_server:cast(?SERVER, {endpoints_exhausted, Ref});
connection_helper(Ref, Protocol, Strategy, [Addr|Addrs]) ->
    case riak_core_connection:sync_connect(Addr, Protocol) of
        ok ->
            %% notify connection manager of success
            gen_server:cast(?SERVER, {endpoint_connected, Addr});
        {error, Reason} ->
            %% notify connection manager this EP failed and try next one
            gen_server:cast(?SERVER, {endpoint_failed, Addr, Reason}),
            connection_helper(Ref, Protocol, Strategy, Addrs)
    end.

locate_endpoints({Type, Name}, Strategy, Locators) ->
    case orddict:find(Type, Locators) of
        {ok, Locate} ->
            Locate(Name, Strategy);
        error ->
            {error, {unknown_target_type, Type}}
    end.

%% Make note of the failed connection attempt and update
%% our book keeping for that endpoint. Black-list it, and
%% adjust a backoff timer so that we wait a while before
%% trying this endpoint again.
fail_endpoint(Addr, Reason, State) ->
    Fun = fun(EP=#ep{backoff_delay = Backoff, failures = Failures}) ->
                  erlang:send_after(Backoff, self(), {backoff_timer, Addr}),
                  EP#ep{failures = orddict:update_counter(Reason, 1, Failures),
                        nb_failures = EP#ep.nb_failures + 1,
                        backoff_delay = increase_backoff(Backoff),
                        last_fail = os:timestamp(),
                        is_black_listed = true}
          end,
    update_endpoint(Addr, Fun, State).

connect_endpoint(Addr, State) ->
    update_endpoint(Addr, fun(EP) ->
                                  EP#ep{is_black_listed = false,
                                        nb_success = EP#ep.nb_success + 1,
                                        backoff_delay = ?INITIAL_BACKOFF}
                          end, State).

update_endpoint(Addr, Fun, State = #state{endpoints = EPs}) ->
    case orddict:find(Addr, EPs) of
        error ->
            EP2 = Fun(#ep{addr = Addr}),
            State#state{endpoints = orddict:store(Addr,EP2,EPs)};
        {ok, EP} ->
            EP2 = Fun(EP),
            State#state{endpoints = orddict:store(Addr,EP2,EPs)}
    end.

fail_request(Reason, #req{ref = Ref, spec = Spec},
             State = #state{pending = Pending}) ->
    %% Tell the module it failed
    {Proto, {_TcpOptions, Module,Args}} = Spec,
    Module:connect_failed(Proto, {error, Reason}, Args),
    %% Remove the request from the pending list
    State#state{pending = lists:keydelete(Ref, #req.ref, Pending)}.

update_endpoints(Addrs, Endpoints) ->
    %% add addr to Endpoints if not already there
    Fun = (fun(Addr, EPs) ->
                   case orddict:is_key(Addr, Endpoints) of
                       true -> EPs;
                       false ->
                           EP = #ep{addr=Addr},
                           ?debugFmt("Adding new endpoint: ~p", [EP]),
                           orddict:store(Addr, EP, EPs)
                   end
           end),
    lists:foldl(Fun, Endpoints, Addrs).

%% Return the addresses of non-blacklisted endpoints that are also
%% members of the list EpAddrs.
filter_blacklisted_endpoints(EpAddrs, AllEps) ->
    [EP#ep.addr || {AddrKey,EP} <- orddict:to_list(AllEps),
                   lists:member(AddrKey, EpAddrs),
                   EP#ep.is_black_listed == false].
