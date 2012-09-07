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
-define(INITIAL_BACKOFF, 10 * 1000). %% 10 second initial backoff
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
-record(ep, {name,     %% Endpoint name- {IP, Port}
             nb_curr_connections = 0 :: counter(), %% number of current connections
             nb_success = 0 :: counter(), %% total successfull connects on this ep
             nb_failed = 0 :: counter(),  %% total failed connects on this ep
             is_black_listed = false :: boolean(), %% true after a failed connection attempt
             backoff_delay=0 :: counter(), %% incremented on each failure, reset to zero on success
             failures = orddict:new() :: orddict:orddict(), %% failure reasons
             last_fail                   % time of last failure
             }).

%% connection request record
-record(req, {ref,      % Unique reference for this connection request
              pid,      % Helper pid trying to make connection
              target,   % target to connect to {Type, Name}
              spec,     % client spec
              strategy, % connection strategy
              cur,      % current connection endpoint
              next}).   % ordered list to try and connect to
              

%% connection manager state:
%% cluster_finder := function that returns the ip address
-record(state, {is_paused = false :: boolean(),
                %% peer_addrs :: clustername() -> [ip_addr()]
                peer_addrs = orddict:new() :: orddict:orddict(),
                cluster_finder = fun() -> {error, undefined} end :: cluster_finder_fun(),
                pending = [] :: [#req{}], % pending requests
                %% endpoints :: {module(),ip_addr()} -> ep()
                endpoints = [] :: [#ep{}], % endpoints
                locators = orddict:new() :: orddict:orddict(), %% connection locators
                nb_total_succeeded = 0 :: counter(),
                nb_total_failed = 0 :: counter()
               }).

-export([start_link/0,
         set_peers/2,
         get_peers/1,
         resume/0,
         pause/0,
         is_paused/0,
         set_cluster_finder/1,
         get_cluster_finder/0,
         connect/2,
         register_locator/2
         ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% internal functions
%% -export([connection_helper/4, increase_backoff/1, do_connect/4]).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() -> {ok, pid()}).
start_link() ->
    Args = [],
    Options = [],
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, Options).

%% set a list of peer node IP/Port addresses of the remote cluster.
set_peers(ClusterName, PeerNodeAddrs) ->
    gen_server:cast(?SERVER, {set_peers, ClusterName, PeerNodeAddrs}).

get_peers(ClusterName) ->
    gen_server:call(?SERVER, {get_peers, ClusterName}).

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

%% Establish a connection to the remote destination. be persistent about it,
%% but not too annoying to the remote end. Connect by name of cluster or
%% IP address.
-spec(connect({name,clustername()} | {addr,ip_addr()}, clientspec()) -> ok).
connect(Dest, ClientSpec) ->
    gen_server:call(?SERVER, {connect, Dest, ClientSpec, default}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{is_paused = true}}.

handle_call({get_peers, ClusterName}, _From, State) ->
    PeerNodeAddrs = case orddict:find(ClusterName, State#state.peer_addrs) of
                        {ok, Val} ->
                            Val;
                        error ->
                            []
                    end,
    {reply, PeerNodeAddrs, State};

handle_call(is_paused, _From, State) ->
    {reply, State#state.is_paused, State};

handle_call(get_cluster_finder, _From, State) ->
    {reply, State#state.cluster_finder, State};

%% connect based on address. Return process id of helper
handle_call({connect, Target, ClientSpec, Strategy}, _From,
            State = #state{pending = Pending}) ->
    case locate_endpoints(Target, Strategy, State) of
        {ok, Endpoints} ->
            Ref = make_ref(),
            Req = #req{ref = Ref, 
                       target = Target,
                       next = Endpoints,
                       spec = ClientSpec,
                       strategy = Strategy},
            State2 = State#state{pending = [Req | Pending]},
            {reply, {ok, Ref}, try_next_endpoint(Req, State2)};
        ER ->
            {reply, ER, State}
    end;
handle_call({register_locator, Type, Fun}, _From,
            State = #state{locators = Locators}) ->
    {reply, ok, State#state{locators = orddict:store(Type, Fun, Locators)}};
handle_call(_Unhandled, _From, State) ->
    ?TRACE(?debugFmt("Unhandled gen_server call: ~p", [_Unhandled])),
    {reply, {error, unhandled}, State}.




handle_cast({set_peers, ClusterName, PeerNodeAddrs}, State) ->
    OldDict = State#state.peer_addrs,
    NewDict = orddict:store(ClusterName, PeerNodeAddrs, OldDict),
    {noreply, State#state{peer_addrs = NewDict}};

handle_cast(pause, State) ->
    {noreply, State#state{is_paused = true}};

handle_cast(resume, State) ->
    {noreply, State#state{is_paused = false}};

handle_cast({set_cluster_finder, FinderFun}, State) ->
    {noreply, State#state{cluster_finder=FinderFun}};

%% %% helper process is telling us that it failed to reach an
%% %% address. That process will continue until it exhausts all
%% %% known endpoints (as of when it started). Here, we just need
%% %% to take note of the failed connection attempt and update
%% %% our book keeping for that endpoint. Black-list it, and
%% %% adjust a backoff timer so that we wait a while before
%% %% trying this endpoint again.
%% %%
%% handle_cast({endpoint_failed, Addr, _Protocol}, State) ->
%%     case orddict:find(Addr, State#state.endpoints) of
%%         {ok, EP} ->
%%             %% mark connection as black-listed and start timer for reset
%%             EP = orddict:fetch(Addr, State#state.endpoints),
%%             Backoff = increase_backoff(EP#ep.backoff_delay),
%%             NewEP = EP#ep{is_black_listed = true,
%%                           nb_failed = EP#ep.nb_failed + 1,
%%                           backoff_delay = Backoff},
%%             NewEPS = orddict:store(Addr, NewEP, State#state.endpoints),
%%             %% schedule a message to un-blacklist this endpoint
%%             erlang:send_after(Backoff, self(), {backoff_timer, Addr}),
%%             {noreply, State#state{endpoints = NewEPS}};
%%         error ->
%%             lager:error("Endpoint: ~p, missing from known endpoints.", [Addr]),
%%             {noreply, State}
%%     end;

%% handle_cast({endpoint_connected, Addr, _Protocol}, State) ->
%%     EP = orddict:fetch(Addr, State#state.endpoints),
%%     NewEP = EP#ep{is_black_listed = false,
%%                   nb_success = EP#ep.nb_success + 1,
%%                   backoff_delay = ?INITIAL_BACKOFF},
%%     NewEPS = orddict:store(Addr, NewEP, State#state.endpoints),
%%     {noreply, State#state{endpoints = NewEPS}};

%% %% message from the helper process. It ran out of endpoints to try.
%% %% it terminated. start a new one.
%% handle_cast({endpoints_exhausted, From, Dest, Protocol}, State) ->
%%     %% remove the exhausted helper process pid from our list of pending connections.
%%     Pids = sets:del_element(From, State#state.helper_pids),
%%     %% start a new connection helper process
%%     {_Pid, NewState} = do_connect(Dest, Protocol, default, State#state{helper_pids=Pids}),
%%     {noreply, NewState};

handle_cast(_Unhandled, _State) ->
    ?TRACE(?debugFmt("Unhandled gen_server cast: ~p", [_Unhandled])),
    {error, unhandled}. %% this will crash the server

%% it is time to remove Addr from the black-listed addresses

handle_info({unblacklist, Name}, State = #state{endpoints = EPs}) ->
    case lists:keytake(Name, #ep.name, EPs) of
        {value, EP, EPs2} ->
            {noreply, State#state{endpoints = [EP#ep{is_black_listed = false} | EPs2]}};
        false ->
            %% TODO: should never happen
            {norepy, State}
    end;
handle_info({retry_req, Ref}, State = #state{pending = Pending}) ->
    case lists:keyfind(Ref, #req.ref, Pending) of
        false ->
            {noreply, State#state{pending = lists:keytake(Ref, #req.ref, Pending)}};
        Req ->
            {noreply, try_next_endpoint(Req, State)}
    end;
    
handle_info({'EXIT', From, Reason}, State = #state{pending = Pending}) ->
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
                    {noreply, try_next_endpoint(Req, State2)}
            end
    end;
handle_info({backoff_timer, Addr}, State) ->
    EP = orddict:fetch(Addr, State#state.endpoints),
    NewEP = EP#ep{is_black_listed = false},
    NewEPS = orddict:store(Addr, NewEP, State#state.endpoints),
    {noreply, State#state{endpoints = NewEPS}};

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

%% %% connect by address, no retries
%% do_connect({addr,_Addr}=Dest, Protocol, Strategy, State) ->
%%     Pid = spawn_link(?MODULE, connection_helper, [Dest, Protocol, Strategy, []]),
%%     {Pid,State};
%% do_connect({name,ClusterName}=Dest, Protocol, Strategy, State) ->
%%     EndPoints = least_connected_eps(ClusterName, State),
%%     Pid = spawn_link(?MODULE, connection_helper, [Dest, Protocol, Strategy, EndPoints]),
%%     %% add this Pid to our helper processes
%%     Pids = sets:add_element(Pid, State#state.helper_pids),
%%     {Pid,State#state{helper_pids=Pids}}.

%% increase the backoff delay, but cap at a maximum
increase_backoff(0) ->
    100;
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

%% %% a spawned process that will try and connect to a remote address
%% %% or named cluster, with retries on failure to connect.
%% %%
%% connection_helper({addr, Addr}, Protocol, _Strategy, _EPs) ->
%%     riak_core_connection:sync_connect(Addr, Protocol);
%% connection_helper(Dest, Protocol, default, []) ->
%%     %% bummer. no end points available yet. There is nowhere else to delay,
%%     %% so sleep here a little before notifying connection manager we need
%%     %% to retry this request. Process terminates normally. A new one will
%%     %% be restarted by the connection manager.
%%     timer:sleep(1000),
%%     gen_server:cast(?SERVER, {endpoints_exhausted, self(), Dest, Protocol});
%% connection_helper(Dest, Protocol, default, [{{_T,Addr},_EP}|EE]) ->
%%     case riak_core_connection:sync_connect(Addr, Protocol) of
%%         ok ->
%%             %% notify connection manager of success
%%             gen_server:cast(?SERVER, {endpoint_connected, Addr, Protocol});
%%         {error, _Reason} ->
%%             %% TODO: log Reason of failure? or maybe do it in endoint_failed?
%%             %% notify connection manager this EP failed
%%             gen_server:cast(?SERVER, {endpoint_failed, Addr, Protocol}),
%%             connection_helper(Dest, Protocol, default, EE)
%%     end.


%% Try the next endpoint for the request,filtering by blacklisted 
%% connections.  If all endpoints used up, call the location service
%% again.  If no endpoints available, schedule rechecking the connection.
try_next_endpoint(Req = #req{ref = Ref, target = Target, spec = Spec,
                             strategy = Strategy, next = Next},
                  State = #state{pending = Pending, endpoints = Endpoints}) ->
    case next_available_endpoints(Next, Endpoints) of
        [] ->
            case locate_endpoints(Target, Strategy, State) of
                {ok, []} ->
                    Interval = app_helper:get_env(riak_core, connmgr_no_endpoint_retry, 5000),
                    erlang:send_after(Interval, self(), {retry_req, Ref}),
                    State#state{pending = lists:keystore(Ref, #req.ref, Pending,
                                                         Req#req{pid = undefined,
                                                                 cur = awaiting_retry,
                                                                 next = []})};
                {ok, Next2} ->
                    try_next_endpoint(Req#req{cur = undefined, next = Next2}, State);
                {error, Reason} ->
                    fail_request(Reason, Req, State)
            end;
        [Cur | Next2] ->
            Pid = spawn_link(
                    fun() ->
                            exit(try
                                     riak_core_connection:sync_connect(Cur, Spec)
                                 catch
                                     T:R ->
                                         {exception, {T, R}}
                                 end)
                    end),
            State#state{pending = lists:keystore(Ref, #req.ref, Pending,
                                                 Req#req{pid = Pid, 
                                                         cur = Cur,
                                                         next = Next2})}
    end.

%% Return the next non-blacklisted endpoint
next_available_endpoints([], _Endpoints) ->
    [];
next_available_endpoints([Cur|Rest]=Next, Endpoints) ->
    case lists:keyfind(Cur, #ep.name, Endpoints) of
        false -> % never seen it before, give it a try
            Next;
        #ep{is_black_listed = BL} when BL == false ->
            Next;
        _ -> % blacklisted, try the next one
            next_available_endpoints(Rest, Endpoints)
    end.

locate_endpoints({Type, Name}, Strategy, #state{locators = Locators,
                                                endpoints = Endpoints}) ->
    case orddict:find(Type, Locators) of
        {ok, Locate} ->
            case Locate(Name, Strategy) of
                {ok, Next} ->
                    {ok, next_available_endpoints(Next, Endpoints)};
                ER ->
                    ER
            end;
        error ->
            {error, {unknown_target_type, Type}}
    end.

fail_endpoint(Name, Reason, State) ->
    Fun = fun(EP = #ep{backoff_delay = Backoff, failures = Failures}) ->
                  Failures2 = orddict:update_counter(Reason, 1, Failures),
                  Backoff2 = increase_backoff(Backoff),
                  case Backoff of
                      0 ->
                          %% First failure, just note and increase backoff
                          EP#ep{failures = Failures2, backoff_delay = Backoff2,
                                last_fail = os:timestamp()};
                      _ ->
                          %% Subsequent failures, make endpoint unavailable
                          erlang:send_after(Backoff, self(), {unblacklist, Name}),
                          EP#ep{failures = Failures2, backoff_delay = Backoff2,
                                last_fail = os:timestamp(),
                                is_black_listed = true}
                  end
          end,
    update_endpoint(Name, Fun, State).

connect_endpoint(Name, State) ->
    update_endpoint(Name, fun(EP) ->
                                  EP#ep{is_black_listed = false,
                                        backoff_delay = 0}
                          end, State).

update_endpoint(Name, Fun, State = #state{endpoints = EPs}) ->
    case lists:keytake(Name, #ep.name, EPs) of
        false ->
            EP2 = Fun(#ep{name = Name}),
            State#state{endpoints = [EP2 | EPs]};
        {value, EP, EPs2} ->
            EP2 = Fun(EP),
            State#state{endpoints = [EP2 | EPs2]}
    end.

fail_request(Reason, #req{ref = Ref, spec = Spec},
             State = #state{pending = Pending}) ->
    %% Tell the module it failed
    {Proto, {_TcpOptions, Module,Args}} = Spec,
    Module:connect_failed(Proto, {error, Reason}, Args),
    %% Remove the request from the pending list
    State#state{pending = lists:keydelete(Ref, #req.ref, Pending)}.

