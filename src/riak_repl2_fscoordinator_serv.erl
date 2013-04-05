%% @doc Service which replies to requests for the IP:Port of the node where
%% a given partition lives. Responsible for determining if the node for a 
%% partition is available for use as the sink. Reservations and actual running
%% sinks are used to determine availability. Once a reservation is issued, it
%% is up to the keylist_client to claim it.

-module(riak_repl2_fscoordinator_serv).
-include("riak_repl.hrl").
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(state, {
    transport,
    socket,
    proto
}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/4, status/0, status/1, status/2]).

%% ------------------------------------------------------------------
%% service manager callback Function Exports
%% ------------------------------------------------------------------

-export([register_service/0, start_service/5]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

%% @doc Given the communication channel data, start up a serv. There is one
%% serv per remote cluster being fullsync'ed from.
-spec start_link(any(), any(), any(), any()) -> {'ok', pid()}.
start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, {Socket, Transport,
            Proto, Props}, []).

%% @doc Get the stats for every serv.
%% @see status/1
-spec status() -> [tuple()].
status() ->
    LeaderNode = riak_repl2_leader:leader_node(),
    case LeaderNode of
        undefined ->
            {[], []};
        _ ->
            case riak_repl2_fscoordinator_serv_sup:started() of
                [] ->
                    [];
                Repls ->
                    [status(Pid) || {_Remote, Pid} <- Repls]
            end
    end.

%% @doc Get the status for the given serv.
-spec status(Pid :: pid()) -> [tuple()].
status(Pid) ->
    status(Pid, infinity).

%% @doc Get the status for the given serv giving up after the timeout; or never
%% give up if the timeout is `infinity'.
-spec status(Pid :: pid(), Timeout :: timeout()) -> [tuple()].
status(Pid, Timeout) ->
    gen_server:call(Pid, status, Timeout).


%% ------------------------------------------------------------------
%% service manager Function Definitions
%% ------------------------------------------------------------------

register_service() ->
    ProtoPrefs = {fs_coordinate, [{1,0}]},
    TcpOptions = [{keepalive, true}, {packet, 4}, {active, false}, 
        {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:register_service(HostSpec, {round_robin, undefined}).

start_service(Socket, Transport, Proto, _Args, Props) ->
    {ok, Pid} = riak_repl2_fscoordinator_serv_sup:start_child(Socket,
        Transport, Proto, Props),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

%% @hidden
init({Socket, Transport, Proto, _Props}) ->
    Max = app_helper:get_env(riak_repl, max_fssink_node, ?DEFAULT_MAX_SINKS_NODE),
    lager:info("Starting fullsync coordinator server (sink) with max_fssink_node=~p", [Max]),
    SocketTag = riak_repl_util:generate_socket_tag("fs_coord_srv", Transport, Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, coordsrv,
                                       SocketTag}, Transport),
    {ok, #state{socket = Socket, transport = Transport, proto = Proto}}.


%% @hidden
handle_call(status, _From, State = #state{socket=Socket, transport = Transport}) ->
    SocketStats = riak_core_tcp_mon:format_socket_stats(
            riak_core_tcp_mon:socket_status(Socket), []),
    {ok, PeerData} = Transport:peername(Socket),
    PeerData2 = peername_to_string(PeerData),
    SelfStats = [
        {socket, SocketStats}
    ],
    {reply, {PeerData2, SelfStats}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


%% @hidden
handle_cast(Msg, State) ->
    lager:info("Unexpected message ~p", [Msg]),
    {noreply, State}.


%% @hidden
handle_info({Closed, Socket}, #state{socket = Socket} = State) when
    Closed =:= tcp_closed; Closed =:= ssl_closed ->
    lager:info("Connect closed"),
    {stop, normal, State};

handle_info({Erred, Socket, _Reason}, #state{socket = Socket} = State) when
    Erred =:= tcp_error; Erred =:= ssl_error ->
    lager:error("Connection closed unexpectedly"),
    {stop, normal, State};

handle_info({Proto, Socket, Data}, #state{socket = Socket,
    transport = Transport} = State) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    State2 = handle_protocol_msg(Msg, State),
    {noreply, State2};

handle_info(init_ack, #state{socket=Socket, transport=Transport} = State) ->
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.


%% @hidden
terminate(_Reason, _State) ->
    ok.


%% @hidden
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_protocol_msg({whereis, Partition, ConnIP, _ConnPort}, State) ->
    % which node is the partition for
    % is that node available
    % send an appropriate reply
    #state{transport = Transport, socket = Socket} = State,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Node = get_partition_node(Partition, Ring),
    Map = riak_repl_ring:get_nat_map(Ring),
    {ok, NormIP} = riak_repl_util:normalize_ip(ConnIP),
    %% apply the NAT map
    RealIP = riak_repl2_ip:maybe_apply_nat_map(NormIP, ConnPort, Map),
    Reply = case riak_repl2_fs_node_reserver:reserve(Partition) of
        ok ->
            case get_node_ip_port(Node, RealIP) of
                {ok, {ListenIP, Port}} ->
                    %% TODO need to apply the reversed nat-map, now
                    {location, Partition, {Node, ListenIP, Port}};
                {error, _} ->
                    riak_repl2_fs_node_reserver:unreserve(Partition),
                    {location_down, Partition, Node}
            end;
        busy ->
            lager:debug("node_reserver returned location_busy for partition ~p on node ~p", [Partition, Node]),
            {location_busy, Partition, Node};
        down ->
            {location_down, Partition, Node}
    end,
    Transport:send(Socket, term_to_binary(Reply)),
    State;

handle_protocol_msg({unreserve, Partition}, State) ->
    riak_repl2_fs_node_reserver:unreserve(Partition),
    State.

get_partition_node(Partition, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    proplists:get_value(Partition, Owners).

get_node_ip_port(Node, NormIP) ->
    {ok, {_IP, Port}} = rpc:call(Node, application, get_env, [riak_core, cluster_mgr]),
    {ok, IfAddrs} = inet:getifaddrs(),
    Subnet = riak_repl2_ip:determine_netmask(IfAddrs, NormIP),
    Masked = riak_repl2_ip:mask_address(NormIP, Subnet),
    case get_matching_address(Node, NormIP, Masked) of
        {ok, {ListenIP, _}} ->
            {ok, {ListenIP, Port}};
        Else ->
            Else
    end.

get_matching_address(Node, NormIP, Masked) when Node =:= node() ->
    Res = riak_repl2_ip:get_matching_address(NormIP, Masked),
    {ok, Res};

get_matching_address(Node, NormIP, Masked) ->
    case rpc:call(Node, riak_repl2_ip, get_matching_address, [NormIP, Masked]) of
        % this is a clause that will be removed/useless once all nodes on a 
        % cluster are >= 1.3
        {badrpc, {'EXIT', {undef, _StackTrace}}} ->
            case rpc:call(Node, riak_repl_app, get_matching_address, [NormIP, Masked]) of
                {badrpc, Err} ->
                    {error, Err};
                OkRes ->
                    {ok, OkRes}
            end;
        {badrpc, Err} ->
            {error, Err};
        Res ->
            {ok, Res}
    end.

peername_to_string({{A,B,C,D},Port}) ->
    lists:flatten(io_lib:format("~B.~B.~B.~B:~B", [A,B,C,D,Port])).
