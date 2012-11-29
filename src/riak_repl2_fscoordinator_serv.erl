%% @doc Service which replies to requests for the IP:Port of the node where
%% a given partition lives.

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

start_link(Socket, Transport, Proto, Props) ->
    gen_server:start_link(?MODULE, {Socket, Transport,
            Proto, Props}, []).

status() ->
    LeaderNode = riak_repl2_leader:leader_node(),
    case LeaderNode of
        undefined ->
            {[], []};
        _ ->
            case riak_repl2_fscoordinator_serv_sup:started(LeaderNode) of
                [] ->
                    [];
                Repls ->
                    [status(Pid) || {_Remote, Pid} <- Repls]
            end
    end.

status(Pid) ->
    status(Pid, infinity).

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

init({Socket, Transport, Proto, _Props}) ->
    SocketTag = riak_repl_util:generate_socket_tag("fs_coord_srv", Socket),
    lager:debug("Keeping stats for " ++ SocketTag),
    riak_core_tcp_mon:monitor(Socket, {?TCP_MON_FULLSYNC_APP, coordsrv,
                                       SocketTag}, Transport),
    {ok, #state{socket = Socket, transport = Transport, proto = Proto}}.

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

handle_cast(_Msg, State) ->
    {noreply, State}.

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

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_protocol_msg({whereis, Partition, ConnIP, _ConnPort}, State) ->
    % which node is the partition for
    % is that node available
    % send an appropriate reply
    Node = get_partition_node(Partition),
    Reply = case is_node_available(Node) of
        true ->
            case get_node_ip_port(Node, ConnIP) of
                {ok, {ListenIP, Port}} ->
                    {location, Partition, {Node, ListenIP, Port}};
                {error, _} ->
                    {location_down, Partition}
            end;
        false ->
            {location_busy, Partition}
    end,
    #state{socket = Socket, transport = Transport} = State,
    Transport:send(Socket, term_to_binary(Reply)),
    State.

get_partition_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    proplists:get_value(Partition, Owners).

is_node_available(Node) ->
    Kids = supervisor:which_children({riak_repl2_fssink_sup, Node}),
    Max = app_helper:get_env(riak_repl, max_fssink_node, ?DEFAULT_MAX_SINKS_NODE),
    length(Kids) < Max.

get_node_ip_port(Node, ConnIP) ->
    {ok, {_IP, Port}} = application:get_env(riak_core, cluster_mgr),
    {ok, IfAddrs} = inet:getifaddrs(),
    {ok, NormIP} = riak_repl_util:normalize_ip(ConnIP),
    Subnet = riak_repl_app:determine_netmask(IfAddrs, NormIP),
    Masked = riak_repl_app:mask_address(NormIP, Subnet),
    case get_matching_address(Node, NormIP, Masked) of
        {ok, {ListenIP, _}} ->
            {ok, {ListenIP, Port}};
        Else ->
            Else
    end.

get_matching_address(Node, NormIP, Masked) when Node =:= node() ->
    Res = riak_repl_app:get_matching_address(NormIP, Masked),
    {ok, Res};

get_matching_address(Node, NormIP, Masked) ->
    case rpc:call(Node, riak_repl_app, get_matching_address, [NormIP, Masked]) of
        {badrpc, Err} ->
            {error, Err};
        Res ->
            {ok, Res}
    end.

peername_to_string({{A,B,C,D},Port}) ->
    lists:flatten(io_lib:format("~B.~B.~B.~B:~B", [A,B,C,D,Port])).
