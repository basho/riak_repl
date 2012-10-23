%% @doc Service which replies to requests for the IP:Port of the node where
%% a given partition lives.

-module(riak_repl2_fscoordinator_serv).
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

-export([start_link/3]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Socket, Transport, Proto) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, {Socket, Transport, Proto}, []).

%% ------------------------------------------------------------------
%% service manager Function Definitions
%% ------------------------------------------------------------------

register_service() ->
    ProtoPrefs = {fs_coordinate, [{1,0}]},
    TcpOptions = [{keepalive, true}, {packet, 4}, {active, false}, 
        {nodelay, true}],
    HostSpec = {ProtoPrefs, {TcpOptions, ?MODULE, start_service, undefined}},
    riak_core_service_mgr:register_service(HostSpec, {round_robin, undefined}).

start_service(Socket, Transport, Proto, _Args) ->
    {ok, Pid} = riak_repl2_fscoordinator_serv_sup:start_child(Socket, Transport, Proto),
    ok = Transport:controlling_process(Socket, Pid),
    Pid ! init_ack,
    {ok, Pid}.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init({Socket, Transport, Proto}) ->
    {ok, #state{socket = Socket, transport = Transport, proto = Proto}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({Closed, Socket}, #state{socket = Socket} = State) when
    Closed =:= tcp_closed; Closed =:= ssl_closed ->
    lager:info("Connect closed"),
    {stop, normal, State};

handle_info({Erred, Socket, Reason}, #state{socket = Socket} = State) when
    Erred =:= tcp_error; Erred =:= ssl_error ->
    lager:error("Connection closed unexpectedly"),
    {stop, normal, State};

handle_info({Proto, Socket, Data}, #state{socket = Socket,
    transport = Transport} = State) when Proto==tcp; Proto==ssl ->
    Transport:setopts(Socket, [{active, once}]),
    Msg = binary_to_term(Data),
    State2 = handle_protocol_msg(Msg, State),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

handle_protocol_msg({whereis, Partition}, State) ->
    #state{transport = Transport, socket = Socket} = State,
    Ring = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    Node = proplists:get_value(Partition, Owners),
    {IP, Port} = case node() of
        Node ->
            {ok, ServiceAddr} = application:get_env(riak_core, cluster_mgr),            ServiceAddr;
        _ ->
            gen_server:call({?MODULE, Node}, get_service_addr)
    end,
    % TODO And if the IP is a hostname, like "localhost"?
    Outbound = {location, Partition, {Node, IP, Port}},
    riak_repl_tcp_server:send(Transport, Socket, Outbound),
    State.
