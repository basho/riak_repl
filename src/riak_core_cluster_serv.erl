-module(riak_core_cluster_serv).
-behavior(gen_server).

-include("riak_core_cluster.hrl").
-include("riak_core_connection.hrl").

-record(state, {
    transport, socket, local_ver, remote_ver, remote_addr, remote_name
}).

% external api
-export([ start_link/5 ]).
% gen_server
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).

%% ==========
%% public api
%% ==========

start_link(_Socket, _Transport, {error, _} = Error, _Args, _Props) ->
    Error;

start_link(Socket, Transport, NegotiatedVers, _Args, Props) ->
    {ok, ClientAddr} = Transport:peername(Socket),
    RemoteClusterName = proplists:get_value(clustername, Props),
    lager:debug("Cluster Manager: accepted connection from cluster at ~p named ~p"),
    case gen_server:start_link(?MODULE, {Socket, Transport, NegotiatedVers, ClientAddr, RemoteClusterName, Props}, []) of
        {ok, Pid} ->
            Transport:controlling_process(Socket, Pid),
            ok = gen_server:cast(Pid, control_given),
            {ok, Pid};
        Else ->
            Else
    end.

%% ==========
%% init
%% ==========

init({Socket, Transport, NegotiatedVers, ClientAddr, RemoteClusterName, _Props}) ->
    {ok, {cluster_mgr, LocalVer, RemoteVer}} = NegotiatedVers,
    State = #state{socket = Socket, transport = Transport, remote_addr = ClientAddr, remote_name = RemoteClusterName, local_ver = LocalVer, remote_ver = RemoteVer},
    {ok, State}.

%% ==========
%% handle_call
%% ==========

handle_call(_Req, _From, State) ->
    {reply, {error, invalid}, State}.

%% ==========
%% handle_cast
%% ==========

handle_cast(control_given, State) ->
    #state{transport = Transport, socket = Socket} = State,
    ok = Transport:setops(Socket, [{active, once}]),
    {noreply, State};

handle_cast(_Req, State) ->
    {noreply, State}.

%% ==========
%% handle_info
%% ==========

handle_info({_TransTag, Socket, Data}, State = #state{socket = Socket}) when is_binary(Data) ->
    Termed = binary_to_term(Data),
    handle_socket_info(Termed, State#state.transport, State#state.socket, State);

handle_info({TransClosed, Socket}, State = #state{socket = Socket}) when is_atom(TransClosed) ->
    {stop, {error, closed}, State};

handle_info({_TransTag, Socket, Error}, State = #state{socket = Socket}) ->
    {stop, {error, {connection_error, Error}}, State}.

%% ==========
%% terminate
%% ==========

terminate(_Why, _State) ->
    ok.

%% ==========
%% code_change
%% ==========

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ==========
%% Internal
%% ==========

handle_socket_info({ok, ?CTRL_ASK_NAME}, Transport, Socket, State) ->
    LocalName = riak_core_connection:symbolic_clustername(),
    ok = Transport:send(Socket, term_to_binary(LocalName)),
    ok = Transport:setopts([{active, once}]),
    {noreply, State};

handle_socket_info({ok, ?CTRL_ASK_MEMBERS}, Transport, Socket, State) ->
    case read_ip_address(Socket, Transport, State#state.remote_addr) of
        {ok, RemoteConnectedToIp} ->
            Members = gen_server:call(?CLUSTER_MANAGER_SERVER, {get_my_members, RemoteConnectedToIp}, infinity),
            ok = Transport:send(Socket, term_to_binary(Members)),
            {noreply, State};
        Else ->
            {stop, Else, State}
    end.

read_ip_address(Socket, Transport, Remote) ->
    case Transport:recv(Socket, 0, ?CONNECTION_SETUP_TIMEOUT) of
        {ok, BinAddr} ->
            MyAddr = binary_to_term(BinAddr),
            {ok, MyAddr};
        {error, closed} ->
            {error, closed};
        Error ->
            lager:error("Cluster Manager: failed to receive ip addr from remote ~p: ~p",
                        [Remote, Error]),
            Error
    end.

