%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_listener).
-author('Andy Gross <andy@basho.com>').
-behavior(gen_nb_server).
-include("riak_repl.hrl").
-export([start_link/2]).
-export([close_all_connections/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([sock_opts/0, new_connection/2, stop/1, config/1]).
-record(state, {
          ipaddr :: string(), 
          portnum :: non_neg_integer()
         }).

start_link(IPAddr, PortNum) ->
    gen_nb_server:start_link(?MODULE, IPAddr, PortNum, [IPAddr, PortNum]).

config(Pid) ->
    gen_server:call(Pid, config, infinity).

init([IPAddr, PortNum]) -> 
    {ok, #state{ipaddr=IPAddr, portnum=PortNum}}.

sock_opts() -> [binary, 
                {keepalive, true},
                {nodelay, true},
                {packet, 4},
                {reuseaddr, true},
                {active, false},
                {backlog, 64}].

new_connection(Socket, State) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, SiteName} ->
            SiteNameBin = binary_to_list(SiteName),
            case riak_repl_server_sup:start_server(SiteNameBin) of
                {ok, Pid} ->
                    connection_made(Socket, Pid, State);
                {error, Reason} ->
                    gen_tcp:close(Socket),
                    connection_error(Reason, SiteNameBin, State)
            end;
        {error, Reason} ->
            connection_error(Reason, "unknown", State)
    end.

stop(Pid) when is_pid(Pid) ->
    gen_server:cast(Pid, stop).

close_all_connections() ->
    [exit(P, kill) || {_, P, _, _} <- supervisor:which_children(riak_repl_server_sup)].

handle_call(config, _From, State) ->
    {reply, #repl_listener{nodename=node(),
            listen_addr={State#state.ipaddr, State#state.portnum}}, State};
handle_call(_Req, _From, State) -> {reply, ok, State}.

handle_cast(stop, State) ->
    lager:info("Replication listener for ~s:~p shutting down",
        [State#state.ipaddr, State#state.portnum]),
    {stop, normal, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% helper functions

connection_made(Socket, Pid, State) ->
    lager:debug("Replication client connected"),
    gen_tcp:controlling_process(Socket, Pid),
    riak_repl_tcp_server:set_socket(Pid, Socket),
    riak_repl_stats:server_connects(),
    {ok, State}.

connection_error({Reason, Backtrace}, SiteName, State) ->
    %% use debug severity to prevent DOS issues
    lager:debug("Error accepting connection from site: ~p:~p",
              [SiteName, {Reason, Backtrace}]),
    riak_repl_stats:server_connect_errors(),
    {ok, State};
connection_error(Reason, SiteName, State) ->
    %% use debug severity to prevent DOS issues
    lager:debug("Error accepting connection from site: ~p:~p",
              [SiteName, Reason]),
    riak_repl_stats:server_connect_errors(),
    {ok, State}.


