%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_rtq_proxy).

%% @doc A proxy process that handles realtime messages received while and
%% after the riak_repl application has shut down. This allows us to avoid
%% dropping realtime messages around shutdown events.

-behaviour(gen_server).

%% API
-export([start/0, start_link/0, push/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {nodes=[],          %% peer replication nodes
                versions=[]}).     %% {node(), wire-version()}

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    %% start under the kernel_safe_sup supervisor so we can block node
    %% shutdown if we need to do process any outstanding work
    LogSup = {?MODULE, {?MODULE, start_link, []}, permanent,
              5000, worker, [?MODULE]},
    supervisor:start_child(kernel_safe_sup, LogSup).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

push(NumItems, Bin) ->
    gen_server:cast(?MODULE, {push, NumItems, Bin}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    %% trap exit so we can have terminate() called
    process_flag(trap_exit, true),
    Nodes = riak_repl_util:get_peer_repl_nodes(),
    [erlang:monitor(process, {riak_repl2_rtq, Node}) || Node <- Nodes],
    %% cache the supported wire format of peer nodes to avoid rcp calls later.
    Versions = get_peer_wire_versions(Nodes),
    {ok, #state{nodes=Nodes, versions=Versions}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({push, NumItems, _Bin}, State = #state{nodes=[]}) ->
    lager:warning("No available nodes to proxy ~p objects to~n", [NumItems]),
    catch(riak_repl_stats:rt_source_errors()),
    {noreply, State};
handle_cast({push, NumItems, W1BinObjs}, State) ->
    %% push items to another node for queueing. If the other node does not speak binary
    %% object format, then downconvert the items (if needed) before pushing.
    Node = hd(State#state.nodes),
    Nodes = tl(State#state.nodes),
    PeerWireVer = wire_version_of_node(Node, State#state.versions),
    lager:debug("Proxying ~p items to ~p with wire version ~p", [NumItems, Node, PeerWireVer]),
    BinObjs = riak_repl_util:maybe_downconvert_binary_objs(W1BinObjs, PeerWireVer),
    gen_server:cast({riak_repl2_rtq, Node}, {push, NumItems, BinObjs}),
    {noreply, State#state{nodes=Nodes ++ [Node]}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, _, {riak_repl2_rtq, Node}, _}, State) ->
    lager:info("rtq proxy target ~p is down", [Node]),
    {noreply, State#state{nodes=State#state.nodes -- [Node]}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    case State#state.nodes of
        [] ->
            ok;
        _ ->
            %% relay as much as we can, blocking shutdown
            flush_pending_pushes(State)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

flush_pending_pushes(State) ->
    receive
        {'$gen_cast', Msg} ->
            {noreply, NewState} = handle_cast(Msg, State),
            flush_pending_pushes(NewState)
    after
        100 ->
            ok
    end.

%% return tuple of node and it's supported wire version
get_peer_wire_versions(Nodes) ->
    [ begin
          WireVer = riak_repl_util:peer_wire_format(Node),
          {Node, WireVer}
      end || Node <- Nodes].

wire_version_of_node(Node, Versions) ->
    case lists:keyfind(Node, 1, Versions) of
        false ->
            w0;
        {_Node, Ver} ->
            Ver
    end.
    
