-module(riak_repl2_rt_spanning).

% api
-export([start_link/0, stop/0]).
-export([clusters/0]).
-export([replications/0, add_replication/2, drop_replication/2, drop_cluster/1]).

% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

% api
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

clusters() ->
    ordsets:from_list(gen_server:call(?MODULE, clusters)).

drop_cluster(ClusterName) ->
    gen_server:cast(?MODULE, {drop_cluster, ClusterName}).

replications() ->
    Replications = gen_server:call(?MODULE, replications),
    Repls = lists:map(fun({Source, Sinks}) ->
        Sinks2 = ordsets:from_list(Sinks),
        {Source, Sinks2}
    end, Replications),
    orddict:from_list(Repls).

add_replication(Source, Sink) ->
    gen_server:cast(?MODULE, {add_replication, Source, Sink}).

drop_replication(Source, Sink) ->
    gen_server:cast(?MODULE, {drop_replication, Source, Sink}).

% gen_server
init(_) ->
    {ok, digraph:new()}.

handle_call(clusters, _From, Graph) ->
    {reply, digraph:vertices(Graph), Graph};

handle_call(replications, _From, Graph) ->
    Vertices = digraph:vertices(Graph),
    Out = lists:foldl(fun(Vertex, Acc) ->
        Neighbors = digraph:out_neighbours(Graph, Vertex),
        [{Vertex, Neighbors} | Acc]
    end, [], Vertices),
    {reply, Out, Graph};

handle_call(_Msg, _From, Graph) ->
    {reply, {error, nyi}, Graph}.

handle_cast(stop, Graph) ->
    {stop, normal, Graph};

handle_cast({drop_cluster, ClusterName}, Graph) ->
    digraph:del_vertex(Graph, ClusterName),
    {noreply, Graph};

handle_cast({add_replication, Source, Sink}, Graph) ->
    Sinks = digraph:out_neighbours(Graph, Source),
    case lists:member(Sink, Sinks) of
        true ->
            ok;
        false ->
            add_edges_with_vertices(Graph, Source, Sink)
    end,
    {noreply, Graph};

handle_cast({drop_replication, Source, Sink}, Graph) ->
    OutEdges = digraph:out_edges(Graph, Source),
    lists:map(fun(Edge) ->
        case digraph:edge(Graph, Edge) of
            {Edge, Source, Sink, _Label} ->
                digraph:del_edge(Graph, Edge);
            _ ->
                ok
        end
    end, OutEdges),
    {noreply, Graph};

handle_cast(_Msg, Graph) ->
    {noreply, Graph}.

handle_info(_Msg, Graph) ->
    {noreply, Graph}.

terminate(_Why, _Graph) ->
    ok.

code_change(_Vsn, Graph, _Extra) ->
    {ok, Graph}.

%% internal

add_edges_with_vertices(Graph, Source, Sink) ->
    add_edges_with_vertices(Graph, Source, Sink, digraph:add_edge(Graph, Source, Sink)).

add_edges_with_vertices(Graph, Source, Sink, {error, {bad_vertex, Source}}) ->
    digraph:add_vertex(Graph, Source),
    add_edges_with_vertices(Graph, Source, Sink, digraph:add_edge(Graph, Source, Sink));

add_edges_with_vertices(Graph, Source, Sink, {error, {bad_vertex, Sink}}) ->
    digraph:add_vertex(Graph, Sink),
    add_edges_with_vertices(Graph, Source, Sink, digraph:add_edge(Graph, Source, Sink));

add_edges_with_vertices(_Graph, _Source, _Sink, _Edge) ->
    ok.
