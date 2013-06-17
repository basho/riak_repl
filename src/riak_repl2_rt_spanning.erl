-module(riak_repl2_rt_spanning).

% api
-export([start_link/0, stop/0]).
-export([known_clusters/0]).
-export([replications/0, add_replicaton/2, drop_replication/2]).

% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% api
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:cast(?MODULE, stop).

known_clusters() ->
    gen_server:call(?MODULE, known_clusters).

replications() ->
    gen_server:call(?MODULE, replications).

add_replicaton(Source, Sink) ->
    gen_server:cast(?MODULE, {add_replicaton, Source, Sink}).

drop_replication(Source, Sink) ->
    gen_server:cast(?MODULE, {drop_replication, Source, Sink}).

% gen_server
init(_) ->
    {ok, digraph:new()}.

handle_call(known_clusters, _From, Graph) ->
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

handle_cast({add_cluster, ClusterName}, Graph) ->
    digraph:add_vertex(Graph, ClusterName),
    {noreply, Graph};

handle_cast({drop_cluster, ClusterName}, Graph) ->
    digraph:del_vertex(Graph, ClusterName),
    {noreply, Graph};

handle_cast({add_replicaton, Source, Sink}, Graph) ->
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

-ifdef(TEST).

functionality_test_() ->
    {setup, fun() ->
        ok
    end,
    fun(ok) ->
        case whereis(?MODULE) of
            undefined ->
                ok;
            Pid ->
                exit(Pid, kill)
        end
    end,
    fun(ok) -> [

        {"start up", fun() ->
            Got = ?MODULE:start_link(),
            ?assertMatch({ok, _Pid}, Got),
            ?assert(is_pid(element(2, Got))),
            unlink(element(2, Got))
        end},

        {"get list of known clusters", fun() ->
            ?assertEqual([], ?MODULE:known_clusters())
        end},

        {"list replications", fun() ->
            Got = ?MODULE:replications(),
            ?assertEqual([], Got)
        end},

        {"add a replication", fun() ->
            ?MODULE:add_replicaton("source", "sink"),
            ?assertEqual(lists:sort(["source", "sink"]), lists:sort(?MODULE:known_clusters())),
            Repls = ?MODULE:replications(),
            ?assertEqual(["sink"], proplists:get_value("source", Repls)),
            ?assertEqual([], proplists:get_value("sink", Repls))
        end},

        {"drop replication", fun() ->
            ?MODULE:add_replicaton("source", "sink"),
            ?MODULE:drop_replication("source", "sink"),
            ?assertEqual(lists:sort(["source", "sink"]), lists:sort(?MODULE:known_clusters())),
            Repls = ?MODULE:replications(),
            ?assertEqual([], proplists:get_value("source", Repls)),
            ?assertEqual([], proplists:get_value("sink", Repls))

        end},

        {"tear down", fun() ->
            Pid = whereis(?MODULE),
            Mon = erlang:monitor(process, Pid),
            ?MODULE:stop(),
            Got = receive
                {'DOWN', Mon, process, Pid, _Why} ->
                    true
            after 1000 ->
                {error, timeout}
            end,
            ?assert(Got)
        end}

    ] end}.

-endif.