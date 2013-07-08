%% @doc Allows a place for interacting with the spanning_upate messges. This
%% is currently limited only to where to forward an update message, as well as
%% kicking of a new message when a sink is started or stopped.
-module(riak_repl2_rt_spanning_coord).

-export([spanning_update/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-else.
-define(debugMsg(_),ok).
-define(debugFmt(_,_),ok).
-endif.

spanning_update(SourceName, SinkName, ConnectAction, Routed) ->
    set_model(SourceName, SinkName, ConnectAction),
    Sources = riak_repl2_rtsource_conn_sup:enabled(),
    Sinks = riak_repl2_rtsink_conn_sup:started(),
    Routed2 = lists:foldl(fun({Name, _Pid}, Acc) ->
        [Name | Acc]
    end, Routed, Sources),
    Routed3 = ordsets:from_list(Routed2),
    [riak_repl2_rtsource_conn:spanning_update(Pid, SourceName, SinkName, ConnectAction, Routed3) || {Name, Pid} <- Sources, not lists:member(Name, Routed)],
    [riak_repl2_rtsink_conn:spanning_update(Pid, SourceName, SinkName, ConnectAction, Routed) || Pid <- Sinks],
    ok.

set_model(Source, Sink, connect) ->
    riak_repl2_rt_spanning_model:add_cascade(Source, Sink);
set_model(Source, Sink, disconnect) ->
    riak_repl2_rt_spanning_model:drop_cascade(Source, Sink);
set_model(Source, _Sink, disconnect_all) ->
    riak_repl2_rt_spanning_model:drop_all_cascades(Source).
