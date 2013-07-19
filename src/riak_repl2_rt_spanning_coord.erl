%% @doc Allows a place for interacting with the spanning_upate messges. This
%% is currently limited only to where to forward an update message, as well as
%% kicking of a new message when a sink is started or stopped.
-module(riak_repl2_rt_spanning_coord).

-behavior(gen_event).

% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2, handle_info/2,
    terminate/2, code_change/3]).
% api
-export([start_link/0, stop/0, subscribe/3, subscribe/4, unsubscribe/1,
    begin_chain/3, continue_chain/5]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-else.
-define(debugMsg(_),ok).
-define(debugFmt(_,_1),ok).
-endif.

-record(local_update, {
    source :: any(), sink :: any(),
    action :: 'connect' | 'disconnect' | 'disconnect_all',
    routed = [] :: [any()],
    local_routed = [] :: [any()],
    version = 0 :: non_neg_integer()
}).

%% === API =====

start_link() ->
    case ets:info(?MODULE) of
        undefined ->
            ets:new(?MODULE, [named_table, public]);
        _ ->
            ok
    end,
    gen_event:start_link({local, ?MODULE}).

stop() ->
    catch ets:delete(?MODULE),
    gen_event:stop(?MODULE).

subscribe(Name, CallbackMod, State) ->
    SubId = make_ref(),
    subscribe(SubId, Name, CallbackMod, State).

subscribe(Id, Name, CallbackMod, State) ->
    case gen_event:add_sup_handler(?MODULE, {?MODULE, {Name, Id}}, {Name, CallbackMod, State}) of
        ok ->
            {ok, {Name, Id}};
        Else ->
            Else
    end.

unsubscribe(Id) ->
    gen_event:delete_handler(?MODULE, {?MODULE, Id}, normal).

begin_chain(Source, Sink, Action) ->
    set_model(Source, Sink, Action),
    Vsn = case ets:insert_new(?MODULE, {{Source, Sink}, 1}) of
        false ->
            ets:update_counter(?MODULE, {Source, Sink}, 1);
        true ->
            1
    end,
    Locals = get_local_names(),
    Msg = #local_update{source = Source, sink = Sink, action = Action,
        routed = [], local_routed = Locals, version = Vsn},
    gen_event:notify(?MODULE, Msg).

continue_chain(Source, Sink, Action, Vsn, Routed) ->
    SendMsg = case ets:lookup(?MODULE, {Source, Sink}) of
        [] ->
            ets:insert(?MODULE, {{Source, Sink}, Vsn}),
            true;
        [{{Source, Sink}, OldVsn}] when OldVsn < Vsn ->
            ets:insert(?MODULE, {{Source, Sink}, Vsn}),
            true;
        _ ->
            false
    end,
    if
        SendMsg ->
            set_model(Source, Sink, Action),
            Msg = #local_update{source = Source, sink = Sink,
                action = Action, routed = Routed,
                local_routed = get_local_names(), version = Vsn},
            gen_event:notify(?MODULE, Msg);
        true ->
            ok
    end.

% === gen_event =====

init({Name, CallbackMod, CbState}) ->
    {ok, {Name, CallbackMod, CbState}}.

handle_event(Msg, {Name, CallbackMod, CbState}) when is_record(Msg, local_update) ->
    case lists:member(Name, Msg#local_update.routed) of
        false ->
            #local_update{routed = PassedIn, local_routed = Locals,
                action = Action, source = Source, sink = Sink,
                version = Vsn} = Msg,
            Routed2 = ordsets:union(Locals, PassedIn),
            CallbackMod:spanning_update(Source, Sink, Action, Routed2, Vsn, CbState);
        true ->
            ok
    end,
    {ok, {Name, CallbackMod, CbState}}.

handle_call(_Req, _State) ->
    {remove_handler, nyi}.

handle_info(_Info, _State) ->
    remove_handler.

terminate(_Why, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% === internal =====

set_model(Source, Sink, connect) ->
    riak_repl2_rt_spanning_model:add_cascade(Source, Sink);
set_model(Source, Sink, disconnect) ->
    riak_repl2_rt_spanning_model:drop_cascade(Source, Sink);
set_model(Source, _Sink, disconnect_all) ->
    riak_repl2_rt_spanning_model:drop_all_cascades(Source).

get_local_names() ->
    Locals = [Name || {?MODULE, {Name, _}} <- gen_event:which_handlers(?MODULE)],
    ordsets:from_list(Locals).
