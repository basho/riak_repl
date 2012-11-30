-module(riak_repl2_fs_node_reserver).
-include("riak_repl.hrl").
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% 20 seconds. sources should claim within 5 seconds, but give them a little more time
-define(RESERVATION_TIMEOUT, (20 * 1000)).

-record(state, {
    leader :: node(),
    reservations = []
    }).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).
-export([reserve/1, unreserve/1, claim_reservation/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

reserve(Partition) ->
    Node = get_partition_node(Partition),
    % I don't want to crash the caller if the node is down
    try gen_server:call({?SERVER, Node}, {reserve, Partition}) of
        Out -> Out
    catch
        'EXIT':{noproc, _} ->
            down;
        'EXIT':{{nodedown, _}, _} ->
            down
    end.

unreserve(Partition) ->
    Node = get_partition_node(Partition),
    gen_server:cast({?SERVER, Node}, {unreserve, Partition}).

claim_reservation(Partition) ->
    Node = get_partition_node(Partition),
    gen_server:cast({?SERVER, Node}, {claim_reservation, Partition}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call({reserve, Partition}, _From, State) ->
    Kids = supervisor:which_children(riak_repl2_fssink_sup, node()),
    Max = app_helper:get_env(riak_repl, max_fssink_node, ?DEFAULT_MAX_SINKS_NODE),
    Running = length(Kids),
    Reserved = length(State#state.reservations),
    if
        (Running + Reserved) < Max ->
            Tref = erlang:send_after(?RESERVATION_TIMEOUT, self(), {reservation_expired, Partition}),
            Reserved2 = [{Partition, Tref} | State#state.reservations],
            {reply, ok, State#state{reservations = Reserved2}};
        true ->
            {reply, busy, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({unreserve, Partition}, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {noreply, State#state{reservations = Reserved2}};

handle_cast({claim_reservation, Partition}, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {noreply, State#state{reservations = Reserved2}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({reservation_expired, Partition}, State) ->
    Reserved2 = cancel_reservation_timeout(Partition, State#state.reservations),
    {noreply, State#state{reservations = Reserved2}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

get_partition_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    proplists:get_value(Partition, Owners).

cancel_reservation_timeout(Partition, Reserved) ->
    case proplists:get_value(Partition, Reserved) of
        undefined ->
            Reserved;
        Tref ->
            erlang:cancel_timer(Tref),
            proplists:delete(Partition, Reserved)
    end.

