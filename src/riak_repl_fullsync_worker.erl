-module(riak_repl_fullsync_worker).

-behaviour(gen_server).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, do_put/3, do_get/6, do_get/7]).

-record(state, {
    }).

start_link(_Args) ->
    gen_server:start_link(?MODULE, [], []).

do_put(Pid, Obj, Pool) ->
    gen_server:call(Pid, {put, Obj, Pool}, infinity).

do_get(Pid, Bucket, Key, Transport, Socket, Pool) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool}, infinity).

do_get(Pid, Bucket, Key, Transport, Socket, Pool, Partition) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool, Partition}, infinity).


init([]) ->
    {ok, #state{}}.

handle_call({put, Obj, Pool}, From, State) ->
    %% unblock the caller
    gen_server:reply(From, ok),
    %% do the put
    riak_repl_util:do_repl_put(Obj),
    %% unblock this worker for more work (or death)
    poolboy:checkin(Pool, self()),
    {noreply, State};
handle_call({get, B, K, Transport, Socket, Pool}, From, State) ->
    %% unblock the caller
    gen_server:reply(From, ok),
    %% do the get and send it to the client
    {ok, Client} = riak:local_client(),
    case Client:get(B, K, 1, ?REPL_FSM_TIMEOUT) of
        {ok, RObj} ->
            %% we don't actually have the vclock to compare, so just send the
            %% key and let the other side sort things out.
            case riak_repl_util:repl_helper_send(RObj, Client) of
                cancel ->
                    skipped;
                Objects when is_list(Objects) ->
                    [riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, O}) || O <- Objects],
                    riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, RObj})
            end,
            ok;
        {error, notfound} ->
            ok;
        _ ->
            ok
    end,
    %% unblock this worker for more work (or death)
    poolboy:checkin(Pool, self()),
    {noreply, State};
handle_call({get, B, K, Transport, Socket, Pool, Partition}, From, State) ->
    %% unblock the caller
    gen_server:reply(From, ok),

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    OwnerNode = riak_core_ring:index_owner(Ring, Partition),

    Preflist = [{Partition, OwnerNode}],

    ReqID = make_ref(),

    Req = ?KV_GET_REQ{bkey={B, K}, req_id=ReqID},
    %% Assuming this function is called from a FSM process
    %% so self() == FSM pid
    riak_core_vnode_master:command(Preflist,
        Req,
        {raw, ReqID, self()},
        riak_kv_vnode_master),

    receive
        {ReqID, Reply} ->
            case Reply of
                {r, {ok, RObj}, _, ReqID} ->
                    %% we don't actually have the vclock to compare, so just send the
                    %% key and let the other side sort things out.
                    {ok, Client} = riak:local_client(),
                    case riak_repl_util:repl_helper_send(RObj, Client) of
                        cancel ->
                            skipped;
                        Objects when is_list(Objects) ->
                            [riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, O}) || O <- Objects],
                            riak_repl_tcp_server:send(Transport, Socket, {fs_diff_obj, RObj})
                    end,
                    ok;
                {r, {error, notfound}, _, ReqID} ->
                    ok;
                _ ->
                    ok
            end
    after
        ?REPL_FSM_TIMEOUT ->
            ok
    end,
    %% unblock this worker for more work (or death)
    poolboy:checkin(Pool, self()),
    {noreply, State};
handle_call(_Event, _From, State) ->
    {reply, ok, State}.

handle_cast(_Event, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

