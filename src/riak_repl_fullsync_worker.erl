-module(riak_repl_fullsync_worker).

-behaviour(gen_server).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, do_put/3, do_binputs/5, do_get/7, do_get/8]).

-export([do_binputs_internal/4]). %% Used for unit/integration testing, not public interface

-record(state, {}).

start_link(_Args) ->
    gen_server:start_link(?MODULE, [], []).

do_put(Pid, Obj, Pool) ->
    gen_server:cast(Pid, {put, Obj, Pool}).

do_binputs(Pid, BinObjs, DoneFun, Pool, Ver) ->
    %% safe to cast as the pool size will add backpressure on the sink
    gen_server:cast(Pid, {puts, BinObjs, DoneFun, Pool, Ver}).

do_get(Pid, Bucket, Key, Transport, Socket, Pool, Ver) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool, Ver}, infinity).

do_get(Pid, Bucket, Key, Transport, Socket, Pool, Partition, Ver) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool, Partition, Ver},
                    infinity).


init([]) ->
    {ok, #state{}}.

encode_obj_msg(V, {fs_diff_obj, RObj}) ->
    case V of
        w0 ->
            term_to_binary({fs_diff_obj, RObj});
        w1 ->
            BObj = riak_repl_util:to_wire(w1,RObj),
            term_to_binary({fs_diff_obj, BObj})
    end.

handle_call({get, B, K, Transport, Socket, Pool, Ver}, From, State) ->
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
                    %% Cindy: Santa, why can we encode our own binary object?
                    %% Santa: Because the send() function will convert our tuple
                    %%        to a binary
                    [riak_repl_tcp_server:send(Transport, Socket,
                                               encode_obj_msg(Ver,{fs_diff_obj,O}))
                     || O <- Objects],
                    riak_repl_tcp_server:send(Transport, Socket,
                                              encode_obj_msg(Ver,{fs_diff_obj,RObj}))
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
%% Handle a get() request by sending the named object via the tcp server back
%% to the tcp client.
handle_call({get, B, K, Transport, Socket, Pool, Partition, Ver}, From, State) ->
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
                            %% Cindy: Santa, why can we encode our own binary object?
                            %% Santa: Because, Cindy, the send() function accepts
                            %%        either a binary or a term.
                            [riak_repl_tcp_server:send(Transport, Socket,
                                                       encode_obj_msg(Ver,{fs_diff_obj,O}))
                             || O <- Objects],
                            riak_repl_tcp_server:send(Transport, Socket,
                                                      encode_obj_msg(Ver,{fs_diff_obj,RObj}))
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

handle_cast({put, RObj, Pool}, State) ->
    %% do the put
    riak_repl_util:do_repl_put(RObj),
    %% unblock this worker for more work (or death)
    poolboy:checkin(Pool, self()),
    {noreply, State};
handle_cast({puts, BinObjs, DoneFun, Pool, Ver}, State) ->
    ?MODULE:do_binputs_internal(BinObjs, DoneFun, Pool, Ver), % so it can be mecked
    {noreply, State};
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


%% Put a list of objects b2d, reinsert back in the pool and call DoneFun.
%% TODO: rename external 'do_blah' functions.  rest of riak uses do_blah
%% for internal work
do_binputs_internal(BinObjs, DoneFun, Pool, Ver) ->
   % io:format("Called do_binputs_internal\n"),
    %% TODO: add mechanism for detecting put failure so 
    %% we can drop rtsink and have it resent
    Objects = riak_repl_util:from_wire(Ver, BinObjs),
    [riak_repl_util:do_repl_put(Obj) || Obj <- Objects],
    poolboy:checkin(Pool, self()),
    %% let the caller know
    DoneFun().
