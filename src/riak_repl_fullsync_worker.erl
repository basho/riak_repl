-module(riak_repl_fullsync_worker).

-behaviour(gen_server).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
        code_change/3]).
-export([start_link/1, do_put/3, do_binputs/4, do_get/6, do_get/7]).

-export([do_binputs_internal/3]). %% Used for unit/integration testing, not public interface

-record(state, {
          ver :: riak_object:r_object_vsn()  %% greatest shared obj version
    }).

start_link(_Args) ->
    gen_server:start_link(?MODULE, [], []).

do_put(Pid, Obj, Pool) ->
    gen_server:cast(Pid, {put, Obj, Pool}).

do_binputs(Pid, BinObjs, DoneFun, Pool) ->
    %% safe to cast as the pool size will add backpressure on the sink
    gen_server:cast(Pid, {puts, BinObjs, DoneFun, Pool}).

do_get(Pid, Bucket, Key, Transport, Socket, Pool) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool}, infinity).

do_get(Pid, Bucket, Key, Transport, Socket, Pool, Partition) ->
    gen_server:call(Pid, {get, Bucket, Key, Transport, Socket, Pool, Partition}, infinity).


init([]) ->
    {ok, #state{ver=v0}}. %% initially use legacy riak_object format as "common" ver

encode_obj_msg(V, {fs_diff_obj, RObj}) ->
    case V of
        v0 ->
            term_to_binary({fs_diff_obj, RObj});
        v1 ->
            BObj = riak_repl_util:to_wire(w1,RObj),
            term_to_binary({fs_diff_obj, BObj})
    end.

handle_call({get, B, K, Transport, Socket, Pool}, From, State) ->
    %% unblock the caller
    gen_server:reply(From, ok),
    %% do the get and send it to the client
    {ok, Client} = riak:local_client(),
    case Client:get(B, K, 1, ?REPL_FSM_TIMEOUT) of
        {ok, RObj} ->
            %% we don't actually have the vclock to compare, so just send the
            %% key and let the other side sort things out.
            V = State#state.ver,
            case riak_repl_util:repl_helper_send(RObj, Client) of
                cancel ->
                    skipped;
                Objects when is_list(Objects) ->
                    %% Cindy: Santa, why can we encode our own binary object?
                    %% Santa: Because the send() function will convert our tuple
                    %%        to a binary
                    [riak_repl_tcp_server:send(Transport, Socket,
                                               encode_obj_msg(V,{fs_diff_obj,O}))
                     || O <- Objects],
                    riak_repl_tcp_server:send(Transport, Socket,
                                              encode_obj_msg(V,{fs_diff_obj,RObj}))
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
                    V = State#state.ver,
                    case riak_repl_util:repl_helper_send(RObj, Client) of
                        cancel ->
                            skipped;
                        Objects when is_list(Objects) ->
                            %% Cindy: Santa, why can we encode our own binary object?
                            %% Santa: Because, Cindy, the send() function accepts
                            %%        either a binary or a term.
                            [riak_repl_tcp_server:send(Transport, Socket,
                                                       encode_obj_msg(V,{fs_diff_obj,O}))
                             || O <- Objects],
                            riak_repl_tcp_server:send(Transport, Socket,
                                                      encode_obj_msg(V,{fs_diff_obj,RObj}))
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
handle_cast({puts, BinObjs, DoneFun, Pool}, State) ->
    ?MODULE:do_binputs_internal(BinObjs, DoneFun, Pool), % so it can be mecked
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
do_binputs_internal(BinObjs, DoneFun, Pool) ->
   % io:format("Called do_binputs_internal\n"),
    %% TODO: add mechanism for detecting put failure so 
    %% we can drop rtsink and have it resent
    Ver = v0,
    Objects = case Ver of
                  %% old-ish repl sends term_to_binary([RObjs])
                  v0 -> riak_repl_util:from_wire(w0, BinObjs);
                  %% new-ish repl sends binaried list of {K, B, and BinObject}
                  _V -> riak_repl_util:from_wire(w1, BinObjs)
              end,
    [riak_repl_util:do_repl_put(Obj) || Obj <- Objects],
    poolboy:checkin(Pool, self()),
    %% let the caller know
    DoneFun().
