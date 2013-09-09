-module(riak_repl_reduced).
-behavior(riak_kv_mutator).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-export([mutate_get/1, mutate_put/5]).

mutate_get(InObject) ->
    lager:debug("mutate_get"),
    Contents = riak_object:get_contents(InObject),
    Reals = lists:foldl(fun({Meta, _Value}, N) ->
        case dict:find(?MODULE, Meta) of
            {ok, M} when M < N ->
                M;
            _ ->
                N
        end
    end, always, Contents),
    case Reals of
        0 ->
            lager:debug("proxy get"),
            proxy_get(InObject);
        always ->
            % not a reduced object
            InObject;
        N ->
            BKey = {riak_object:bucket(InObject), riak_object:key(InObject)},
            DocIdx = riak_core_util:chash_key(BKey),
            Preflist = riak_core_apl:get_primary_apl(DocIdx, N, riak_kv),
            Nth = random:uniform(length(Preflist)),
            Self = self(),
            case lists:nth(Nth, Preflist) of
                {{_Partition, Self}, _PrimaryNess} ->
                    lager:debug("odd that we get ourselves as a pref. doing proxy"),
                    proxy_get(InObject);
                {Single, _PrimaryNess} ->
                    local_ring_get(InObject, BKey, Single)
            end
    end.

local_ring_get(InObject, BKey, Partition) ->
    {_P, MonitorTarg} = Partition,
    MonRef = erlang:monitor(process, MonitorTarg),
    Preflist = [Partition],
    ReqId = make_ref(),
    Req = ?KV_GET_REQ{bkey=BKey, req_id = ReqId},
    riak_core_vnode_master:command(Preflist, Req, {raw, ReqId, self()}, riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, RObj}, _, ReqId}} ->
            RObj;
        {ReqId, {r, {error, notfound}, _, ReqId}} ->
            proxy_get(InObject);
        {'DOWN', MonRef, prcess, MonitorTarg, Wut} ->
            lager:info("could not get value from target due to exit: ~p", [Wut]),
            proxy_get(InObject)
    after
        ?REPL_FSM_TIMEOUT ->
            lager:info("timeout"),
            proxy_get(InObject)
    end.

proxy_get(Object) ->
    % TODO implement
    Object.

mutate_put(InMeta, InVal, RevealedMeta, In, Props) ->
    FunList = [fun skip_reduce_cause_local/5, fun reduce_by_bucket/5, fun reduce_by_cluster/5],
    mutate_put(InMeta, InVal, RevealedMeta, In, Props, until_not_false([InMeta, InVal, RevealedMeta, In, Props], FunList)).

mutate_put(InMeta, InVal, RevealedMeta, _In, _Props, local) ->
    lager:debug("local reduction: only tagging cluster of record"),
    Cluster = riak_core_connection:symbolic_clustername(),
    Meta2 = dict:store(cluster_of_record, Cluster, InMeta),
    RevealedMeta2 = dict:store(cluster_of_record, Cluster, RevealedMeta),
    {Meta2, InVal, RevealedMeta2};

mutate_put(InMeta, InVal, RevealedMeta, _In, _Props, false) ->
    lager:debug("no mutation done"),
    {InMeta, InVal, RevealedMeta};

mutate_put(InMeta, InVal, RevealedMeta, _In, _Props, always) ->
    lager:debug("always full objects"),
    {InMeta, InVal, RevealedMeta};

mutate_put(InMeta, InVal, RevealedMeta, In, _Props, never) ->
    lager:debug("never full objects, reduce"),
    reduce(InMeta, InVal, RevealedMeta, In, 0);

mutate_put(InMeta, InVal, RevealedMeta, RObj, BucketProps, NumberReals) ->
    BKey = {riak_object:bucket(RObj), riak_object:key(RObj)},
    DocIdx = riak_core_util:chash_key(BKey),
    Bucket_N = proplists:get_value(n_val,BucketProps),
    Preflist = riak_core_apl:get_primary_apl(DocIdx, Bucket_N, riak_kv),
    if
        length(Preflist) =< NumberReals ->
            mutate_put(InMeta, InVal, RevealedMeta, RObj, BucketProps, never);
        true ->
            lager:debug("only keep ~p real copies from ~p", [NumberReals, length(Preflist)]),
            {RealsList,_Ignored} = lists:split(NumberReals, Preflist),
            maybe_reduce(InMeta, InVal, RevealedMeta, RObj, RealsList, NumberReals)
    end.

maybe_reduce(InMeta, InVal, RevealedMeta, RObj, RealList, NumberReals) ->
    Self = self(),
    PrefPids = lists:map(fun({{Partition, _Node}, _PrimaryNess}) ->
        {ok, VnodePid} = riak_core_vnode_master:get_vnode_pid(Partition, riak_kv_vnode),
        VnodePid
    end, RealList),
    lager:debug("maybe reduce~n"
        "    Self: ~p~n"
        "    PrefPids: ~p", [Self, PrefPids]),
    case lists:member(Self, PrefPids) of
        true ->
            {InMeta, InVal, RevealedMeta};
        false ->
            reduce(InMeta, InVal, RevealedMeta, RObj, NumberReals)
    end.

reduce(InMeta, _InVal, RevealedMeta, RObj, NumberReals) ->
    lager:debug("doing a reduction"),
    AAEHash = term_to_binary(riak_object:hash(RObj)),

    NewMetas = [
        {?MODULE, NumberReals},
        {aae_hash, AAEHash}
    ],

    {Meta2, RevealedMeta2} = lists:foldl(fun({Key, Val}, {M,R}) ->
        {dict:store(Key, Val, M), dict:store(Key, Val, R)}
    end, {InMeta, RevealedMeta}, NewMetas),

    {Meta2, <<>>, RevealedMeta2}.

until_not_false(_ArgList, []) ->
    false;

until_not_false(ArgList, [Fun | Tail]) ->
    case erlang:apply(Fun, ArgList) of
        false ->
            until_not_false(ArgList, Tail);
        Res ->
            Res
    end.

skip_reduce_cause_local(Meta, _Value, _RevealedMeta, _Obj, _Props) ->
    case dict:find(cluster_of_record, Meta) of
        error ->
            local;
        {ok, ClusterName} ->
            case riak_core_connection:symbolic_clustername() of
                ClusterName ->
                    lager:debug("already locally tagged"),
                    always;
                _OtherName ->
                    % let the other functions determine smallening
                    false
            end
    end.

reduce_by_bucket(_Meta, _Value, _RevealedMeta, _Obj, Props) ->
    case proplists:get_value(full_objects, Props) of
        undefined ->
            false;
        Else ->
            Else
    end.

reduce_by_cluster(_Meta, _Value, _RevealedMeta, _In, _Props) ->
    riak_core_metadata:get({riak_repl, reduced_n}, full_objects, [{default, always}]).
