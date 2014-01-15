%% @doc A riak_kv_mutator to shrink down objects on a put, then expand them
%% on a get.  Shrinking is based either on cluster or bucket settings.
%%
%% Either due to size or legal constraints, it is sometimes desirable to
%% reduce the amount of data stored on a sink cluster. This mutator can
%% take objects that are stored fully on another cluster and reduce the
%% disk space used. It can then pull the full object from the source
%% cluster if needed. In short, one is trading CPU and (possibly)
%% bandwidth to reduce storage cost.
%%
%% Like other kv_mutators, this module is used on every put and any get
%% when the module was used on the object's put.
%%
%% == Configuration ==
%%
%% When a put is done, the object is checked for the `cluster_of_record'
%% metadata. If that data is missing or has the same name as the cluster,
%% the object is not reduced. Beyond that, when to reduce can be configured.
%%
%% By default the mutator never reduces an object. To enable reduction
%% cluster-wide, use riak_core_metadata and set
%% ``{'riak_repl', 'reduced_n'}, 'full_objects}''' to one of ``'always''',
%% ``'never''', or a positive integer.
%%
%% The ``'full_objects''' bucket property override cluster settings, and uses
%% the same option values.
%%
%% The ``'always''' option means to always use full_objects; in other words
%% never reduce the objects. This is the default value.
%%
%% A positive integer means keep that many or N full objects, whichever is
%% smaller. N is the usual N val on put. If a full value cannot be found,
%% a proxy get is attempted.
%%
%% The ``'never''' option means all requests for the object will use proxy get
%% to the cluster of record. If the object cannot be retrieved from there,
%% a not found value is returned.
%%
%% For proxy get to work, the source cluster must enable proxy get to the sink
%% cluster.

-module(riak_repl_reduced).
-behavior(riak_kv_mutator).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_repl.hrl").

-export([mutate_get/1, mutate_put/5]).

%% @doc Un-reduce an object if needed. If a real object cannot be found on this
%% cluster, a proxy get is attempted. If that fails, a not found is returned.
-spec mutate_get(InObject :: riak_object:riak_object()) ->
    riak_object:riak_object() | 'notfound'.
mutate_get(InObject) ->
    Key = riak_object:key(InObject),
    Bucket = riak_object:bucket(InObject),
    lager:debug("mutate_get for ~p in ~p", [Key, Bucket]),
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
            lager:debug("Not a reduced object"),
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
                    lager:debug("local ring get"),
                    local_ring_get(InObject, BKey, Single)
            end
    end.

local_ring_get(InObject, BKey, Partition) ->
    lager:debug("Local ring get for ~p on partition ~p", [BKey, Partition]),
    {Index, _Node} = Partition,
    {ok, MonitorTarg} = riak_core_vnode_manager:get_vnode_pid(Index, riak_kv_vnode),
    MonRef = erlang:monitor(process, MonitorTarg),
    Preflist = [Partition],
    ReqId = make_ref(),
    Req = ?KV_GET_REQ{bkey=BKey, req_id = ReqId},
    riak_core_vnode_master:command(Preflist, Req, {raw, ReqId, self()}, riak_kv_vnode_master),
    receive
        {ReqId, {r, {ok, RObj}, _, ReqId}} ->
            lager:debug("successful get!"),
            RObj;
        {ReqId, {r, {error, notfound}, _, ReqId}} ->
            lager:debug("not found, falling back to proxy"),
            proxy_get(InObject);
        {'DOWN', MonRef, prcess, MonitorTarg, Wut} ->
            lager:info("could not get value from target due to exit: ~p", [Wut]),
            proxy_get(InObject)
    after
        ?REPL_FSM_TIMEOUT ->
            lager:info("timeout"),
            proxy_get(InObject)
    end.

% by the time we get here, we should have already determined that this is a
% reduced object that does not have a full object on the local cluster
proxy_get(Object) ->
    case get_cluster_of_record(Object) of
        undefined ->
            lager:debug("no cluster of record, no proxy get."),
            notfound;
        Cluster ->
            proxy_get(Cluster, Object)
    end.

proxy_get(Cluster, Object) ->
   case riak_repl2_leader:leader_node() of
        undefined ->
            lager:debug("no leader node, no proxy get"),
            notfound;
        LeaderNode ->
            proxy_get(LeaderNode, Cluster, Object)
    end.

proxy_get(Leader, Cluster, Object) ->
    Bucket = riak_object:bucket(Object),
    Key = riak_object:key(Object),
    ProcessName = riak_repl_util:make_pg_proxy_name(Cluster),
    try riak_repl2_pg_proxy:proxy_get({ProcessName, Leader}, Bucket, Key, []) of
        {error, notfound} ->
            lager:debug("Couldn't find ~p on cluster ~p", [{Bucket,Key},Cluster]),
            notfound;
        {ok, NewObject} ->
            lager:debug("successful proxy get!"),
            NewObject;
        Resp ->
            lager:debug("no idea: ~p", [Resp]),
            notfound
    catch
        What:Why ->
            lager:debug("proxy get failed: ~p:~p", [What,Why]),
            notfound
    end.

get_cluster_of_record(Object) ->
    Metas = riak_object:get_metadatas(Object),
    lists:foldl(fun
        (Meta, undefined) ->
            case dict:find(cluster_of_record, Meta) of
                error ->
                    undefined;
                {ok, C} ->
                    C
            end;
        (_Meta, Acc) ->
            Acc
    end, undefined, Metas).

%% @doc Check if the object should be reduced, and do so; if nothing else, tag
%% the cluster of record.
-spec mutate_put(InMeta :: dict(), InVal :: term(), RevealedMeta :: dict(),
    In :: riak_object:riak_object(), Props :: orddict:orddict()) ->
        {dict(), term(), dict()}.
mutate_put(InMeta, InVal, RevealedMeta, In, Props) ->
    FunList = [fun skip_reduce_cause_local/5, fun reduce_by_bucket/5, fun reduce_by_cluster/5],
    mutate_put(InMeta, InVal, RevealedMeta, In, Props, until_not_false([InMeta, InVal, RevealedMeta, In, Props], FunList)).

mutate_put(InMeta, InVal, RevealedMeta, _In, _Props, local) ->
    lager:debug("local reduction: only tagging cluster of record"),
    Cluster = get_clustername(),
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

% the symbolic_clustername functions in riak_core_connection always return
% a string. However, if the cluster is not named, that string is useless
% for proxy get. So, rather than do a proxy get that can never succeede
% we check if the cluster is actually named, and if not, return
% 'undefined' rather than lie.
get_clustername() ->
    case riak_core_ring_manager:get_my_ring() of
        {ok, Ring} ->
            get_clustername(Ring);
        {error, Reason} ->
            lager:error("Can't read symbolic clustername because: ~p", [Reason]),
            undefined
    end.

get_clustername(Ring) ->
    case riak_core_ring:get_meta(symbolic_clustername, Ring) of
        {ok, Name} -> Name;
        undefined -> undefined
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
