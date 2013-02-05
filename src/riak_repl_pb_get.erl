-module(riak_repl_pb_get).

-include_lib("riak_repl_pb_api/include/riak_repl_pb.hrl").
-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-export([client_cluster_names/0]).

-import(riak_pb_kv_codec, [decode_quorum/1]).

-define(PB_MSG_PROXY_GET, 128).
-define(PB_MSG_GET_CLUSTER_ID, 129).
-define(PB_MSG_RESP_CLUSTER_ID, 130).

-record(state, {
        client,    % local client
        modes
    }).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),

    % get the current repl modes and stash them in the state
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_repl_ring:ensure_config(Ring),
    Modes = riak_repl_ring:get_modes(Ring),

    #state{client=C, modes=Modes}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(?PB_MSG_PROXY_GET, Bin) ->
    {ok, riak_repl_pb:decode_rpbreplgetreq(Bin)};
decode(?PB_MSG_GET_CLUSTER_ID, <<>>) ->
    {ok, riak_repl_pb:decode_rpbreplgetclusteridreq(<<>>)}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(#rpbgetresp{} = Msg) ->
    {ok, riak_pb_codec:encode(Msg)};
encode(#rpbreplgetclusteridresp{} = Msg) ->
    {ok,
        [?PB_MSG_RESP_CLUSTER_ID|riak_repl_pb:encode_rpbreplgetclusteridresp(Msg)]}.

%% Process Protocol Buffer Requests
%%
%% @doc Return Cluster Id of the local cluster
process(#rpbreplgetclusteridreq{}, State) ->
    %% get cluster id from local ring manager and format as string
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ClusterId = lists:flatten(
        io_lib:format("~p", [riak_core_ring:cluster_name(Ring)])),
    lager:debug("Repl PB: returning cluster id ~p", [ClusterId]),
    {reply, #rpbreplgetclusteridresp{cluster_id = ClusterId}, State};
%% @doc Return Key/Value pair, derived from the KV version
process(#rpbreplgetreq{bucket=B, key=K, r=R0, pr=PR0, notfound_ok=NFOk,
                   basic_quorum=BQ, if_modified=VClock,
                   head=Head, deletedvclock=DeletedVClock, cluster_id=CName},
            #state{client=C} = State) ->
    R = decode_quorum(R0),
    PR = decode_quorum(PR0),
    lager:debug("doing replicated GET using cluster id ~p", [CName]),
    GetOptions = make_option(deletedvclock, DeletedVClock) ++
        make_option(r, R) ++
        make_option(pr, PR) ++
        make_option(notfound_ok, NFOk) ++
        make_option(basic_quorum, BQ),
    case C:get(B, K, GetOptions) of
        {ok, O} ->
            make_object_response(O, VClock, Head, State);
        {error, {deleted, TombstoneVClock}} ->
            %% Found a tombstone - return its vector clock so it can
            %% be properly overwritten
            {reply, #rpbgetresp{vclock = pbify_rpbvc(TombstoneVClock)}, State};
        {error, notfound} ->
            %% find connection by cluster_id
            CNames = get_client_cluster_names(),
            lager:debug("Cnames ~p", [CNames]),
            case lists:keyfind(CName, 2, CNames) of
                false ->
                    lager:debug("not connected to cluster ~p", [CName]),
                    %% not connected to that cluster, return notfound
                    {reply, #rpbgetresp{}, State};
                {ClientPid, CName} ->
                    lager:debug("Client ~p is connected to cluster ~p",
                        [ClientPid, CName]),
                    case riak_repl_tcp_client:proxy_get(ClientPid, B, K,
                            GetOptions) of
                        {ok, O} ->
                            spawn(riak_repl_util, do_repl_put, [O]),
                            make_object_response(O, VClock, Head, State);
                        {error, {deleted, TombstoneVClock}} ->
                            %% Found a tombstone - return its vector clock so
                            %% it can be properly overwritten
                            {reply, #rpbgetresp{vclock =
                                    pbify_rpbvc(TombstoneVClock)}, State};
                        {error, notfound} ->
                            {reply, #rpbgetresp{}, State};
                        {error, Reason} ->
                            {error, {format,Reason}, State}
                    end
            end;
        {error, Reason} ->
            {error, {format,Reason}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

client_cluster_names() ->
    [{P, client_cluster_name(P)} || {_,P,_,_} <-
        supervisor:which_children(riak_repl_client_sup), P /= undefined].

%%%%%%%%%%%%%%%%%%%%%
%% Internal functions
%%%%%%%%%%%%%%%%%%%%%

%% return a key/value tuple that we can ++ to other options so long as the
%% value is not default or undefined -- those values are pulled from the
%% bucket by the get/put FSMs.
make_option(_, undefined) ->
    [];
make_option(_, default) ->
    [];
make_option(K, V) ->
    [{K, V}].

%% Convert a vector clock to erlang
erlify_rpbvc(undefined) ->
    vclock:fresh();
erlify_rpbvc(<<>>) ->
    vclock:fresh();
erlify_rpbvc(PbVc) ->
    binary_to_term(zlib:unzip(PbVc)).

%% Convert a vector clock to protocol buffers
pbify_rpbvc(Vc) ->
    zlib:zip(term_to_binary(Vc)).

get_client_cluster_names() ->
    {CNames, _BadNodes} = rpc:multicall(riak_core_node_watcher:nodes(riak_kv),
        riak_repl_pb_get, client_cluster_names, []),
    lists:flatten(CNames).

client_cluster_name(Client) ->
    catch(riak_repl_tcp_client:cluster_name(Client)).

make_object_response(O, VClock, Head, State) ->
    case erlify_rpbvc(VClock) == riak_object:vclock(O) of
        true ->
            {reply, #rpbgetresp{unchanged = true}, State};
        _ ->
            Contents = riak_object:get_contents(O),
            PbContent = case Head of
                true ->
                    %% Remove all the 'value' fields from the contents
                    %% This is a rough equivalent of a REST HEAD
                    %% request
                    BlankContents = [{MD, <<>>} || {MD, _} <- Contents],
                    riak_pb_kv_codec:encode_contents(BlankContents);
                _ ->
                    riak_pb_kv_codec:encode_contents(Contents)
            end,
            {reply, #rpbgetresp{content = PbContent,
                    vclock = pbify_rpbvc(riak_object:vclock(O))}, State}
    end.

