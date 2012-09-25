%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_util).
-author('Andy Gross <andy@basho.com>').
-include_lib("public_key/include/OTP-PUB-KEY.hrl").
-include("riak_repl.hrl").
-export([make_peer_info/0,
         make_fake_peer_info/0,
         validate_peer_info/2,
         capability_from_vsn/1,
         get_partitions/1,
         do_repl_put/1,
         site_root_dir/1,
         ensure_site_dir/1,
         binpack_bkey/1,
         binunpack_bkey/1,
         merkle_filename/3,
         keylist_filename/3,
         valid_host_ip/1,
         normalize_ip/1,
         format_socketaddrs/2,
         maybe_use_ssl/0,
         upgrade_client_to_ssl/1,
         choose_strategy/2,
         strategy_module/2,
         configure_socket/2,
         repl_helper_send/2,
         repl_helper_send_realtime/2,
         schedule_fullsync/0,
         schedule_fullsync/1,
         elapsed_secs/1,
         shuffle_partitions/2
     ]).

make_peer_info() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    SafeRing = riak_core_ring:downgrade(1, Ring),
    {ok, RiakVSN} = application:get_key(riak_kv, vsn),
    {ok, ReplVSN} = application:get_key(riak_repl, vsn),
    #peer_info{riak_version=RiakVSN, repl_version=ReplVSN, ring=SafeRing}.

%% Makes some plausible, but wrong, peer info. Used to get to SSL negotiation
%% without leaking sensitive information.
make_fake_peer_info() ->
    FakeRing = riak_repl_ring:ensure_config(riak_core_ring:fresh(1, node())),
    SafeRing = riak_core_ring:downgrade(1, FakeRing),
    #peer_info{riak_version="0.0.0", repl_version="0.0.0", ring=SafeRing}.

validate_peer_info(T=#peer_info{}, M=#peer_info{}) ->
    TheirPartitions = get_partitions(T#peer_info.ring),
    OurPartitions = get_partitions(M#peer_info.ring),
    TheirPartitions =:= OurPartitions.

%% Build a default capability from the version information in #peerinfo{}
capability_from_vsn(#peer_info{repl_version = ReplVsnStr}) ->
    ReplVsn = parse_vsn(ReplVsnStr),
    case ReplVsn >= {0, 14, 0} of
        true ->
            [bounded_queue];
        false ->
            []
    end.

get_partitions(Ring) ->
    lists:sort([P || {P, _} <-
            riak_core_ring:all_owners(riak_core_ring:upgrade(Ring))]).

do_repl_put(Object) ->
    B = riak_object:bucket(Object),
    K = riak_object:key(Object),
    case repl_helper_recv(Object) of
        ok ->
            ReqId = erlang:phash2(erlang:now()),
            B = riak_object:bucket(Object),
            K = riak_object:key(Object),
            Opts = [asis, disable_hooks, {update_last_modified, false}],

            riak_kv_put_fsm_sup:start_put_fsm(node(), [ReqId, Object, 1, 1,
                    ?REPL_FSM_TIMEOUT,
                    self(), Opts]),

            %% block waiting for response
            wait_for_response(ReqId, "put"),

            case riak_kv_util:is_x_deleted(Object) of
                true ->
                    lager:debug("Incoming deleted obj ~p/~p", [B, K]),
                    reap(ReqId, B, K),
                    %% block waiting for response
                    wait_for_response(ReqId, "reap");
                false ->
                    lager:debug("Incoming obj ~p/~p", [B, K])
            end;
        cancel ->
            lager:debug("Skipping repl received object ~p/~p", [B, K])
    end.

reap(ReqId, B, K) ->
    riak_kv_get_fsm_sup:start_get_fsm(node(),
                                      [ReqId, B, K, 1, ?REPL_FSM_TIMEOUT,
                                       self()]).

wait_for_response(ReqId, Verb) ->
    receive
        {ReqId, {error, Reason}} ->
            lager:debug("Failed to ~s replicated object: ~p", [Verb, Reason]);
        {ReqId, _} ->
            ok
    after 60000 ->
            lager:warning("Timed out after 1 minute doing ~s on replicated object",
            [Verb]),
            ok
    end.

repl_helper_recv(Object) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> ok;
        {ok, Mods} ->
            repl_helper_recv(Mods, Object)
    end.

repl_helper_recv([], _O) ->
    ok;
repl_helper_recv([{App, Mod}|T], Object) ->
    try Mod:recv(Object) of
        ok ->
            repl_helper_recv(T, Object);
        cancel ->
            cancel;
        Other ->
            lager:error("Unexpected result running repl recv helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_recv(T, Object)
    catch
        What:Why ->
            lager:error("Crash while running repl recv helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_recv(T, Object)
    end.

repl_helper_send(Object, C) ->
    B = riak_object:bucket(Object),
    case proplists:get_value(repl, C:get_bucket(B)) of
        Val when Val==true; Val==fullsync; Val==both ->
            case application:get_env(riak_core, repl_helper) of
                undefined -> [];
                {ok, Mods} ->
                    repl_helper_send(Mods, Object, C, [])
            end;
        _ ->
            lager:debug("Repl disabled for bucket ~p", [B]),
            cancel
    end.

repl_helper_send([], _O, _C, Acc) ->
    Acc;
repl_helper_send([{App, Mod}|T], Object, C, Acc) ->
    try Mod:send(Object, C) of
        Objects when is_list(Objects) ->
            repl_helper_send(T, Object, C, Objects ++ Acc);
        ok ->
            repl_helper_send(T, Object, C, Acc);
        cancel ->
            cancel;
         Other ->
            lager:error("Unexpected result running repl send helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_send(T, Object, C, Acc)
    catch
        What:Why ->
            lager:error("Crash while running repl send helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_send(T, Object, C, Acc)
    end.

repl_helper_send_realtime(Object, C) ->
    case application:get_env(riak_core, repl_helper) of
        undefined -> [];
        {ok, Mods} ->
            repl_helper_send_realtime(Mods, Object, C, [])
    end.

repl_helper_send_realtime([], _O, _C, Acc) ->
    Acc;
repl_helper_send_realtime([{App, Mod}|T], Object, C, Acc) ->
    try Mod:send_realtime(Object, C) of
        Objects when is_list(Objects) ->
            repl_helper_send_realtime(T, Object, C, Objects ++ Acc);
        ok ->
            repl_helper_send_realtime(T, Object, C, Acc);
        cancel ->
            cancel;
         Other ->
            lager:error("Unexpected result running repl realtime send helper "
                "~p from application ~p : ~p",
                [Mod, App, Other]),
            repl_helper_send_realtime(T, Object, C, Acc)
    catch
        What:Why ->
            lager:error("Crash while running repl realtime send helper "
                "~p from application ~p : ~p:~p",
                [Mod, App, What, Why]),
            repl_helper_send_realtime(T, Object, C, Acc)
    end.


site_root_dir(Site) ->
    {ok, DataRootDir} = application:get_env(riak_repl, data_root),
    SitesRootDir = filename:join([DataRootDir, "sites"]),
    filename:join([SitesRootDir, Site]).

ensure_site_dir(Site) ->
    ok = filelib:ensure_dir(
           filename:join([riak_repl_util:site_root_dir(Site), ".empty"])).

binpack_bkey({B, K}) ->
    SB = size(B),
    SK = size(K),
    <<SB:32/integer, B/binary, SK:32/integer, K/binary>>.

binunpack_bkey(<<SB:32/integer,B:SB/binary,SK:32/integer,K:SK/binary>>) -> 
    {B,K}.


merkle_filename(WorkDir, Partition, Type) ->
    case Type of
        ours ->
            Ext=".merkle";
        theirs ->
            Ext=".theirs"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

keylist_filename(WorkDir, Partition, Type) ->
    case Type of
        ours ->
            Ext=".ours.sterm";
        theirs ->
            Ext=".theirs.sterm"
    end,
    filename:join(WorkDir,integer_to_list(Partition)++Ext).

%% Returns true if the IP address given is a valid host IP address
valid_host_ip(IP) ->     
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:foldl(
        fun({_IF, Attrs}, Match) ->
                case lists:member({addr, NormIP}, Attrs) of
                    true ->
                        true;
                    _ ->
                        Match
                end
        end, false, IFs).

%% Convert IP address the tuple form
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when is_tuple(IP) ->
    {ok, IP}.

format_socketaddrs(Socket, Transport) ->
    {ok, {LocalIP, LocalPort}} = Transport:sockname(Socket),
    {ok, {RemoteIP, RemotePort}} = Transport:peername(Socket),
    lists:flatten(io_lib:format("~s:~p-~s:~p", [inet_parse:ntoa(LocalIP),
                                                LocalPort,
                                                inet_parse:ntoa(RemoteIP),
                                                RemotePort])).

maybe_use_ssl() ->
    SSLOpts = [
        {certfile, app_helper:get_env(riak_repl, certfile, undefined)},
        {keyfile, app_helper:get_env(riak_repl, keyfile, undefined)},
        {cacerts, load_certs(app_helper:get_env(riak_repl, cacertdir, undefined))},
        {depth, app_helper:get_env(riak_repl, ssl_depth, 1)},
        {verify_fun, {fun verify_ssl/3,
                get_my_common_name(app_helper:get_env(riak_repl, certfile,
                        undefined))}},
        {verify, verify_peer},
        {fail_if_no_peer_cert, true},
        {secure_renegotiate, true} %% both sides are erlang, so we can force this
    ],
    Enabled = app_helper:get_env(riak_repl, ssl_enabled, false) == true,
    case validate_ssl_config(Enabled, SSLOpts) of
        true ->
            SSLOpts;
        {error, Reason} ->
            lager:error("Error, invalid SSL configuration: ~s", [Reason]),
            false;
        false ->
            %% not all the SSL options are configured, use TCP
            false
    end.

validate_ssl_config(false, _) ->
    %% ssl is disabled
    false;
validate_ssl_config(true, []) ->
    %% all options validated
    true;
validate_ssl_config(true, [{certfile, CertFile}|Rest]) ->
    case filelib:is_regular(CertFile) of
        true ->
            validate_ssl_config(true, Rest);
        false ->
            {error, lists:flatten(io_lib:format("Certificate ~p is not a file",
                                                [CertFile]))}
    end;
validate_ssl_config(true, [{keyfile, KeyFile}|Rest]) ->
    case filelib:is_regular(KeyFile) of
        true ->
            validate_ssl_config(true, Rest);
        false ->
            {error, lists:flatten(io_lib:format("Key ~p is not a file",
                                                [KeyFile]))}
    end;
validate_ssl_config(true, [{cacerts, CACerts}|Rest]) ->
    case CACerts of
        undefined ->
            {error, lists:flatten(
                    io_lib:format("CA cert dir ~p is invalid",
                                  [app_helper:get_env(riak_repl, cacertdir,
                                                      undefined)]))};
        [] ->
            {error, lists:flatten(
                    io_lib:format("Unable to load any CA certificates from ~p",
                                  [app_helper:get_env(riak_repl, cacertdir,
                                                      undefined)]))};
        Certs when is_list(Certs) ->
            validate_ssl_config(true, Rest)
    end;
validate_ssl_config(true, [_|Rest]) ->
    validate_ssl_config(true, Rest).

upgrade_client_to_ssl(Socket) ->
    case maybe_use_ssl() of
        false ->
            {error, no_ssl_config};
        Config ->
            ssl:connect(Socket, Config)
    end.

load_certs(undefined) ->
    undefined;
load_certs(CertDir) ->
    case file:list_dir(CertDir) of
        {ok, Certs} ->
            load_certs(lists:map(fun(Cert) -> filename:join(CertDir, Cert)
                    end, Certs), []);
        {error, _} ->
            undefined
    end.

load_certs([], Acc) ->
    lager:debug("Successfully loaded ~p CA certificates", [length(Acc)]),
    Acc;
load_certs([Cert|Certs], Acc) ->
    case filelib:is_dir(Cert) of
        true ->
            load_certs(Certs, Acc);
        _ ->
            lager:debug("Loading certificate ~p", [Cert]),
            load_certs(Certs, load_cert(Cert) ++ Acc)
    end.

load_cert(Cert) ->
    {ok, Bin} = file:read_file(Cert),
    case filename:extension(Cert) of
        ".der" ->
            %% no decoding necessary
            [Bin];
        _ ->
            %% assume PEM otherwise
            Contents = public_key:pem_decode(Bin),
            [DER || {Type, DER, Cipher} <- Contents, Type == 'Certificate', Cipher == 'not_encrypted']
    end.

%% Custom SSL verification function for checking common names against the
%% whitelist.
verify_ssl(_, {bad_cert, _} = Reason, _) ->
    {fail, Reason};
verify_ssl(_, {extension, _}, UserState) ->
    {unknown, UserState};
verify_ssl(_, valid, UserState) ->
    %% this is the check for the CA cert
    {valid, UserState};
verify_ssl(_, valid_peer, undefined) ->
    lager:error("Unable to determine local certificate's common name"),
    {fail, bad_local_common_name};
verify_ssl(Cert, valid_peer, MyCommonName) ->

    CommonName = get_common_name(Cert),

    case string:to_lower(CommonName) == string:to_lower(MyCommonName) of
        true ->
            lager:error("Peer certificate's common name matches local "
                "certificate's common name"),
            {fail, duplicate_common_name};
        _ ->
            case validate_common_name(CommonName,
                    app_helper:get_env(riak_repl, peer_common_name_acl, "*")) of
                {true, Filter} ->
                    lager:info("SSL connection from ~s granted by ACL ~s",
                        [CommonName, Filter]),
                    {valid, MyCommonName};
                false ->
                    lager:error("SSL connection from ~s denied, no matching ACL",
                        [CommonName]),
                    {fail, no_acl}
            end
    end.

%% read in the configured 'certfile' and extract the common name from it
get_my_common_name(undefined) ->
    undefined;
get_my_common_name(CertFile) ->
    case catch(load_cert(CertFile)) of
        [CertBin|_] ->
            OTPCert = public_key:pkix_decode_cert(CertBin, otp),
            get_common_name(OTPCert);
        _ ->
            undefined
    end.

%% get the common name attribute out of an OTPCertificate record
get_common_name(OTPCert) ->
    %% You'd think there'd be an easier way than this giant mess, but I
    %% couldn't find one.
    {rdnSequence, Subject} = OTPCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject,
    [Att] = [Attribute#'AttributeTypeAndValue'.value || [Attribute] <- Subject,
        Attribute#'AttributeTypeAndValue'.type == ?'id-at-commonName'],
    case Att of
        {printableString, Str} -> Str;
        {utf8String, Bin} -> binary_to_list(Bin)
    end.

%% Validate common name matches one of the configured filters. Filters can
%% have at most one '*' wildcard in the leftmost component of the hostname.
validate_common_name(_, []) ->
    false;
validate_common_name(_, "*") ->
    {true, "*"};
validate_common_name(CN, [Filter|Filters]) ->
    T1 = string:tokens(string:to_lower(CN), "."),
    T2 = string:tokens(string:to_lower(Filter), "."),
    case length(T1) == length(T2) of
        false ->
            validate_common_name(CN, Filters);
        _ ->
            case hd(T2) of
                "*" ->
                    case tl(T1) == tl(T2) of
                        true ->
                            {true, Filter};
                        _ ->
                            validate_common_name(CN, Filters)
                    end;
                _ ->
                    case T1 == T2 of
                        true ->
                            {true, Filter};
                        _ ->
                            validate_common_name(CN, Filters)
                    end
            end
    end.

%% Choose the common strategy closest to the start of both side's list of
%% preferences. We can't use a straight list comprehension here because for
%% the inputs [a, b, c] and [c, b, a] we want the output to be 'b'. If a LC
%% like `[CS || CS <- ClientStrats, SS <- ServerStrats, CS == SS]' was used,
%% you'd get 'a' or 'c', depending on which list was first.
%%
%% Instead, we assign a 'weight' to each strategy, which is calculated as two
%% to the power of the index in the list, so you get something like this:
%% `[{a,2}, {b,4}, {c,8}]' and `[{c,2}, {b,4}, {a, 8}]'. When we run them
%% through the list comprehension we get: `[{a,10},{b,8},{c,10}]'. The entry
%% with the *lowest* weight is the one that is the common element closest to
%% the beginning in both lists, and thus the one we want. A power of two is
%% used so that there is always one clear winner, simple index weighting will
%% often give the same score to several entries.
choose_strategy(ServerStrats, ClientStrats) ->
    %% find the first common strategy in both lists
    CalcPref = fun(E, Acc) ->
            Index = length(Acc) + 1,
            Preference = math:pow(2, Index),
            [{E, Preference}|Acc]
    end,
    LocalPref = lists:foldl(CalcPref, [], ServerStrats),
    RemotePref = lists:foldl(CalcPref, [], ClientStrats),
    %% sum the common strategy preferences together
    TotalPref = [{S, Pref1+Pref2} || {S, Pref1} <- LocalPref, {RS, Pref2} <- RemotePref, S == RS],
    case TotalPref of
        [] ->
            %% no common strategies, force legacy
            ?LEGACY_STRATEGY;
        _ ->
            %% sort and return the first one
            element(1, hd(lists:keysort(2, TotalPref)))
    end.

strategy_module(Strategy, server) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_server"]));
strategy_module(Strategy, client) ->
    list_to_atom(lists:flatten(["riak_repl_", atom_to_list(Strategy),
                "_client"])).

%% set some common socket options, based on appenv
configure_socket(Transport, Socket) ->
    RB = case app_helper:get_env(riak_repl, recbuf) of
        RecBuf when is_integer(RecBuf), RecBuf > 0 ->
            [{recbuf, RecBuf}];
        _ ->
            []
    end,
    SB = case app_helper:get_env(riak_repl, sndbuf) of
        SndBuf when is_integer(SndBuf), SndBuf > 0 ->
            [{sndbuf, SndBuf}];
        _ ->
            []
    end,

    SockOpts = RB ++ SB,
    case SockOpts of
        [] ->
            ok;
        _ ->
            Transport:setopts(Socket, SockOpts)
    end.

%% send a start_fullsync to the calling process when it is time for fullsync
schedule_fullsync() ->
    schedule_fullsync(self()).

schedule_fullsync(Pid) ->
    case application:get_env(riak_repl, fullsync_interval) of
        {ok, disabled} ->
            ok;
        {ok, FullsyncIvalMins} ->
            FullsyncIval = timer:minutes(FullsyncIvalMins),
            erlang:send_after(FullsyncIval, Pid, start_fullsync)
    end.

%% Work out the elapsed time in seconds, rounded to centiseconds.
elapsed_secs(Then) ->
    CentiSecs = timer:now_diff(now(), Then) div 10000,
    CentiSecs / 100.0.

shuffle_partitions(Partitions, Seed) ->
    lager:info("Shuffling partition list using seed ~p", [Seed]),
    random:seed(Seed),
    [Partition || {Partition, _} <-
        lists:keysort(2, [{Key, random:uniform()} || Key <- Partitions])].

%% Parse the version into major, minor, micro digits, ignoring any release
%% candidate suffix
parse_vsn(Str) ->
    Toks = string:tokens(Str, "."),
    Vsns = [begin
                {I,_R} = string:to_integer(T),
                I
            end || T <- Toks],
    list_to_tuple(Vsns).

