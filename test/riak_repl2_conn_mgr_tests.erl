-module(riak_repl2_conn_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

-export([test1service/4, test2service/4, connected/5, connect_failed/3]).

%% host service functions
test1service(_Socket, _Transport, {Proto, MyVer, RemoteVer}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?debugFmt("started test1service mine = ~p, remote = ~p", [MyVer, RemoteVer]),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == test1proto),
    timer:sleep(2000),
    {ok, self()}.

test2service(_Socket, _Transport, {Proto, MyVer, RemoteVer}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
%%    ?debugFmt("started test2service mine = ~p, remote = ~p", [MyVer, RemoteVer]),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == test1proto),
    timer:sleep(2000),
    {ok, self()}.

%% client connection callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
%%    ?debugFmt("client connected on {~p,~p} with args ~p, mine = ~p remote = ~p",
%%              [_IP, _Port, Args, MyVer, RemoteVer]),
    ?assert(Proto == test1proto),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({Proto,_Vers}, {error, Reason}, Args) ->
    ?debugFmt("connect_failed: Proto = ~p, error = ~p", [Proto, Reason]),
    ?assert(Args == some_args),
    ?assert(Proto == test1protoFailed).


handshake_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    Port = 10365,
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{test1proto, [{2,1}, {1,0}]}, ?MODULE, test1service, [{1,0}, {1,1}]},
                    {{test2proto, [{3,4}]}, ?MODULE, test2service, []}
                   ],
    riak_repl2_conn_mgr:start_dispatcher({IP,Port}, MaxListeners, TcpOptions, SubProtocols),

    %% try to connect via a client that speaks 0.1 and 1.1
    ClientProtocol = {test1proto, [{0,1},{1,1}]},
    riak_repl2_conn_mgr:connect({IP,Port}, ClientProtocol, TcpOptions, {?MODULE, [{1,1},{1,0}]}),

    timer:sleep(2000),
    application:stop(ranch),
    ok.

failed_protocol_match_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    Port = 10364,
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{test1protoFailed, [{2,1}, {1,0}]}, ?MODULE, test1service, [{1,0}, {1,1}]}
                   ],
    riak_repl2_conn_mgr:start_dispatcher({IP,Port}, MaxListeners, TcpOptions, SubProtocols),

    %% try to connect via a client that speaks 0.1 and 3.1. No Match with host!
    ClientProtocol = {test1protoFailed, [{0,1},{3,1}]},
    riak_repl2_conn_mgr:connect({IP,Port}, ClientProtocol, TcpOptions, {?MODULE, some_args}),

    timer:sleep(2000),
    application:stop(ranch),
    ok.
