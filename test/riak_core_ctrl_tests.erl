-module(riak_core_ctrl_tests).

-include_lib("eunit/include/eunit.hrl").

-export([ctrlService/4, connected/5, connect_failed/3]).

%% host control service
ctrlService(_Socket, _Transport, {error, _Reason}, _Args) ->
    ?assert(false);
ctrlService(_Socket, _Transport, {ok, {Proto, MyVer, RemoteVer}}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    ?assert(Proto == repl2ctrl),
    timer:sleep(2000),
    {ok, self()}.

%% Client side protocol callbacks
connected(_Socket, _Transport, {_IP, _Port}, {Proto, MyVer, RemoteVer}, Args) ->
    [ExpectedMyVer, ExpectedRemoteVer] = Args,
    ?assert(Proto == repl2ctrl),
    ?assert(ExpectedMyVer == MyVer),
    ?assert(ExpectedRemoteVer == RemoteVer),
    timer:sleep(2000).

connect_failed({_Proto,_Vers}, {error, _Reason}, _Args) ->
    ?assert(false).

%%
open_ctrl_channel_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    CtrlPort = 6789,
    Expect = [{1,0}, {1,0}],
    %% Socket options set on both client and host. Note: binary is mandatory.
    TcpOptions = [{keepalive, true},
                  {nodelay, true},
                  {packet, 4},
                  {reuseaddr, true},
                  {active, false}],
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{repl2ctrl, [{1,0}]}, ?MODULE, ctrlService, Expect}],
    riak_repl2_connection:start_dispatcher({IP,CtrlPort}, MaxListeners, TcpOptions, SubProtocols),

    %% try to connect via a client that speaks 1.0 ctrl syntax
    ClientProtocol = {repl2ctrl, [{1,0}]},
    riak_repl2_connection:connect({IP,CtrlPort}, ClientProtocol, TcpOptions, {?MODULE, Expect}),

    timer:sleep(2000),
    application:stop(ranch),
    ok.
