-module(riak_repl2_conn_mgr_tests).

-include_lib("eunit/include/eunit.hrl").

-export([test1service/4, test2service/4, connected/5]).

%% host service functions
test1service(_Socket, _Transport, ClientProtocol, Args) ->
    ?debugFmt("started test1service with args ~p, client speaks ~p", [Args, ClientProtocol]).

test2service(_Socket, _Transport, ClientProtocol, Args) ->
    ?debugFmt("started test2service with args ~p, client speaks ~p", [Args, ClientProtocol]).

%% client connection callbacks
connected(_Socket, _Transport, {IP, Port}, HostProtocol, Args) ->
        ?debugFmt("client connected on {~p,~p} with args ~p, host speaks ~p",
                  [IP, Port, Args, HostProtocol]).



handshake_test() ->
    %% start ranch as an application so that we have a supervision tree,
    %% otherwise ranch will crash with a noproc in gen_server call.
    application:start(ranch),
    %% local host
    IP = "127.0.0.1",
    Port = 10365,
    %% start dispatcher
    MaxListeners = 10,
    SubProtocols = [{{test1proto, [{2,1}, {1,0}]}, ?MODULE, test1service, [one]},
                    {{test2proto, [{3,4}]}, ?MODULE, test2service, [two]}
                   ],
    ?debugMsg("Starting connection manager"),
    riak_repl2_conn_mgr:start_dispatcher({IP,Port}, MaxListeners, SubProtocols),
    ?debugMsg("Started connection manager"),
    timer:sleep(1000),
    %% try to connect via a client
    Client = fun() ->
                     ClientProtocol = {test1proto, {1,1}},
                     ?debugMsg("Connecting with client"),
                     riak_repl2_conn_mgr:connect({IP,Port}, ClientProtocol, [], {?MODULE, [handshake]})
             end,
    spawn(Client),
    timer:sleep(2000),
    application:stop(ranch),
    ok.
