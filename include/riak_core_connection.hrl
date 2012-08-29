%% Riak Core Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

%% handshake messages to safely initiate a connection. Let's not accept
%% a connection to a telnet session by accident!
-define(CTRL_HELLO, <<"riak-ctrl:hello">>).
-define(CTRL_ACK, <<"riak-ctrl:ack">>).

-define(CONNECTION_SETUP_TIMEOUT, 60000).

-type(ip_addr_str() :: string()).
-type(ip_portnum() :: non_neg_integer()).
-type(ip_addr() :: {ip_addr_str(), ip_portnum()}).

-type(proto_id() :: atom()).
-type(rev() :: non_neg_integer()). %% major or minor revision number
-type(proto() :: {proto_id(), {rev(), rev()}}). %% e.g. {realtime_repl, 1, 0}
-type(protoprefs() :: {proto_id(), [{rev(), rev()}]}).

%% Function = fun(Socket, Transport, Protocol, Args) -> ok
%% Protocol :: proto()
-type(service_started_callback() :: fun((inet:socket(), module(), proto(), [any()]) -> no_return())).
-type(protospec() :: {protoprefs(), module(), service_started_callback(), [any()]}).
