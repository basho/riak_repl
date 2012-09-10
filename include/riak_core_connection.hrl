%% Riak Core Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

%% handshake messages to safely initiate a connection. Let's not accept
%% a connection to a telnet session by accident!
-define(CTRL_HELLO, <<"riak-ctrl:hello">>).
-define(CTRL_ACK, <<"riak-ctrl:ack">>).
-define(CTRL_GET_NAME, <<"riak-ctrl:get_name">>).
-define(CTRL_MY_NAME, <<"riak-ctrl:my_name">>).

-define(CLUSTER_MANAGER_SERVER, riak_core_cluster_manager).
-define(CONNECTION_SETUP_TIMEOUT, 10000).
-define(CM_CALL_TIMEOUT, 2000).

-type(ip_addr_str() :: string()).
-type(ip_portnum() :: non_neg_integer()).
-type(ip_addr() :: {ip_addr_str(), ip_portnum()}).
-type(tcp_options() :: [any()]).

-type(proto_id() :: atom()).
-type(rev() :: non_neg_integer()). %% major or minor revision number
-type(proto() :: {proto_id(), {rev(), rev()}}). %% e.g. {realtime_repl, 1, 0}
-type(protoprefs() :: {proto_id(), [{rev(), rev()}]}).
-type(clustername() :: string()).
-type(cluster_finder_fun() :: fun(() -> {ok,node()} | {error, term()})).

%% Function = fun(Socket, Transport, Protocol, Args) -> ok
%% Protocol :: proto()
-type(service_started_callback() :: fun((inet:socket(), module(), proto(), [any()]) -> no_return())).

%% Host protocol spec
-type(hostspec() :: {protoprefs(), {tcp_options(), module(), service_started_callback(), [any()]}}).

%% Client protocol spec
-type(clientspec() :: {protoprefs(), {tcp_options(), module(),[any()]}}).


%% Scheduler strategies tell the connection manager how distribute the load.
%%
%% Client scheduler strategies
%% ---------------------------
%% default := service side decides how to choose node to connect to. limits number of
%%            accepted connections to max_nb_cons().
%% askme := the connection manager will call your client for a custom protocol strategy
%%          and likewise will expect that the service side has plugged the cluster
%%          manager with support for that custom strategy. UNSUPPORTED so far.
%%          TODO: add a behaviour for the client to implement that includes this callback.
%% Service scheduler strategies
%% ----------------------------
%% round_robin := choose the next available node on cluster to service request. limits
%%                the number of accepted connections to max_nb_cons().
%% custom := service must provide a strategy to the cluster manager for choosing nodes
%%           UNSUPPORTED so far. Should we use a behaviour for the service module?
-type(max_nb_cons() :: non_neg_integer()).
-type(client_scheduler_strategy() :: default | askme).
-type(service_scheduler_strategy() :: {round_robin, max_nb_cons()} | custom).
