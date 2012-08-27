%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-define(REPL_FSM_TIMEOUT, 15000).
-define(REPL_QUEUE_TIMEOUT, 1000).
-define(REPL_MERK_TIMEOUT, infinity).
-define(REPL_CONN_RETRY, 30000).
-define(DEFAULT_REPL_PORT, 9010).
-define(NEVER_SYNCED, {0, 0, 0}).
-define(MERKLE_BUFSZ, 1048576).
-define(MERKLE_CHUNKSZ, 65536).
-define(REPL_DEFAULT_QUEUE_SIZE, 104857600).
-define(REPL_DEFAULT_MAX_PENDING, 5).
-define(REPL_DEFAULT_ACK_FREQUENCY, 5).
-define(FSM_SOCKOPTS, [{packet, 4}, {send_timeout, 300000}]).
-define(REPL_VERSION, 3).
-define(LEGACY_STRATEGY, syncv1).
-define(KEEPALIVE_TIME, 60000).
-define(PEERINFO_TIMEOUT, 60000).
-define(ELECTION_TIMEOUT, 60000).

-type(ip_addr_str() :: string()).
-type(ip_portnum() :: non_neg_integer()).
-type(repl_addr() :: {ip_addr_str(), ip_portnum()}).
-type(repl_addrlist() :: [repl_addr()]).
-type(repl_socket() :: port()).
-type(repl_sitename() :: string()).
-type(repl_sitenames() :: [repl_sitename()]).
-type(repl_ns_pair() :: {node(), repl_sitename()}).
-type(repl_ns_pairs() :: [repl_ns_pair()]).
-type(repl_np_pair() :: {repl_sitename(), pid()}).
-type(repl_np_pairs() :: [repl_np_pair()]).
-type(repl_node_sites() :: {node(), [{repl_sitename(), pid()}]}).
-type(ring() :: tuple()).
-type(repl_config() :: dict()|undefined).

-record(peer_info, {
          riak_version :: string(), %% version number of the riak_kv app
          repl_version :: string(), %% version number of the riak_kv app
          ring         :: ring()    %% instance of riak_core_ring()
         }).

-record(fsm_state, {
          socket          :: repl_socket(),   %% peer socket
          sitename        :: repl_sitename(), %% peer sitename
          my_pi           :: #peer_info{},    %% local peer_info
          client          :: tuple(),         %% riak local_client
          partitions = [] :: list(),          %% list of local partitions
          work_dir        :: string()         %% working directory 
         }).

-record(repl_listener, {
          nodename    :: atom(),     %% cluster-local node name
          listen_addr :: repl_addr() %% ip/port to bind/listen on
         }).

-record(repl_site, {
          name  :: repl_sitename(),   %% site name
          addrs=[] :: repl_addrlist(),%% list of ip/ports to connect to
          last_sync=?NEVER_SYNCED :: tuple()  
         }).

-record(nat_listener, {
          nodename    :: atom(),      %% cluster-local node name
          listen_addr :: repl_addr(), %% ip/port to bind/listen on
          nat_addr :: repl_addr()     %% ip/port that nat bind/listens to
         }).

-define(REPL_HOOK, {struct, 
                    [{<<"mod">>, <<"riak_repl_leader">>},
                     {<<"fun">>, <<"postcommit">>}]}).
