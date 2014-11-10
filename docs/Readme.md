# Supervisor hierarchy

* riak_repl_client_sup (v2 sites sup)
* riak_repl_server_sup (v2 listeners sup)
* riak_repl_leader
* riak_repl2_leader
* riak_core_cluster_mgr_sup
  * riak_core_service_mgr
  * riak_core_connection_mgr
  * riak_core_cluster_conn_sup
    * riak_core_cluster_conn
  * riak_repl_cluster_mgr
  * riak_core_tcp_mon
* riak_repl2_fs_node_reserver
* riak_repl2_rt_sup
  * riak_repl2_rtsource_sup
  * riak_repl2_rtsink_sup
    * rtsink pool
      * riak_repl_fullsync_worker ...
    * riak_repl2_rtsink_conn_sup
      * riak_repl2_rtsink_conn ...
  * riak_repl2_rt
* riak_repl2_fscoordinator_sup
  * riak_repl2_fscoordinator ...
* riak_repl2_fscoordinator_serv_sup
  * riak_repl2_fscoordinator_serv ...
* riak_repl2_fssource_sup
  * riak_repl2_fssource ...
* riak_repl2_fssink_sup
  * riak_repl2_fssink
* riak_repl2_pg_proxy_sup
  * riak_repl2_pg_proxy
* riak_repl2_pg_sup
  * riak_repl2_pg_block_provider_sup
  * riak_repl2_pg_block_requester_sup
  * riak_repl2_pg

# Replication data in the ring

The entire configuration is stored in the `riak_repl_ring` entry of the ring
meta-data.

1. [riak_repl_ring][../src/riak_repl_ring.erl]

# Module quick reference

## riak_repl_listener_sup

Not a real supervisor. Has utilities to start riak_repl_tcp_server listeners and manage their lifetime. Listeners are handled by ranch. These are the v2 source processes.

## riak_core_tcp_mon

Collects and stores connection metrics. You monitor a connection and stats about it are polled at regular intervals.

## riak_core_connection_mgr

Manages requests to connect to clusters or specific nodes and the lifetime of those connections. It has the concept of a locator, which translates a cluster name to a list of physical ip:port addresses to be tried. Connections may be retried with backoff. A helper process handles each connection and its retries (1) (2)

1. [riak_core_connection_mgr:start_request/2][]
2. [riak_core_connection_mgr:connection_helper/4][]

## riak_core_service_mgr

Replication listens for different types of connections on a single port stored in the {riak_core, cluster_mgr} env var. The service manager listens on this port and decides which service should handle an incoming connection depending on the protocol (1). It uses the ranch connection library. Different services are registered on the service manager to handle connections that that port. Notice that only the round_robin strategy is really supported (2).

It also keeps stats at the service level. Callbacks can be registered that will receive these stats as input periodically.

Incoming connections are handled by the [riak_core_service_mgr:dispatch_service/4][] ranch callback. A basic handshake exchanges version and capabilities, then the connection is possibly upgraded to SSL (on a different socket) (5). Then a protocol version is negotiated. Then we see if the requested service exists and one of the versions the client supports is supported over here. If everything goes well, a process is started to handle the connection and is registered to be monitored by the service manager (4)

1. [riak_core_service_mgr:start_link/0][]
1. [riak_core_service_mgr:register_service/2][]
1. [riak_core_service_mgr:exchange_handshakes_with/4][]
1. [riak_core_service_mgr:start_negotiated_service/4][]

## riak_repl2_fs_node_reserver

Runs on every node, taking reservations for sink processes for a full sync, making sure we don't reserve more than max configured.  Sinks claim their reservation when they start. Reservations expire after a bit.

# Console commands:

**NOTE**: start-fullsync is v2, fullsync start is v3, etc

## Enabling fullsync to a sink

Enabling fullsync to a sink first adds the sink (called remote in the code) to the repl information in the ring (1) (2), then starts a coordinator for that sink on the leader node (3). if fullsync_interval is set, a fullsync is scheduled (4).  The coordinator connects to the sink cluster using the fs_coordinate protocol (5), which will be handled on the other side by creating a riak_repl2_fscoordinator_serv process (6). As far as I can tell, this sink side process could be started on any node, not just the leader.

1. [riak_repl_console:fullsync/1][]
2. [riak_repl_ring:fs_enable_trans/2][]
3. [riak_repl2_fscoordinator_sup:start_coord/2][]
4. [riak_repl_util:schedule_cluster_fullsync/2][]
5. [riak_repl2_fscoordinator:init/1][]
6. [riak_repl2_fscoordinator_serv:sync_register_service/0][]

## Starting fullsync

Starting a fullsync to a sink cluster calls riak_repl_fscoordinator_sup on the
leader node to get the list of processes corresponding to each enabled fullsync
(to a sink), which are its children (1) (2).  If it finds it, it async sends it
the start_fullsync message (3)

When the coordinator process receives the start_fullsync message, it starts
sending `whereis` requests to the sink to locate partitions (4) (5). Partitions
are chosen to obey the limits of source/sink workers per node and cluster at
any given point (6). 

On the sink side, the riak_repl2_fscoordinator_serv process replies to these
whereis requests. It reserves slots for those partitions on the nodes they live
in, obeying the configured max on sink processes. It may return the location of
the partition if succesful or location_busy/location_down messages back (6).
The source side process keeps track of busy or unavailable nodes and tries not
to hit them for a bit. Upon receiving a location message, a riak_repl2_fssource
process is started on the node owning that partition that will connect to the
sink node where that partition is located. A worker for either the keylisting
or AAE strategy wil be created to do its thing.

1. [riak_repl_console:fullsync/1][]
2. [riak_repl2_fscoordinator_sup:started/0][]
3. [riak_repl2_fscoordinator:start_fullsync/1][]
4. [riak_repl2_fscoordinator:start_up_reqs/1][]
5. [riak_repl2_fscoordinator:send_next_wheres_req/1][]
6. [riak_repl2_fscoordinator:determine_best_partition/1][]
7. [riak_repl2_fscoordinator_serv:handle_protocol_msg/4][], the whereis clause.
8. [riak_repl2_fscoordinator:start_fssource/4][]
9. [riak_repl2_fssource:handle_call/3][], connected clause.

### Keylist fullsync

### AAE fullsync



1. [riak_repl_aae_source:start_link/7][]

## Stopping fullsync

Stopping a fullsync to a sink is similar to starting it, but we send the stop_fullsync asynchronous message to the coordinator instead of start_fullsync.

1. [riak_repl_console:fullsync/1][]
2. [riak_repl2_fscoordinator:stop_fullsync/1][]

