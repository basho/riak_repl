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

Before v2, the terms 'source' and 'sink' were not used; client and server
were. The client would run on what we now term the sink, and the server
would run on the sink. Thus, we have the confusing names of the two modules
used for fullsync keylist stategy: riak_repl_keylist_client and
riak_repl_keylist_server. For consistency and clarity, the terms 'source'
and 'sink' will be used.

#### High Level

1. For each partition, start a riak_repl_fullsync_helper.
2. Each helper will fold over the keys in the given parition.
3. During the fold, hash each object.
4. Put the hashed object in a file.
5. Sink sends their keylist file to the source.
6. The two keylist files are compared and differences sent over.

#### At least shorter than the code.

Source: fssource
1. fssource signaled that connection estblished to sink.
2. Call [riak_repl_keylist_server:start_link/6][]
3. Call [riak_repl_keylist_server:start_fullsync/2][]
4. fssource waits for errors, or message from fssink 'fullsync_complete'.
5. Once fullsync complete, stops, which stops the started keylist_server.

Source: riak_repl_keylist_server:start_link/6
1. Wait for start_fullsync/2 call, this will give the partition list. In v2, this is a list of 1.
2. Create a file for the sink's keylist.
3. Create a file for source's keylist.
4. [riak_repl_fullsync_helper:start_link/1][].
5. [riak_repl_fullsync_helper:make_keylist/3][].
6. Send fullsync start with partition id to sink side over socket.
7. Wait for partition id to be sent back from sink side.
8. Wait for local keylist file to be filled.
9. Request sink's keylist for the partition.
10. On {kl_hunk, binary()} from sink, append given binary() to sink's keylist file.
11. On kl_eof, continue to next step, otherwise keep waitng for hunks.
12. [riak_repl_fullsynce_helper:start_link/1][].
13. [riak_repl_fullsync_helper:diff_stream/5].
14. For each diff message received, riak_repl_fullsync_worker:do_get/8. That reads the object using a local client, and sends it accross the wire.
15. After getting diff_done, send diff_done to sink.

Source riak_repl_fullsync_helper:make_keylist/3
1. Fold over keys on given partition into given file.
2. For each key, put a hash of the entire object into the file.
3. Once fold is complete, sort file.
4. Signal to owner fsm (given in the start_link function) that the keylist has been built.

Source riak_repl_fullsync_helper:diff_stream/5
1. Open an iterator for both the local keylist file and the remote keylist file.
2. On differeing hash or missing hash of the sinks's file, send message to riak_repl_keylist_server giving the {bucket(), key()} of the object.
3. Once all comparisons are done, send diff_done to riak_repl_keylist_server.
4. Exit normal.

Sink: fssink
1. fssink singnaled that a connection as been established, and it is now in charge.
2. [riak_repl_keylist_client:start_link/4][].
3. any message from the socket other than a fullsync diff object is fowarded to the keylist_client.
4. fullsync diff object messages are put using a raw riak_kv_put_fsm.
5. Exits normally on socket error or close.

Sink: riak_repl_keylist_client:start_link/4
1. Wait for {start_fullsync, [PartitionId]} from socket.
2. If vnode lock was not successful, exit abnormally, thus ending the sync.
3. Claim a reservation for the partion.
4. Send to the source the partition id.
5. [riak_repl_fullsync_helper:start_link/1][].
6. [riak_repl_fullsync_helper:make_keylist3][].
7. Wait for local keylist hash file to be filled and a request for our keylist.
8. Send out keylist is chunks, occasionally requesting an ack that chunks are being handled, until eof is reached.
9. Inform source that the end of the file is reached.
10. As {diff_ack, PartitionId} messages come in, reply with same on socket.
11. When diff_done comes in, return to waiting; this will end up being exited when the source (which sent the diff_done) closes the connection.


### AAE fullsync



1. [riak_repl_aae_source:start_link/7][]

## Stopping fullsync

Stopping a fullsync to a sink is similar to starting it, but we send the stop_fullsync asynchronous message to the coordinator instead of start_fullsync.

1. [riak_repl_console:fullsync/1][]
2. [riak_repl2_fscoordinator:stop_fullsync/1][]

## Realtime replication to a sink management

The core of realtime replication is the realtime queue. The Realtime queue
is called rtq for short. It is started on application start, and thus
always running.

The supervisor path to the rtq is:

    riak_repl_sup ->
        riak_repl2_rt_sup ->
            riak_repl2_rtsource_sup ->
                riak_repl2_rtq

The supervisor is a rest_for_one, with the rtq as the first in line. If the
rtq goes down, so does rtq overload protection, and the realtime source
connection supervisor.

The realtime and fullsync requirements are basically the same: both source
and sink clusters need names. The source cluster must be connected to
the sink cluster. Realtime is first enabled then started.

## What happens once realtime is enabled to a sink

Once enabled, the riak_repl2_rt applies a ring transformation to add the
remote given to the list of rt_enabled items in the ring dictionary. Adding
a sink mutliple times results in a long no-op.

After enabling the sink on the source, it can then be started. Trying to
start realtime to a sink that has not been enabled results in a long no-op.
Starting the realtime adds the remote to a list in the ring dictionary with
the name 'rt_started'.

Both the above ring transformations, assuming they changed something, get
handled in the riak_repl_ring_handler module. The realtime postcommit hook
is always installed, being a noop if it was already present.

If any realtime connections need to be started, the ring handler registers
a client to the rtq for the sink name, and calls the rtsource_conn_sup to
'enable' the realtime connection. The rtsource_conn module uses the
connection manager to start the connection.

Once the rtsource_conn is connected, a helper is started using
riak_repl2_rtsource_helper. The helper's job is to pull from the rtq
and send the object across the wire. It is also in charge of sending
heartbeats.

1. [riak_repl_console:realtime/1][]
2. [riak_repl2_rt:enable/1][]
3. [riak_repl2_rt:start/1][]
4. [riak_repl_ring_handler:handle_event/2][] - 2nd ring_update clause.
5. [riak_repl2_rt:ensure_rt/2][]
6. [riak_repl2_rtq:register/1][]
7. [riak_repl2_rtsource_conn_sup:enable/1][]
8. [riak_repl2_rtsource_conn:start_link/1][]
9. [riak_repl2_rtsouce_conn:handle_call/3][] - connected clause
10. [riak_repl2_rtsouce_helper:start_link/4][]
11. [riak_repl2_rtq:pull/2][]

