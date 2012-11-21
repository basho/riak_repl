%% Riak Core Connection Manager
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.

-include_lib("riak_core/include/riak_core_connection.hrl").

%% cluster manager messages
-define(CTRL_ASK_NAME, <<"riak-ctrl:ask_name">>).
-define(CTRL_ASK_MEMBERS, <<"riak-ctrl:ask_members">>).

-define(CLUSTER_MANAGER_SERVER, riak_core_cluster_manager).
-define(CLUSTER_MGR_SERVICE_ADDR, {"0.0.0.0", 9085}).
-define(CM_CALL_TIMEOUT, 2000).
-define(CLUSTER_NAME_LOCATOR_TYPE, cluster_by_name).
-define(CLUSTER_ADDR_LOCATOR_TYPE, cluster_by_addr).
-define(CLUSTER_PROTO_ID, cluster_mgr).

-type(clustername() :: string()).
