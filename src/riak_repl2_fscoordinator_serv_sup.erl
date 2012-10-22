-module(riak_repl2_fscoordinator_serv_sup).
-behavior(supervisor).

-export([init/1]).

-export([start_link/0, start_child/3]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Socket, Transport, Proto) ->
    supervisor:start_child(?MODULE, [Socket, Transport, Proto]).

init(_) ->
    ChildSpec = {id, {riak_repl2_fscoordinator_serv, start_link, []},
        transient, brutal_kill, worker, [riak_repl2_fscoordinator_serv]},
    {ok, {{simple_one_for_one, 10, 10}, [ChildSpec]}}.
