%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Quickcheck test module for `riak_core_cluster_conn'.

-module(riak_core_cluster_conn_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

%% eqc properties
-export([prop_cluster_conn_state_transition/0]).

%% States
-export([connecting/1,
         waiting_for_cluster_name/1,
         waiting_for_cluster_members/1,
         connected/1,
         stopped/1]).

%% eqc_fsm callbacks
-export([initial_state/0,
         initial_state_data/0,
         next_state_data/5,
         precondition/4,
         postcondition/5]).

%% Helpers
-export([test/0,
         test/1]).

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-define(TEST_ITERATIONS, 500).
-define(TEST_MODULE, riak_core_cluster_conn).
-define(CLUSTER_NAME, "FARFARAWAY").
-define(ERROR_MSG, "FBADBEEF").

-define(EQ(Lhs, Rhs), PPP = Lhs =:= Rhs, case PPP of true -> ok; _ -> io:format(user, "EQ ~p =/= ~p at line ~p\n", [Lhs, Rhs, ?LINE]) end, PPP).
-define(NEQ(Lhs, Rhs), PPP = Lhs =/= Rhs, case PPP of true -> ok; _ -> io:format(user, "NEQ ~p =:= ~p at line ~p\n", [Lhs, Rhs, ?LINE]) end, PPP).

-type cluster_member_1_0_type() :: {inet:ip_address(), inet:port_number()}.
-type cluster_member_1_1_type() :: {node(), {cluster_member_1_0_type()}}.
-type cluster_member_type() :: cluster_member_1_0_type() | cluster_member_1_1_type().

-record(ccst_state, {fsm_pid :: pid(),
                     fsm_protocol_version :: { pos_integer(), pos_integer()},
                     cluster_members = [] :: cluster_member_type()}).

%% Generators
-define(protocol_versions, [{1, 0}, {1, 1}, {0, 1}, {2, 0}]). % 1.0 and 1.1 are known at this time

protocol_version() ->
    elements(?protocol_versions).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {spawn,
      [
       {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(30, ?QC_OUT(prop_cluster_conn_state_transition()))))}
      ]
     }}.

setup() ->
    riak_repl_test_util:start_lager().

cleanup(Apps) ->
    riak_repl_test_util:stop_apps(Apps).

%% ====================================================================
%% EQC Properties
%% ====================================================================

%% This property is designed to exercise the state transitions of
%% `riak_core_cluster_conn'. The goal is to ensure that the reception
%% of expected and unexpected events results in the expected state
%% transition behavior. It is not intended to verify any of the side
%% effects that may occur in any particular state and those are
%% avoided by starting the fsm in `test' mode.
prop_cluster_conn_state_transition() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?FORALL(ProtocolVersion, protocol_version(),
            begin
                {H, {_F, _S}, Res} =
                    run_commands(?MODULE, Cmds, [{protocol_version, ProtocolVersion}]),
                          aggregate(zip(state_names(H), command_names(Cmds)),
                          ?WHENFAIL(
                             begin
                                 ?debugFmt("\nCmds: ~p~n",
                                           [zip(state_names(H),
                                                command_names(Cmds))]),
                                 ?debugFmt("\nResult: ~p~n", [Res]),
                                 ?debugFmt("\nHistory: ~p~n", [H])
                             end,
                             equals(ok, Res)))
            end)).

%%====================================================================
%% eqc_fsm callbacks
%%====================================================================

initiating_connection(#ccst_state{fsm_pid = FsmPid}) ->
    [
     {history, {call, ?MODULE, poll_cluster, [FsmPid]}},
     {history, {call, ?MODULE, garbage_event, [FsmPid]}},
     {history, {call, ?TEST_MODULE, status, [FsmPid]}},
     {connecting, {call, ?MODULE, send_timeout, [FsmPid]}}
    ].

connecting(#ccst_state{fsm_pid = FsmPid, fsm_protocol_version = ProtocolVersion}) ->
    [
     {history, {call, ?MODULE, poll_cluster, [FsmPid]}},
     {history, {call, ?MODULE, garbage_event, [FsmPid]}},
     {history, {call, ?TEST_MODULE, status, [FsmPid]}},
     {waiting_for_cluster_name, {call, ?MODULE, connected_to_remote, [FsmPid, ProtocolVersion]}},
     {stopped, {call, ?MODULE, send_timeout, [FsmPid]}},
     {stopped, {call, ?MODULE, send_connection_failed, [FsmPid, ?ERROR_MSG]}}
    ].

waiting_for_cluster_name(#ccst_state{fsm_pid = FsmPid}) ->
    [
     {history, {call, ?MODULE, poll_cluster, [FsmPid]}},
     {history, {call, ?MODULE, garbage_event, [FsmPid]}},
     {history, {call, ?TEST_MODULE, status, [FsmPid]}},
     {waiting_for_cluster_members, {call, ?MODULE, cluster_name, [FsmPid]}}
    ].

waiting_for_cluster_members(#ccst_state{fsm_pid = FsmPid,
                                        fsm_protocol_version = ProtocolVersion,
                                        cluster_members = ExistingMembers}) ->
    [
     {history, {call, ?MODULE, poll_cluster, [FsmPid]}},
     {history, {call, ?MODULE, garbage_event, [FsmPid]}},
     {history, {call, ?TEST_MODULE, status, [FsmPid]}},
     {connected, {call, ?MODULE, send_update_cluster_members, [FsmPid, 5, ProtocolVersion, ExistingMembers]}},
     {connected, {call, ?MODULE, send_cluster_members, [FsmPid, 3, ProtocolVersion, ExistingMembers]}}
    ].

connected(#ccst_state{fsm_pid = FsmPid, fsm_protocol_version = _ProtocolVersion}) ->
    [
     {history, {call, ?MODULE, garbage_event, [FsmPid]}},
     {history, {call, ?TEST_MODULE, status, [FsmPid]}},
     {waiting_for_cluster_name, {call, ?MODULE, poll_cluster, [FsmPid]}},
     {stopped, {call, ?TEST_MODULE, stop, [FsmPid]}}
    ].

stopped(_S) ->
    [
      %% as though _sup is present, restart
      {initiating_connection, { call, ?MODULE, start_link, [{var, protocol_version}] }}
    ].

initial_state() ->
    stopped.

initial_state_data() ->
    #ccst_state{}.

next_state_data(stopped, initiating_connection, S, R, { call, ?MODULE, start_link, [ProtocolVersion] }) ->
    S#ccst_state{fsm_pid=R, fsm_protocol_version = ProtocolVersion};
next_state_data(waiting_for_cluster_members, connected, S, R, {call, ?MODULE, send_cluster_members, [_Pid, _Count, _ProtocolVersion]}) ->
    S#ccst_state{cluster_members=R};
next_state_data(_, stopped, S, _R, _C) ->
    S#ccst_state{fsm_pid=undefined};
next_state_data(_From, _To, S, _R, _C) ->
    S.

precondition(connecting, stopped, #ccst_state{fsm_pid = FsmPid}, _C) ->
    FsmPid /= undefined;
precondition(_From, _To, _S, _C) ->
    true.

postcondition(initiating_connection, connecting, _S, {call, ?MODULE, send_timeout, [Pid]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), connecting);
postcondition(connected, connected, _S ,{call, ?MODULE, status, _}, R) ->
    ExpectedStatus = {fake_socket,
                      ranch_tcp,
                      "overtherainbow",
                      [{clustername, ?CLUSTER_NAME}],
                      {1,0}},
    {_, status, Status} = R,
    ?EQ(Status, ExpectedStatus);
postcondition(State, State, _S ,{call, ?MODULE, status, [Pid]}, R) ->
    A0 = ?EQ(R, State),
    A1 = ?EQ(?MODULE:current_fsm_state(Pid), State),
    A0 andalso A1;
postcondition(State, State, _S ,{call, ?MODULE, garbage_event, [Pid]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), State);
postcondition(connected, waiting_for_cluster_name, _S, {call, ?MODULE, poll_cluster, [Pid]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), waiting_for_cluster_name);
postcondition(State, State, _S, {call, ?MODULE, poll_cluster, [Pid]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), State);
postcondition(connecting, waiting_for_cluster_name, _S, {call, ?MODULE, connected_to_remote, [Pid, _ProtoVersion]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), waiting_for_cluster_name);
postcondition(waiting_for_cluster_name, waiting_for_cluster_members, _S, {call, ?MODULE, cluster_name, [Pid]}, _R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), waiting_for_cluster_members);
postcondition(waiting_for_cluster_members, connected, _S, {call, ?MODULE, send_cluster_members, [Pid, _Count, _ProtocolVersion, ExistingMembers]}, R) ->
    ?EQ(?MODULE:current_fsm_state(Pid), connected),
    assert_existing_members_present(ExistingMembers, R),
    assert_existing_members_present(R, ?MODULE:current_fsm_members(Pid));
postcondition(connected, stopped, _S ,{call, ?TEST_MODULE, stop, [Pid]}, _R) ->
    ?EQ(is_process_alive(Pid), false);
postcondition(connected, stopped, _S ,{call, ?MODULE, send_timeout, [Pid]}, _R) ->
    ?EQ(is_process_alive(Pid), false);
%% Catch all
postcondition(_From, _To, _S , _C, _R) ->
    true.

%%====================================================================
%% Helpers
%%====================================================================

test() ->
    test(500).

test(Iterations) ->
    eqc:quickcheck(eqc:numtests(Iterations, prop_cluster_conn_state_transition())).

start_link(_ProtocolVersion) ->
    process_flag(trap_exit, true),
    {ok, Pid} = ?TEST_MODULE:start_link(?CLUSTER_NAME, test),
    Pid.

current_fsm_state(Pid) ->
    {CurrentState, _} = ?TEST_MODULE:current_state(Pid),
    CurrentState.

current_fsm_members(Pid) ->
    {current_members, Members} = ?TEST_MODULE:current_members(Pid),
    Members.

send_timeout(Pid) ->
    gen_fsm:send_event(Pid, timeout).

send_connection_failed(Pid, Error) ->
    gen_fsm:send_event(Pid, {connect_failed, Error}).

poll_cluster(Pid) ->
    gen_fsm:send_event(Pid, poll_cluster).

garbage_event(Pid) ->
    gen_fsm:send_event(Pid, slartibartfast).

connected_to_remote(Pid, ProtoVersion) ->
    Event = {connected_to_remote,
             fake_socket,
             ranch_tcp,
             "overtherainbow",
             [{clustername, ?CLUSTER_NAME}],
             ProtoVersion},
    gen_fsm:send_event(Pid, Event).

cluster_name(Pid) ->
    Event = {cluster_name, ?CLUSTER_NAME},
    gen_fsm:send_event(Pid, Event).

map_cluster_member_to_v10({_Node, {_IP, _Port}} = Member) ->
    element(2, Member);
map_cluster_member_to_v10(Member) ->
    Member.

map_cluster_members_to_v10([{_Node, {_IP, _Port}}|_T] = Members) ->
    [map_cluster_member_to_v10(El) || El <- Members];
map_cluster_members_to_v10([{_IP, _Port}|_T] = Members) ->
    Members;
map_cluster_members_to_v10(_) ->
    [].

create_cluster_members(Count, {1, 0}) ->
    map_cluster_members_to_v10(create_cluster_members(Count, {1, 1}));
create_cluster_members(Count, _ProtoVersion) ->
    [{
         list_to_binary(["fake-node-", integer_to_list(I)]),
         { list_to_binary(["fake-address-", integer_to_list(I)]),
           I }
     } || I <- lists:seq(1, Count)].

event_atom({1, 0} = _ProtoVersion) ->
    cluster_members;
event_atom(_) ->
    all_cluster_members.

send_cluster_members(Pid, Count, ProtoVersion, ExistingMembers) ->
    send_cluster_members(Pid, Count, ProtoVersion, event_atom(ProtoVersion), ExistingMembers).

send_cluster_members(Pid, Count, ProtoVersion, EventAtom, ExistingMembers) ->
    Members = create_cluster_members(Count, ProtoVersion),
    Event = {EventAtom, Members},
    gen_fsm:send_event(Pid, Event),
    SortedNew = ordsets:from_list(Members),
    lists:filter(fun(Mem) ->
                     not ordsets:is_element(Mem, SortedNew)
                 end,
                 ExistingMembers) ++ lists:reverse(Members).

send_update_cluster_members(Pid, Count, {1, 0} = ProtoVersion, _ExistingMembers) ->
    Members = create_cluster_members(Count, ProtoVersion),
    gen_fsm:send_event(Pid, {event_atom(ProtoVersion), Members}),
    Members;
send_update_cluster_members(Pid, Count, ProtoVersion, ExistingMembers) ->
    Members0 = create_cluster_members(Count, ProtoVersion),
    Members = lists:map(fun({Node, {_IP, _Port}} = M) ->
                            case lists:keyfind(Node, 1, ExistingMembers) of
                                {_Node, {IP, _Port}} ->
                                    {Node, {IP, undefined}};
                                _ ->
                                    M
                            end
                        end, Members0),
    GarbageMember = {"garbage-node", { "garbage-ip", undefined }},
    gen_fsm:send_event(Pid, {event_atom(ProtoVersion), [GarbageMember|Members]}),
    Members.

assert_existing_members_present(ExistingMembers, NewMembers) ->
    NotCarriedMembers = ExistingMembers -- NewMembers,
    ?EQ([], NotCarriedMembers).

-endif.
