%% Riak EnterpriseDS
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_fullsync_expiry_hook).
%%
%% @doc 
%% gen_server implentation of a callback hook to block fullsync sending expired keys
%% on backends that are configured for expiration. 
%%
%% At startup, check to see if any backends are configured for expiration. If not, 
%% exit. This means this gen_server cannot be configured on the fly -- the system
%% must be re-started in order for new backends to be looked at.
%%
%% Resgisters with repl_helper as a 'send' callback in riak_repl_util:repl_helper_send/2
%% which runs all callbacks and sends replications only if all participants send 'ok'.
%%
-behaviour(gen_server).
 
-export([recv/1,send/2,send_realtime/2]).
 
-export([start/0,start_link/0,init/1]).
 
-export([handle_call/3,handle_info/2,handle_cast/2,code_change/3,terminate/2]).
 
-export([show_config/0]).
 
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% proplist of {backend, {app_env section name, expiry key name, unit}} 
%% for each expiry-enabled backend
%% unit should be the number of seconds
%% i.e. 1 for seconds, 60 for minutes, 3600 for hours, etc.
-define(DEFAULT_BACKENDS, [
                             {riak_kv_bitcask_backend, {bitcask, expiry_secs, 1}},
                             {riak_kv_tower_backend, {hanoidb, expiry_secs, 1}}
                           ]). 
 
start() ->
        gen_server:start({local, ?MODULE}, ?MODULE, [], []).
 
start_link() ->
        gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
 
 
init(_Args) -> 
    case get_config() of
        no_expiry -> 
           lager:log(debug,self(),"No backends expire - stopping gen_server ~p",[?MODULE]),
           {stop, normal}; 
        Conf when is_list(Conf) -> 
           lager:log(debug,self(),"Expiry settings: ~p",[Conf]),
           lager:log(debug,self(),"Installing expiry repl helper"),
           riak_core:register([{repl_helper, ?MODULE}]),
           {ok, Conf}
    end.
 
show_config() ->
    gen_server:call(?MODULE,config).
 
handle_call({expiry,Name},_From,State) ->
    {reply,proplists:get_value(Name,State),State};
handle_call(config,_From,State) ->
    {reply,State,State};
 
%%stub out the rest of the gen_server
handle_call(_Msg,_From,State) -> {reply, undefined, State}.
handle_info(_Info,State) -> {noreply, State}.
handle_cast(_Req, State) -> {noreply, State}.
 
code_change(_Oldvsn,State,Extra) when is_function(Extra,1) ->
    {ok, Extra(State)};
code_change(_Oldvsn,State,_Extra) -> 
    {ok,State}.
 
terminate(_Reason,_State) -> ok.
 
 
%% repl_helper functions 
recv(_) -> ok.
 
send_realtime(_,_) -> ok.
 
send(Robject, C) ->
    Name = case catch proplists:get_value(backend,C:get_bucket(riak_object:bucket(Robject))) of
               N when is_binary(N) -> N;
               _ -> <<"riak_repl2_fullsync_expiry_hook_default_backend">>
    end,
    case catch gen_server:call(?MODULE,{expiry,Name}) of
        Secs when is_integer(Secs) -> check_age(Robject,Secs);
        {'EXIT',{noproc,{gen_server,call,[gen_expiry_repl_helper,{expiry,Name}]}}} ->
             lager:log(debug,self(),"~s not running, attempting start",[?MODULE]),
             start(),
             case whereis(?MODULE) of
               Pid when is_pid(Pid) -> send(Robject,C);
               _ -> ok
             end;
        _ -> 
        ok
    end.
 
 
%% private functions
 
check_age(Robject,Limit) when is_integer(Limit) ->

   case catch dict:fetch(<<"X-Riak-Last-Modified">>,riak_object:get_metadata(Robject)) of 
      {M,S,_} -> 
         {NM, NS, _} = os:timestamp(),
         Age = ((NM - M) * 1000000 + (NS - S)),
         case Age > Limit of
             false -> ok;
             true -> 
                 lager:log(debug,self(),"Cancel replication of key ~p ~p due to expiry (~p/~p)",[riak_object:bucket(Robject), riak_object:key(Robject), Age, Limit]),
                 cancel
         end;
      _ -> ok
   end;
check_age(_,_) -> ok.
 
choose_default(Tab) when is_list(Tab) ->
        Name = case app_helper:get_env(riak_kv,multi_backend_default,undefined) of
                 N when is_binary(N) -> N;
                 _ -> 
                       {N, _Mod, _Conf} = hd(app_helper:get_env(riak_kv,multi_backend)),
                       N
               end,
        case proplists:get_value(Name,Tab) of
            Secs when is_integer(Secs) -> 
                 lager:log(debug,self(),"Default backend ~p expiry: ~B",[Name,Secs]),
                 [{<<"riak_repl2_fullsync_expiry_hook_default_backend">>,Secs}|Tab];
            _ -> 
                 lager:log(debug,self(),"Default backend ~p does not expire",[Name]),
                 Tab
        end;
choose_default(Tab) -> 
    Tab.
 
get_config() ->

    case app_helper:get_env(riak_kv,storage_backend) of
      riak_kv_multi_backend ->
        lager:log(debug,self(),"riak_kv,multi_backend: ~s", 
          [app_helper:get_env(riak_kv,multi_backend)]),
        lager:log(debug,self(),"?MODULE,backends: ~s", 
          [app_helper:get_env(?MODULE,backends,?DEFAULT_BACKENDS)]),

        choose_default(get_expiry(app_helper:get_env(riak_kv,multi_backend),
          app_helper:get_env(?MODULE,backends,?DEFAULT_BACKENDS)));
      Other ->
        lager:log(debug,self(),"using ~s backend",[Other]),
        get_expiry([{<<"riak_repl2_fullsync_expiry_hood_default_backend">>,Other,[]}],
          app_helper:get_env(?MODULE,backends,?DEFAULT_BACKENDS))
    end.
 
get_expiry(undefined,_) ->
    no_expiry;
get_expiry([],_) ->
    no_expiry;
get_expiry(Conf,Backends) ->
    get_expiry(Conf,[],Backends).
 
get_expiry([],[],_) -> no_expiry;
get_expiry([],Tab,_) -> Tab;
get_expiry([Backend={Name,_Mod,_Props}|Rest],Tab,Backends) ->
    lager:log(debug,self(),"considering backend ~p",[Backend]),
    case backend_expiry(Backend,Backends) of
        Secs when is_integer(Secs), Secs > 0 ->
            Tab0 = [{Name,Secs}|Tab],
            lager:log(debug,self(),"repl helper found expiry ~p Seconds for backend ~p",[Secs,Name]),
            get_expiry(Rest,Tab0,Backends); 
        Secs ->
            lager:log(debug,self(),"backend ~p does not expire(~p)",[Name,Secs]), 
            get_expiry(Rest,Tab,Backends)
    end. 
    
%% We will need to add a clause here for any other expiry-enabled backends`
backend_expiry({_Name, Mod, Plist},Backends) ->
    case proplists:get_value(Mod,Backends,undefined) of
         {Section,Key,Size} ->
		proplists:get_value(Key, Plist, proplists:get_value(Key,application:get_all_env(Section),0)) * Size;
          _ -> undefined
   end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-define(TEST_NOW_TUPLE, {1373,912890,559778}). % {{2013,7,15},{14,28,10}}

riak_repl2_fullsync_expiry_hook_test_() ->
    { setup,
      fun setup/0,
      fun cleanup/1,
      [
       fun riak_repl2_fullsync_expiry_hook_case/0
      ]
    }.

setup() ->
    process_flag(trap_exit, true),

    application:set_env(riak_kv, storage_backend, riak_kv_multi_backend),
    application:set_env(riak_kv, backends, ?DEFAULT_BACKENDS),
    application:set_env(riak_kv, 
      multi_backend, [
                 {<<"cache">>, riak_kv_bitcask_backend, [{expiry_secs, 60}]}]),

    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:start_test_ring(),
    riak_repl_test_util:abstract_gen_tcp(),
    riak_repl_test_util:abstract_stats(),
    riak_repl_test_util:abstract_stateful(),
    riak_repl2_rt_source_sink_tests:start_sink(),
    riak_repl2_rt_source_sink_tests:start_source(),
    ok.
cleanup(_Ctx) ->
    riak_repl_test_util:kill_and_wait(riak_core_tcp_mon),
    riak_repl_test_util:kill_and_wait(riak_repl2_rtq),
    riak_repl_test_util:kill_and_wait(riak_repl2_rt),
    riak_repl_test_util:stop_test_ring(),
    riak_repl_test_util:maybe_unload_mecks(
      [riak_core_service_mgr,
       riak_core_connection_mgr,
       gen_tcp, riak_core]),
    ok.

%% Test the callback -- the send() function will return 'ok' if either
%%
%% A) no backends are configured for expiration
%% B) the bucket has no expired with respect to the backend config
%%
%% otherwise it returns 'cancel'.
riak_repl2_fullsync_expiry_hook_case() ->

    mock_riak_core_register(),

    start(),

    RObj1 = create_bucket_with_mod_time(?TEST_NOW_TUPLE),
    Res1 = send(RObj1, riak_core_bucket),

    ?assertEqual(cancel, Res1),

    RObj2 = create_bucket_with_mod_time(erlang:now()),
    Res2 = send(RObj2, riak_core_bucket),

    ?assertEqual(ok, Res2).

mock_riak_core_register() ->
    catch(meck:unload(riak_core)),
    meck:new(riak_core),
    meck:expect(riak_core, register, fun(_Params) ->   
        % do nothing, just here so we can run send/2
        ok
    end).

create_bucket_with_mod_time(Time) ->
    Meta = dict:new(),
    Meta1 = dict:store(<<"X-Riak-Last-Modified">>, Time, Meta),
    Bucket = <<"cache">>,
    Key = <<"key">>,
    Bin = case get_binsize(Key) of
              undefined    -> <<>>;
              N            -> <<42:(N*8)>>
          end,
    riak_object:new(Bucket, Key, Bin, Meta1).

get_binsize(<<"cache", Rest/binary>>) ->
    get_binsize(Rest, 0);
get_binsize(_) ->
    undefined.

get_binsize(<<X:8, Rest/binary>>, Val) when $0 =< X, X =< $9->
    get_binsize(Rest, (Val * 10) + (X - $0));
get_binsize(_, Val) ->
    Val.

-endif.
