%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_ring).
-author('Andy Gross <andy@andygross.org>').
-include("riak_repl.hrl").

-export([ensure_config/1,
         initial_config/0,
         get_repl_config/1,
         set_repl_config/2,
         add_site/2,
         add_site_addr/3,
         del_site_addr/3,
         get_site/2,
         del_site/2,
         add_listener/2,
         add_nat_listener/2,
         get_listener/2,
         del_listener/2,
         del_nat_listener/2,
         get_nat_listener/2]).

-include_lib("eunit/include/eunit.hrl").

-spec(ensure_config/1 :: (ring()) -> ring()).
%% @doc Ensure that Ring has replication config entry in the ring metadata dict.
ensure_config(Ring) ->
    case get_repl_config(Ring) of
        undefined ->
            riak_core_ring:update_meta(?MODULE, initial_config(), Ring);
        _ ->
            Ring
    end.

-spec(get_repl_config/1 :: (ring()) -> dict()|undefined).
%% @doc Get the replication config dictionary from Ring.
get_repl_config(Ring) ->
    case riak_core_ring:get_meta(?MODULE, Ring) of
        {ok, RC} -> RC;
        undefined -> undefined
    end.

-spec(set_repl_config/2 :: (ring(), dict()) -> ring()).
%% @doc Set the replication config dictionary in Ring.
set_repl_config(Ring, RC) ->
    riak_core_ring:update_meta(?MODULE, RC, Ring).

-spec(add_site/2 :: (ring(), #repl_site{}) -> ring()).
%% @doc Add a replication site to the Ring.
add_site(Ring, Site=#repl_site{name=Name}) ->
    RC = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    NewSites = case lists:keysearch(Name, 2, Sites) of
        false ->
            [Site|Sites];
        {value, OldSite} ->
            NewSite = OldSite#repl_site{addrs=lists:usort(
                    OldSite#repl_site.addrs ++
                    Site#repl_site.addrs)},
            [NewSite|lists:delete(OldSite, Sites)]
    end,
    riak_core_ring:update_meta(
        ?MODULE,
        dict:store(sites, NewSites, RC),
        Ring).

-spec(del_site/2 :: (ring(), repl_sitename()) -> ring()).
%% @doc Delete a replication site from the Ring.
del_site(Ring, SiteName) ->
    RC  = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false ->
            Ring;
        {value, Site} ->
            NewSites = lists:delete(Site, Sites),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(sites, NewSites, RC),
              Ring)
    end.

-spec(get_site/2 :: (ring(), repl_sitename()) -> #repl_site{}|undefined).
%% @doc Get a replication site record from the Ring.
get_site(Ring, SiteName) ->
    RC = get_repl_config(Ring),
    Sites  = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false -> undefined;
        {value, ReplSite} -> ReplSite
    end.

-spec(add_site_addr/3 :: (ring(), repl_sitename(), repl_addr()) -> ring()).
%% @doc Add a site address to connect to.
add_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=[Addr|Addrs]});
                true ->
                    Ring
            end
    end.

-spec(del_site_addr/3 :: (ring(), repl_sitename(), repl_addr()) -> ring()).
%% @doc Delete a server address from the site
del_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring;
                true ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=lists:delete(Addr, Addrs)})
            end
    end.

-spec(add_listener/2 :: (ring(), #repl_listener{}) -> ring()).
%% @doc Add a replication listener host/port to the Ring.
add_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false ->
            NewListeners = [Listener|Listeners],
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, RC),
              Ring);
        true ->
            Ring
    end.

-spec(add_nat_listener/2 :: (ring(), #nat_listener{}) -> ring()).
%% @doc Add a replication NAT listener host/port to the Ring.
add_nat_listener(Ring,NatListener) ->
    RC = get_repl_config(Ring),
    case dict:find(natlisteners,RC) of
        {ok, NatListeners} ->
            case lists:member(NatListener, NatListeners) of
            false ->
                NewListeners = [NatListener|NatListeners],
                riak_core_ring:update_meta(
                  ?MODULE,
                  dict:store(natlisteners, NewListeners, RC),
                  Ring);
            true ->
                Ring
            end;
        error ->
            %% there are no natlisteners entries yet.
                NewListeners = [NatListener],
                riak_core_ring:update_meta(
                  ?MODULE,
                  dict:store(natlisteners, NewListeners, RC),
                  Ring)
    end.

-spec(del_listener/2 :: (ring(), #repl_listener{}) -> ring()).
%% @doc Delete a replication listener from the Ring.
del_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false -> Ring;
        true ->
            NatRing = del_nat_listener(Ring,Listener),
            NewListeners = lists:delete(Listener, Listeners),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, get_repl_config(NatRing)), NatRing)
    end.

-spec(del_nat_listener/2 :: (ring(),#repl_listener{}) -> ring()).
%% @doc Delete a nat_listener from the list of nat_listeners
del_nat_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    case get_nat_listener(Ring, Listener) of
        undefined ->  Ring;
        NatListener ->
            NatListeners  = dict:fetch(natlisteners, RC),
            NewNatListeners = lists:delete(NatListener, NatListeners),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(natlisteners, NewNatListeners, RC),
              Ring)
    end.

-spec(get_listener/2 :: (ring(), repl_addr()) -> #repl_listener{}|undefined).
%% @doc Fetch a replication host/port listener record from the Ring.
get_listener(Ring,{_IP,_Port}=ListenAddr) ->
    RC = get_repl_config(Ring),
    Listeners  = dict:fetch(listeners, RC),
    case lists:keysearch(ListenAddr, 3, Listeners) of
        false -> undefined;
        {value,Listener} -> Listener
    end.

-spec(get_nat_listener/2 :: (ring(), #repl_listener{}) -> #nat_listener{}|undefined).
%% @doc Fetch a replication nat host/port listener record from the Ring.
get_nat_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    NatListeners  = dict:fetch(natlisteners, RC),
    % search for a natlistener using only nodename, ip + port,
    % since nat uses nodename+ip+port+natip+natport as a key
    NatListenerMatches = [NatListener || NatListener <- NatListeners,
                        (NatListener#nat_listener.listen_addr == Listener#repl_listener.listen_addr
                        orelse NatListener#nat_listener.nat_addr == Listener#repl_listener.listen_addr),
                        NatListener#nat_listener.nodename == Listener#repl_listener.nodename],
    % this will only return the first nat listener that matches
    % the search criteria
    case NatListenerMatches of
        [NatListener|_] -> NatListener;
        [] -> undefined
    end.


initial_config() ->
    dict:from_list(
      [{natlisteners, []},
       {listeners, []},
       {sites, []},
       {version, ?REPL_VERSION}]
      ).

%% unit tests

-ifdef(TEST).

mock_ring() ->
    riak_core_ring:fresh(16, 'test@test').

ensure_config_test() ->
    Ring = ensure_config(mock_ring()),
    ?assertNot(undefined =:= riak_core_ring:get_meta(?MODULE, Ring)),
    Ring.

add_get_site_test() ->
    Ring0 = ensure_config_test(),
    Site = #repl_site{name="test"},
    Ring1 = add_site(Ring0, Site),
    Site = get_site(Ring1, "test"),
    Ring1.

add_site_addr_test() ->
    Ring0 = add_get_site_test(),
    Site = get_site(Ring0, "test"),
    ?assertEqual([], Site#repl_site.addrs),
    Ring1 = add_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([{"127.0.0.1", 9010}], Site1#repl_site.addrs),
    Ring1.

del_site_addr_test() ->
    Ring0 = add_site_addr_test(),
    Ring1 = del_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([], Site1#repl_site.addrs).

del_site_test() ->
    Ring0 = add_get_site_test(),
    Ring1 = del_site(Ring0, "test"),
    ?assertEqual(undefined, get_site(Ring1, "test")).

add_get_listener_test() ->
    Ring0 = ensure_config_test(),
    Listener = #repl_listener{nodename='test@test',
                              listen_addr={"127.0.0.1", 9010}},
    Ring1 = add_listener(Ring0, Listener),
    Listener = get_listener(Ring1, {"127.0.0.1", 9010}),
    Ring1.

del_listener_test() ->
    Ring0 = add_get_listener_test(),
    Ring1 = del_listener(Ring0, #repl_listener{nodename='test@test',
                                               listen_addr={"127.0.0.1", 9010}}),
    ?assertEqual(undefined, get_listener(Ring1, {"127.0.0.1", 9010})).

add_get_natlistener_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort},
                              nat_addr={NatAddr, NatPort}
                               },
    Ring1 = add_nat_listener(Ring0, NatListener),
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    ?assertNot(undefined == get_nat_listener(Ring1, Listener)).

del_natlistener_test() ->
    Ring0 = add_get_listener_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = del_listener(Ring0, Listener),
    ?assertEqual(undefined, get_nat_listener(Ring1, Listener)).
-endif.
