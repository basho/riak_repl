%% Riak EnterpriseDS
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl2_ip).
-export([get_matching_address/2, determine_netmask/2, mask_address/2,
        maybe_apply_nat_map/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Given the result of inet:getifaddrs() and an IP a client has
%%      connected to, attempt to determine the appropriate subnet mask.  If
%%      the IP the client connected to cannot be found, undefined is returned.
-spec determine_netmask(Ifaddrs :: [{atom(), any()}], SeekIP :: string() | {integer(), integer(), integer(), integer()}) -> 'undefined' | binary().
determine_netmask(Ifaddrs, SeekIP) when is_list(SeekIP) ->
    {ok, NormIP} = riak_repl_util:normalize_ip(SeekIP),
    determine_netmask(Ifaddrs, NormIP);

determine_netmask([], _NormIP) ->
    undefined;

determine_netmask([{_If, Attrs} | Tail], NormIP) ->
    case lists_pos({addr, NormIP}, Attrs) of
        not_found ->
            determine_netmask(Tail, NormIP);
        N ->
            case lists:nth(N + 1, Attrs) of
                {netmask, {_, _, _, _} = NM} ->
                    cidr(list_to_binary(tuple_to_list(NM)),0);
                _ ->
                    determine_netmask(Tail, NormIP)
            end
    end.

lists_pos(Needle, Haystack) ->
    lists_pos(Needle, Haystack, 1).

lists_pos(_Needle, [], _N) ->
    not_found;

lists_pos(Needle, [Needle | _Haystack], N) ->
    N;

lists_pos(Needle, [_NotNeedle | Haystack], N) ->
    lists_pos(Needle, Haystack, N + 1).

%% count the number of 1s in netmask to get the CIDR
%% Maybe there's a better way....?
cidr(<<>>, Acc) ->
    Acc;
cidr(<<X:1/bits, Rest/bits>>, Acc) ->
    case X of
        <<1:1>> ->
            cidr(Rest, Acc + 1);
        _ ->
            cidr(Rest, Acc)
    end.

%% @doc Get the subnet mask as an integer, stolen from an old post on
%%      erlang-questions.
mask_address(Addr={_, _, _, _}, Maskbits) ->
    B = list_to_binary(tuple_to_list(Addr)),
    lager:debug("address as binary: ~p ~p", [B,Maskbits]),
    <<Subnet:Maskbits, _Host/bitstring>> = B,
    Subnet;
mask_address(_, _) ->
    %% presumably ipv6, don't have a function for that one yet
    undefined.

%% return RFC1918 mask for IP or false if not in RFC1918 range
rfc1918({10, _, _, _}) ->
    8;
rfc1918({192,168, _, _}) ->
    16;
rfc1918(IP={172, _, _, _}) ->
    %% this one is a /12, not so simple
    case mask_address({172, 16, 0, 0}, 12) == mask_address(IP, 12) of
        true ->
            12;
        false ->
            false
    end;
rfc1918(_) ->
    false.

%% true/false if IP is RFC1918
is_rfc1918(IP) ->
    case rfc1918(IP) of
        false ->
            false;
        _ ->
            true
    end.

%% @doc Find the right address to serve given the IP the node connected to.
%%      Ideally, it will choose an IP in the same subnet, but it will fall
%%      back to the 'closest' subnet (at least up to a class A). Then it will
%%      just try to find anything that matches the IP's RFC 1918 status (ie.
%%      public or private). Localhost will never be 'guessed', but it can be
%%      directly matched.
get_matching_address(IP, CIDR) ->
    {ok, MyIPs} = inet:getifaddrs(),
    get_matching_address(IP, CIDR, MyIPs).

get_matching_address(IP, CIDR, MyIPs) ->
    {RawListenIP, Port} = app_helper:get_env(riak_core, cluster_mgr),
    {ok, ListenIP} = riak_repl_util:normalize_ip(RawListenIP),
    case ListenIP of
        {0, 0, 0, 0} ->
            case rfc1918(IP) of
                false ->
                    %% search as low as a class A
                    find_best_ip(MyIPs, IP, Port, CIDR, CIDR - 8);
                RFCCIDR ->
                    %% search as low as the bottom of the RFC1918 subnet
                    find_best_ip(MyIPs, IP, Port, CIDR, (CIDR - RFCCIDR)+1)
            end;
        _ ->
            case is_rfc1918(IP) == is_rfc1918(ListenIP) of
                true ->
                    %% Both addresses are either internal or external.
                    %% We'll have to assume the user knows what they're
                    %% doing
                    lager:debug("returning specific listen IP ~p",
                            [ListenIP]),
                    {ListenIP, Port};
                false ->
                    %% we should never get here if things are configured right
                    lager:warning("NAT detected, do you need to define a"
                        "nat-map?"),
                    undefined
            end
    end.


find_best_ip(MyIPs, MyIP, Port, MyCIDR, SearchDepth) when MyCIDR - SearchDepth < 8->
    %% CIDR is now too small to meaningfully return a result
    %% blindly return *anything* that is close, I guess?
    lager:warning("Unable to find an approximate match for ~s/~b,"
        "trying to guess one.",
        [inet_parse:ntoa(MyIP), MyCIDR]),
    %% when guessing, never guess loopback!
    FixedIPs = lists:keydelete("lo", 1, MyIPs),
    Res = lists:foldl(fun({_IF, Attrs}, Acc) ->
                V4Attrs = lists:filter(fun({addr, {_, _, _, _}}) ->
                            true;
                        ({netmask, {_, _, _, _}}) ->
                            true;
                        (_) ->
                            false
                    end, Attrs),
                case V4Attrs of
                    [] ->
                        lager:debug("no valid IPs for ~s", [_IF]),
                        Acc;
                    _ ->
                        lager:debug("IPs for ~s : ~p", [_IF, V4Attrs]),
                        IP = proplists:get_value(addr, V4Attrs),
                        case is_rfc1918(MyIP) == is_rfc1918(IP) of
                            true ->
                                {IP, Port};
                            false ->
                                Acc
                        end
                end
        end, undefined, FixedIPs),
    case Res of
        undefined ->
            lager:warning("Unable to guess an appropriate local IP to match"
                " ~s/~b", [inet_parse:ntoa(MyIP), MyCIDR]),
            Res;
        {IP, _Port} ->
            lager:notice("Guessed ~s to match ~s/~b",
                [inet_parse:ntoa(IP), inet_parse:ntoa(MyIP), MyCIDR]),
            Res
    end;

find_best_ip(MyIPs, MyIP, Port, MyCIDR, SearchDepth) ->
    Res = lists:foldl(fun({_IF, Attrs}, Acc) ->
                V4Attrs = lists:filter(fun({addr, {_, _, _, _}}) ->
                            true;
                        ({netmask, {_, _, _, _}}) ->
                            true;
                        (_) ->
                            false
                    end, Attrs),
                case V4Attrs of
                    [] ->
                        lager:debug("no valid IPs for ~s", [_IF]),
                        Acc;
                    _ ->
                        lager:debug("IPs for ~s : ~p", [_IF, V4Attrs]),
                        IP = proplists:get_value(addr, V4Attrs),
                        NetMask = proplists:get_value(netmask, V4Attrs),
                        CIDR = case cidr(list_to_binary(tuple_to_list(NetMask)), 0) of
                            X when X < SearchDepth ->
                                MyCIDR;
                            X ->
                                X
                        end,

                        lager:debug("CIDRs are ~p ~p for depth ~p", [CIDR-SearchDepth,
                                MyCIDR - SearchDepth, SearchDepth]),
                        case {mask_address(IP, CIDR - SearchDepth),
                                mask_address(MyIP, MyCIDR - SearchDepth)}  of
                            {Mask, Mask} ->
                                %% the 172.16/12 is a pain in the ass
                                case is_rfc1918(IP) == is_rfc1918(MyIP) of
                                    true ->
                                        lager:debug("matched IP ~p for ~p", [IP,
                                                MyIP]),
                                        {IP, Port};
                                    false ->
                                        Acc
                                end;
                            {_A, _B} ->
                                lager:debug("IP ~p with CIDR ~p masked as ~p",
                                        [IP, CIDR, _A]),
                                lager:debug("IP ~p with CIDR ~p masked as ~p",
                                        [MyIP, MyCIDR, _B]),
                                Acc
                        end
                end
        end, undefined, MyIPs),
    case Res of
        undefined ->
            %% TODO check for NAT

            %% Increase the search depth and retry, this will decrement the
            %% CIDR masks by one
            find_best_ip(MyIPs, MyIP, Port, MyCIDR, SearchDepth+1);
        Res ->
            Res
    end.

%% Apply the relevant nat-mapping rule, if any
maybe_apply_nat_map(IP, Port, Map) ->
    case lists:keyfind({IP, Port}, 1, Map) of
        false ->
            case lists:keyfind(IP, 1, Map) of
                {_, InternalIP} ->
                    InternalIP;
                false ->
                    IP
            end
        {_, InternalIP} ->
            InternalIP
    end.

-ifdef(TEST).

make_ifaddrs(Interfaces) ->
    lists:ukeymerge(1, lists:usort(Interfaces), lists:usort([
                {"lo",
                    [{flags,[up,loopback,running]},
                        {hwaddr,[0,0,0,0,0,0]},
                        {addr,{127,0,0,1}},
                        {netmask,{255,0,0,0}},
                        {addr,{0,0,0,0,0,0,0,1}},
                        {netmask,{65535,65535,65535,65535,65535,65535,65535,
                                65535}}]}])).


get_matching_address_test_() ->
    {setup, fun() ->
                application:set_env(riak_core, cluster_mgr, {{0, 0, 0, 0},
                        9090}),
                lager:start(),
                lager:set_loglevel(lager_console_backend, debug)
        end,
        fun(_) ->
                application:unset_env(riak_core, cluster_mgr),
                application:stop(lager)
        end,
        [
            {"adjacent RFC 1918 IPs in subnet",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 0, 0, 99}},
                                        {netmask, {255, 0, 0, 0}}]}]),
                        Res = get_matching_address({10, 0, 0, 1}, 8, Addrs),
                        ?assertEqual({{10,0,0,99},9090}, Res)
                end},
            {"RFC 1918 IPs in adjacent subnets",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 4, 0, 99}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({10, 0, 0, 1}, 24, Addrs),
                        ?assertEqual({{10,4,0,99},9090}, Res)
                end
            },
            {"RFC 1918 IPs in different RFC 1918 blocks",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 4, 0, 99}},
                                        {netmask, {255, 0, 0, 0}}]}]),
                        Res = get_matching_address({192, 168, 0, 1}, 24, Addrs),
                        ?assertEqual({{10,4,0,99},9090}, Res)
                end
            },
            {"adjacent public IPs in subnet",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 8, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({8, 8, 8, 1}, 24, Addrs),
                        ?assertEqual({{8,8,8,8},9090}, Res)
                end
            },
            {"public IPs in adjacent subnets",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({8, 8, 8, 1}, 24, Addrs),
                        ?assertEqual({{8,0,8,8},9090}, Res)
                end
            },
            {"public IPs in different /8s",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                        ?assertEqual({{8,0,8,8},9090}, Res)
                end
            },
            {"connecting to localhost",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({127, 0, 0, 1}, 8, Addrs),
                        ?assertEqual({{127,0,0,1},9090}, Res)
                end
            },
            {"RFC 1918 IPs when all we have are public ones",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {172, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({172, 16, 0, 1}, 24, Addrs),
                        ?assertEqual(undefined, Res)
                end
            },
            {"public IPs when all we have are RFC1918 ones",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 0, 0, 1}},
                                        {netmask, {255, 255, 255, 0}}]}]),
                        Res = get_matching_address({8, 8, 8, 1}, 8, Addrs),
                        ?assertEqual(undefined, Res)
                end
            },
            {"public IPs when all we have are IPv6 ones",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr,
                                            {65152,0,0,0,29270,33279,65179,6921}},
                                        {netmask,
                                            {65535,65535,65535,65535,0,0,0,0}}]}]),
                        Res = get_matching_address({8, 8, 8, 1}, 8, Addrs),
                        ?assertEqual(undefined, Res)
                end
            },
            {"public IPs in different subnets, prefer closest",
                fun() ->
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]},
                                {"eth1",
                                    [{addr, {64, 172, 243, 100}},
                                        {netmask, {255, 255, 255, 0}}]}
                                    ]),
                        Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                        ?assertEqual({{64,172,243,100},9090}, Res)
                end
            },
            {"listen IP is not 0.0.0.0, return statically configured IP if both public",
                fun() ->
                        application:set_env(riak_core, cluster_mgr, {{12, 24, 36, 8},
                                9096}),
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {8, 0, 8, 8}},
                                        {netmask, {255, 255, 255, 0}}]},
                                {"eth1",
                                    [{addr, {64, 172, 243, 100}},
                                        {netmask, {255, 255, 255, 0}}]}
                                    ]),
                        Res = get_matching_address({64, 8, 8, 1}, 24, Addrs),
                        ?assertEqual({{12,24,36,8},9096}, Res)
                end
            },
            {"listen IP is not 0.0.0.0, return statically configured IP if both private",
                fun() ->
                        application:set_env(riak_core, cluster_mgr, {{192, 168, 1, 1},
                                9096}),
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 0, 0, 1}},
                                        {netmask, {255, 255, 255, 0}}]},
                                {"eth1",
                                    [{addr, {64, 172, 243, 100}},
                                        {netmask, {255, 255, 255, 0}}]}
                                    ]),
                        Res = get_matching_address({10, 0, 0, 1}, 24, Addrs),
                        ?assertEqual({{192,168,1,1},9096}, Res)
                end
            },
            {"listen IP is not 0.0.0.0, return undefined if both not public/private",
                fun() ->
                        application:set_env(riak_core, cluster_mgr, {{192, 168, 1, 1},
                                9096}),
                        Addrs = make_ifaddrs([{"eth0",
                                    [{addr, {10, 0, 0, 1}},
                                        {netmask, {255, 255, 255, 0}}]},
                                {"eth1",
                                    [{addr, {64, 172, 243, 100}},
                                        {netmask, {255, 255, 255, 0}}]}
                                    ]),
                        Res = get_matching_address({8, 8, 8, 8}, 24, Addrs),
                        ?assertEqual(undefined, Res)
                end
            }
        ]}.

determine_netmask_test_() ->
    [
        {"simple case",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                [{addr, {10, 0, 0, 1}},
                                    {netmask, {255, 255, 255, 0}}]}]),
                    ?assertEqual(24, determine_netmask(Addrs,
                            {10, 0, 0, 1}))
            end
        },
        {"loopback",
            fun() ->
                    Addrs = make_ifaddrs([{"eth0",
                                [{addr, {10, 0, 0, 1}},
                                    {netmask, {255, 255, 255, 0}}]}]),
                    ?assertEqual(8, determine_netmask(Addrs,
                            {127, 0, 0, 1}))
            end
        }

    ].

-endif.


