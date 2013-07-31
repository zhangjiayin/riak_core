%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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
%% -------------------------------------------------------------------
-module(riak_core_inet).

-export([
         addr_to_binary/1,
         cidr_len/1,
         determine_netmask_len/2,
         get_matching_address/3,
         get_matching_address/4,
         is_rfc1918/1,
         mask_address/2,
         normalize_ip/1,
         rfc1918/1,
         valid_host_ip/1
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% IPv4 addresses are 4-tuples, IPv6 are 8-tuples
-define(is_address(A), is_tuple(A) andalso (tuple_size(A) == 4 orelse tuple_size(A) == 8)).

-type maybe(T) :: T | undefined.
-type ip_address() :: inet:ip_address().
-type portnum() :: inet:port_number().
-type binding() :: {ip_address(), portnum()}.
-type cidr() :: non_neg_integer().

%% Not sure why inet doesn't define these types directly.
-type ifflag() :: up | broadcast | loopback | pointtopoint | running | multicast.
-type ifopt() ::
        {flag, [ifflag()]} |
        {addr, ip_address()} |
        {netmask, ip_address()} |
        {broadaddr, ip_address()} |
        {dstaddr, ip_address()} |
        {hwaddr, [byte()]}.
-type ifaddr() :: {string(), [ifopt()]}.

%% Returns true if the IP address given is a valid host IP address
-spec valid_host_ip(string() | ip_address()) -> boolean().
valid_host_ip(IP) ->
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:any(fun({_IF, Attrs}) ->
                      lists:member({addr, NormIP}, Attrs)
              end, IFs).

%% @doc Convert IP address the tuple form
-spec normalize_ip(string() | ip_address()) -> {ok, ip_address()}.
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when ?is_address(IP) ->
    {ok, IP};
normalize_ip(_) ->
    erlang:error(badarg).


%% @doc Given the result of inet:getifaddrs() and an IP a client has
%%      connected to, attempt to determine the appropriate subnet mask.  If
%%      the IP the client connected to cannot be found, undefined is returned.
-spec determine_netmask_len(Ifaddrs :: [ifaddr()], SeekIP :: string() | ip_address()) -> maybe(pos_integer()).
determine_netmask_len(Ifaddrs, SeekIP) when is_list(SeekIP) ->
    {ok, NormIP} = normalize_ip(SeekIP),
    determine_netmask_len(Ifaddrs, NormIP);
determine_netmask_len([], _NormIP) ->
    undefined;
determine_netmask_len([{_If, Attrs} | Tail], NormIP) ->
    case find_addr_netmask(Attrs, NormIP) of
        NM when ?is_address(NM) ->
            cidr_len(addr_to_binary(NM));
        _ ->
            determine_netmask_len(Tail, NormIP)
    end.

-spec find_addr_netmask([ifopt()], ip_address()) -> maybe(ip_address()).
find_addr_netmask([], _) ->
    undefined;
find_addr_netmask([{addr, NormIP}, {netmask, NM}|_Tail], NormIP) ->
    NM;
find_addr_netmask([_|Tail], NormIP) ->
    find_addr_netmask(Tail, NormIP).

%% @doc Turns an IP address tuple into its equivalent binary format.
-spec addr_to_binary(ip_address()) -> binary().
addr_to_binary({A,B,C,D}) ->
    <<A:8,B:8,C:8,D:8>>;
addr_to_binary({A,B,C,D,E,F,G,H}) ->
    <<A:16,B:16,C:16,D:16,E:16,F:16,G:16,H:16>>.

%% @doc Given a netmask as a binary, return the CIDR length.
-spec cidr_len(binary()) -> cidr().
cidr_len(Bin) ->
    cidr_len(Bin, 0).

cidr_len(<<>>, Acc) -> Acc;
cidr_len(<<0:1, _/bits>>, Acc) -> Acc;
cidr_len(<<1:1, Rest/bits>>, Acc) -> cidr_len(Rest, Acc + 1).

%% @doc Get the subnet mask as an integer, stolen from an old post on
%%      erlang-questions.
-spec mask_address(ip_address(), cidr()) -> non_neg_integer().
mask_address(Addr, Maskbits) when ?is_address(Addr) ->
    <<Subnet:Maskbits, _Host/bitstring>> = addr_to_binary(Addr),
    Subnet.

%% @doc return RFC1918 mask for IP or false if not in RFC1918 range
-spec rfc1918(ip_address()) -> pos_integer() | false.
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
-spec is_rfc1918(ip_address()) -> boolean().
is_rfc1918(IP) ->
    is_integer(rfc1918(IP)).

%% @doc Find the right address to serve given the IP the node connected to.
%%      Ideally, it will choose an IP in the same subnet, but it will fall
%%      back to the 'closest' subnet (at least up to a class A). Then it will
%%      just try to find anything that matches the IP's RFC 1918 status (ie.
%%      public or private). Localhost will never be 'guessed', but it can be
%%      directly matched.
-spec get_matching_address(ip_address(), cidr(), binding() | [binding()]) ->
                                  maybe(binding()).
get_matching_address(IP, CIDR, Listener) ->
    {ok, MyIPs} = inet:getifaddrs(),
    get_matching_address(IP, CIDR, MyIPs, Listener).

-spec get_matching_address(ip_address(), cidr(), [ifaddr()], binding() | [binding()]) ->
                                  maybe(binding()).
get_matching_address(_, _, _, []) -> undefined;
get_matching_address(IP, CIDR, MyIPs, [Listener|Tail]) ->
    case get_matching_address(IP, CIDR, MyIPs, Listener) of
        undefined ->
            get_matching_address(IP, CIDR, MyIPs, Tail);
        Result ->
            Result
    end;
get_matching_address(IP, CIDR, MyIPs, {RawListenIP, Port}) ->
    {ok, ListenIP} = normalize_ip(RawListenIP),
    case ListenIP of
        BindAll when BindAll == {0,0,0,0} orelse
                     BindAll == {0,0,0,0,0,0,0,0} ->
            get_matching_bindall(IP, CIDR, MyIPs, Port);
        _ ->
            get_matching_by_class(IP, ListenIP, Port)
    end.

%% @doc Finds a matching binding within all interfaces.
-spec get_matching_bindall(ip_address(), cidr(), [ifaddr()], portnum()) ->
                                  maybe(binding()).
get_matching_bindall(IP, CIDR, MyIPs, Port) ->
    case rfc1918(IP) of
        false ->
            %% search as low as a class A
            find_best_ip(MyIPs, IP, Port, CIDR, 8);
        RFCCIDR ->
            %% search as low as the bottom of the RFC1918 subnet
            find_best_ip(MyIPs, IP, Port, CIDR, RFCCIDR)
    end.

%% @doc Returns a binding based on address-class matching.
-spec get_matching_by_class(ip_address(), ip_address(), portnum()) ->
                                   maybe(binding()).
get_matching_by_class(IP, ListenIP, Port) ->
    class_match_or_default(IP, ListenIP, Port, undefined).

%% @doc Returns {ListenIP, Port} if the passed addresses match by
%%     class, or the passed default.
class_match_or_default(IP, ListenIP, Port, Default) ->
    case class_matches(IP, ListenIP) of
        true ->
            {ListenIP, Port};
        false ->
            Default
    end.

%% @doc Whether both addresses are either RFC1918 addresses or both
%%      public.
-spec class_matches(ip_address(), ip_address()) -> boolean().
class_matches(IP, ListenIP) ->
    is_rfc1918(IP) == is_rfc1918(ListenIP).

%% @doc Returns a {ListenIP, Port} if the masked addresses match, or
%%      the passed default.
-spec mask_match_or_default(ip_address(), ip_address(), cidr(),
                            portnum(), maybe(binding()))
                            -> maybe(binding()).
mask_match_or_default(IP, ListenIP, CIDR, Port, Default) ->
    case mask_matches(IP, ListenIP, CIDR) of
        true ->
            %% 172.16/12 is a pain in the ass
            class_match_or_default(IP, ListenIP, Port, Default);
        false ->
            Default
    end.

%% @doc Whether the given addresses match when masked by the same
%%      amount.
-spec mask_matches(ip_address(), ip_address(), cidr()) -> boolean().
mask_matches(IP1, IP2, CIDR) ->
    mask_address(IP1, CIDR) == mask_address(IP2, CIDR).

%% @doc Filters interface bindings to IPv4 addresses and netmasks only.
-spec get_bindings_v4([ifopt()]) -> [{addr | netmask, inet:ip4_address()}].
get_bindings_v4(Attrs) ->
    [ Tuple || {Type, Addr}=Tuple <- Attrs,
               (addr == Type orelse netmask == Type),
               is_tuple(Addr) andalso 4 == tuple_size(Addr) ].

%% @doc Finds the best-matching host address for the given address
%%      among the list of interfaces.
-spec find_best_ip([ifaddr()], ip_address(), portnum(), cidr(), cidr()) ->
                          maybe(binding()).
find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) when MyCIDR < MaxDepth ->
    %% CIDR is now too small to meaningfully return a result
    %% blindly return *anything* that is close, I guess?
    lager:warning("Unable to find an approximate match for ~s/~b,"
                  "trying to guess one.",
                  [inet_parse:ntoa(MyIP), MyCIDR]),
    %% when guessing, never guess loopback!
    FixedIPs = lists:keydelete("lo", 1, MyIPs),
    Res =  fold_ifs_for_match(fun(IP, Default) ->
                                      class_match_or_default(MyIP, IP, Port, Default)
                              end, FixedIPs),
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

find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) ->
    Res = fold_ifs_for_match(fun(IP, Default) ->
                                     mask_match_or_default(MyIP, IP, MyCIDR, Port, Default)
                             end, MyIPs),
    case Res of
        undefined ->
            %% Increase the search depth and retry, this will decrement the
            %% CIDR masks by one
            find_best_ip(MyIPs, MyIP, Port, MyCIDR - 1, MaxDepth);
        Res ->
            Res
    end.

-spec fold_ifs_for_match(fun((ip_address(), maybe(binding())) -> maybe(binding())), [ifaddr()]) -> maybe(binding()).
fold_ifs_for_match(Matcher, IFs) ->
    lists:foldl(fun({_IF, Attrs}, Acc) ->
                        case get_bindings_v4(Attrs) of
                            [] ->
                                Acc;
                            [{addr, IP}|_] ->
                                Matcher(IP, Acc)
                        end
                end, undefined, IFs).

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
                    lager:start(),
                    %% for debugging
                    lager:set_loglevel(lager_console_backend, critical),
                    ok
            end,
     fun(_) ->
             application:stop(lager)
     end,
     [
      {"adjacent RFC 1918 IPs in subnet",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {10, 0, 0, 99}},
                                       {netmask, {255, 0, 0, 0}}]}]),
               Res = get_matching_address({10, 0, 0, 1}, 8, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{10,0,0,99},9090}, Res)
       end},
      {"RFC 1918 IPs in adjacent subnets",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {10, 4, 0, 99}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({10, 0, 0, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{10,4,0,99},9090}, Res)
       end
      },
      {"RFC 1918 IPs in different RFC 1918 blocks",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {10, 4, 0, 99}},
                                       {netmask, {255, 0, 0, 0}}]}]),
               Res = get_matching_address({192, 168, 0, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{10,4,0,99},9090}, Res)
       end
      },
      {"adjacent public IPs in subnet",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 8, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({8, 8, 8, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{8,8,8,8},9090}, Res)
       end
      },
      {"public IPs in adjacent subnets",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({8, 8, 8, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{8,0,8,8},9090}, Res)
       end
      },
      {"public IPs in different /8s",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({64, 8, 8, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{8,0,8,8},9090}, Res)
       end
      },
      {"connecting to localhost",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({127, 0, 0, 1}, 8, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{127,0,0,1},9090}, Res)
       end
      },
      {"RFC 1918 IPs when all we have are public ones",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {172, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({172, 16, 0, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual(undefined, Res)
       end
      },
      {"public IPs when all we have are RFC1918 ones",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {172, 16, 0, 1}},
                                       {netmask, {255, 255, 255, 0}}]}]),
               Res = get_matching_address({172, 0, 8, 8}, 24, Addrs, {{0,0,0,0},9090}),
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
               Res = get_matching_address({8, 8, 8, 1}, 8, Addrs, {{0,0,0,0},9090}),
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
               Res = get_matching_address({64, 8, 8, 1}, 24, Addrs, {{0,0,0,0},9090}),
               ?assertEqual({{64,172,243,100},9090}, Res)
       end
      },
      {"listen IP is not 0.0.0.0, return statically configured IP if both public",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]},
                                     {"eth1",
                                      [{addr, {64, 172, 243, 100}},
                                       {netmask, {255, 255, 255, 0}}]}
                                    ]),
               Res = get_matching_address({64, 8, 8, 1}, 24, Addrs, {{12, 24, 36, 8},9096}),
               ?assertEqual({{12,24,36,8},9096}, Res)
       end
      },
      {"listen IP is not 0.0.0.0, return statically configured IP if both private",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {10, 0, 0, 1}},
                                       {netmask, {255, 255, 255, 0}}]},
                                     {"eth1",
                                      [{addr, {64, 172, 243, 100}},
                                       {netmask, {255, 255, 255, 0}}]}
                                    ]),
               Res = get_matching_address({10, 0, 0, 1}, 24, Addrs, {{192, 168, 1, 1}, 9096}),
               ?assertEqual({{192,168,1,1},9096}, Res)
       end
      },
      {"listen IP is not 0.0.0.0, return undefined if both not public/private",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {10, 0, 0, 1}},
                                       {netmask, {255, 255, 255, 0}}]},
                                     {"eth1",
                                      [{addr, {64, 172, 243, 100}},
                                       {netmask, {255, 255, 255, 0}}]}
                                    ]),
               Res = get_matching_address({8, 8, 8, 8}, 24, Addrs, {{192, 168, 1, 1},9096}),
               ?assertEqual(undefined, Res)
       end
      },
      {"list of ip/port pairs for listeners",
       fun() ->
               Addrs = make_ifaddrs([{"eth0",
                                      [{addr, {8, 0, 8, 8}},
                                       {netmask, {255, 255, 255, 0}}]},
                                     {"eth1",
                                      [{addr, {64, 172, 243, 100}},
                                       {netmask, {255, 255, 255, 0}}]}
                                    ]),
               Listeners = [{{12, 24, 36, 8},9096}, {{0,0,0,0}, 10020}],
               Res = get_matching_address({64, 8, 8, 1}, 24, Addrs, Listeners),
               ?assertEqual({{12,24,36,8},9096}, Res)
       end}
     ]}.

determine_netmask_test_() ->
    [
     {"simple case",
      fun() ->
              Addrs = make_ifaddrs([{"eth0",
                                     [{addr, {10, 0, 0, 1}},
                                      {netmask, {255, 255, 255, 0}}]}]),
              ?assertEqual(24, determine_netmask_len(Addrs, {10, 0, 0, 1}))
      end
     },
     {"loopback",
      fun() ->
              Addrs = make_ifaddrs([{"eth0",
                                     [{addr, {10, 0, 0, 1}},
                                      {netmask, {255, 255, 255, 0}}]}]),
              ?assertEqual(8, determine_netmask_len(Addrs,
                                                    {127, 0, 0, 1}))
      end
     }

    ].
-endif.
