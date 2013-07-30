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


%% Returns true if the IP address given is a valid host IP address
-spec valid_host_ip(string() | inet:ip_address()) -> boolean().
valid_host_ip(IP) ->
    {ok, IFs} = inet:getifaddrs(),
    {ok, NormIP} = normalize_ip(IP),
    lists:any(fun({_IF, Attrs}) ->
                      lists:member({addr, NormIP}, Attrs)
              end, IFs).

%% @doc Convert IP address the tuple form
-spec normalize_ip(string() | inet:ip_address()) -> {ok, tuple()}.
normalize_ip(IP) when is_list(IP) ->
    inet_parse:address(IP);
normalize_ip(IP) when ?is_address(IP) ->
    {ok, IP};
normalize_ip(_) ->
    erlang:error(badarg).


%% @doc Given the result of inet:getifaddrs() and an IP a client has
%%      connected to, attempt to determine the appropriate subnet mask.  If
%%      the IP the client connected to cannot be found, undefined is returned.
-spec determine_netmask_len(Ifaddrs :: [{atom(), any()}], SeekIP :: string() | inet:ip_address()) -> 'undefined' | pos_integer().
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

find_addr_netmask([], _) ->
    undefined;
find_addr_netmask([{addr, NormIP}, {netmask, NM}|_Tail], NormIP) ->
    NM;
find_addr_netmask([_|Tail], NormIP) ->
    find_addr_netmask(Tail, NormIP).

-spec addr_to_binary(inet:ip_address()) -> binary().
addr_to_binary({A,B,C,D}) ->
    <<A:8,B:8,C:8,D:8>>;
addr_to_binary({A,B,C,D,E,F,G,H}) ->
    <<A:16,B:16,C:16,D:16,E:16,F:16,G:16,H:16>>.

-spec cidr_len(binary()) -> non_neg_integer().
cidr_len(Bin) ->
    cidr_len(Bin, 0).

cidr_len(<<>>, Acc) -> Acc;
cidr_len(<<0:1, _/bits>>, Acc) -> Acc;
cidr_len(<<1:1, Rest/bits>>, Acc) -> cidr_len(Rest, Acc + 1).

%% @doc Get the subnet mask as an integer, stolen from an old post on
%%      erlang-questions.
-spec mask_address(inet:ip_address(), non_neg_integer()) -> non_neg_integer().
mask_address(Addr, Maskbits) when ?is_address(Addr) ->
    <<Subnet:Maskbits, _Host/bitstring>> = addr_to_binary(Addr),
    Subnet.

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
get_matching_address(IP, CIDR, Listener) ->
    {ok, MyIPs} = inet:getifaddrs(),
    get_matching_address(IP, CIDR, MyIPs, Listener).

get_matching_address(IP, CIDR, MyIPs, {RawListenIP, Port}) ->
    {ok, ListenIP} = normalize_ip(RawListenIP),
    case ListenIP of
        BindAll when BindAll == {0, 0, 0, 0} orelse
                     BindAll == {0,0,0,0,0,0,0,0} ->
            get_matching_bindall(IP, CIDR, MyIPs, Port);
        _ ->
            get_matching_by_class(IP, ListenIP, Port)
    end.

get_matching_bindall(IP, CIDR, MyIPs, Port) ->
    case rfc1918(IP) of
        false ->
            %% search as low as a class A
            find_best_ip(MyIPs, IP, Port, CIDR, 8);
        RFCCIDR ->
            %% search as low as the bottom of the RFC1918 subnet
            find_best_ip(MyIPs, IP, Port, CIDR, RFCCIDR)
    end.

get_matching_by_class(IP, ListenIP, Port) ->
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
                          " nat-map?"),
            undefined
    end.

find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) when MyCIDR < MaxDepth ->
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
                                              lager:debug("wildly guessing that  ~p is close"
                                                          "to ~p", [IP, MyIP]),
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

find_best_ip(MyIPs, MyIP, Port, MyCIDR, MaxDepth) ->
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
                                      case {mask_address(IP, MyCIDR),
                                            mask_address(MyIP, MyCIDR)}  of
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
                                                          [IP, MyCIDR, _A]),
                                              lager:debug("IP ~p with CIDR ~p masked as ~p",
                                                          [MyIP, MyCIDR, _B]),
                                              Acc
                                      end
                              end
                      end, undefined, MyIPs),
    case Res of
        undefined ->
            %% Increase the search depth and retry, this will decrement the
            %% CIDR masks by one
            find_best_ip(MyIPs, MyIP, Port, MyCIDR - 1, MaxDepth);
        Res ->
            Res
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
      }
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
