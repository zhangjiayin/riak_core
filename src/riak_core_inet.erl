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
         classify_v6/1,
         determine_netmask_len/2,
         get_matching_address/3,
         get_matching_address/4,
         is_rfc1918/1,
         mask_address/2,
         v6_routable/2,
         normalize_ip/1,
         rfc1918/1,
         valid_host_ip/1
        ]).

-ifdef(TEST).
-ifdef(EQC).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-type ipv6_class() :: loopback | unspecified | {ipv4_mapped, binary()} |
                      {ipv4_compatible, binary()} | {link_local, binary()} |
                      {site_local, bitstring(), binary()} |
                      {documentation, binary()} | {'6to4', binary()} |
                      {orchid, binary()} | {multicast, atom(), atom(), binary()} |
                      {global, binary()}.

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
    %% Loopback interfaces could be "lo", or "lo0", "lo1", etc.
    FixedIPs = lists:filter(fun({[$l,$o|_], _}) -> false; (_) -> true end, MyIPs),
    Res = fold_ifs_for_match(fun(IP, Default) ->
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

%% @doc Guesses whether two addresses are routable based on their
%%      address class and configuration. Even if this returns true,
%%      two addresses may not be routable, additional information will
%%      need to be checked.
%% @end
-spec v6_routable(ipv6_class(), ipv6_class()) -> boolean().
%% Loopback is only routable with itself.
v6_routable(loopback, loopback) -> true;
v6_routable(_, loopback) -> false;
v6_routable(loopback, _) -> false;
%% Link-local are only routable to the same address
v6_routable({link_local, A}, {link_local, A}) -> true;
v6_routable({link_local, _}, _) -> false;
v6_routable(_, {link_local,_}) -> false;
%% Unique-local addresses are only within an organization, not across
%% the Internet. They should only be routable to other unique-local
%% addresses.
v6_routable({unique_local,_,_,_},{unique_local,_,_,_}) -> true;
v6_routable({unique_local,_,_,_}, _) -> false;
v6_routable(_, {unique_local,_,_,_}) -> false;
v6_routable(_,_) ->
    %% At this point, we don't know any other restrictions, we have to
    %% guess true.
     true.


%% @doc Classifies an IPv6 address according to the numerous RFCs on
%%      the topic. This will return an atom (in the case of singleton
%%      addresses) or a tuple where the first element is atom of the
%%      class name.
-spec classify_v6(string() | binary() | inet:ipv6_address()) -> ipv6_class().
classify_v6(Addr) when is_list(Addr) ->
    {ok, Tuple} = inet_parse:address(Addr),
    classify_v6(Tuple);
classify_v6(Addr) when is_tuple(Addr) andalso tuple_size(Addr) == 8 ->
    %% Using bit-matching on addresses is easier and matches the RFCs
    %% better.
    classify_v6(addr_to_binary(Addr));
%% RFC5156 Section 2.1.  Node-Scoped Unicast
%%
%%   ::1/128 is the loopback address [RFC4291].
%%
%%   ::/128 is the unspecified address [RFC4291].
%%
%%   These two addresses should not appear on the public Internet.
classify_v6(<<1:128>>) -> loopback;
classify_v6(<<0:128>>) -> unspecified;
%% RFC5156 Section 2.2.  IPv4-Mapped Addresses
%%
%%   ::FFFF:0:0/96 are the IPv4-mapped addresses [RFC4291].  Addresses
%%   within this block should not appear on the public Internet.
classify_v6(<<0:80, 16#ffff:16, V4:4/bytes>>) -> {ipv4_mapped, V4};
%% RFC5156 Section 2.3.  IPv4-Compatible Addresses
%%
%%   ::<ipv4-address>/96 are the IPv4-compatible addresses [RFC4291].
%%   These addresses are deprecated and should not appear on the public
%%   Internet.
classify_v6(<<0:96, V4:4/bytes>>) -> {ipv4_compatible, V4};
%% RFC5156 Section 2.4.  Link-Scoped Unicast
%%
%%   fe80::/10 are the link-local unicast [RFC4291] addresses.  Addresses
%%   within this block should not appear on the public Internet.
classify_v6(<<2#1111111010:10, 0:54, IF:8/bytes>>) -> {link_local, IF};
%% RFC4291 2.5.7.  Site-Local IPv6 Unicast Addresses
%%
%%   Site-Local addresses were originally designed to be used for
%%   addressing inside of a site without the need for a global prefix.
%%   Site-local addresses are now deprecated as defined in [SLDEP].
%%
%%   Site-Local addresses have the following format:
%%
%%   |   10     |
%%   |  bits    |         54 bits         |         64 bits            |
%%   +----------+-------------------------+----------------------------+
%%   |1111111011|        subnet ID        |       interface ID         |
%%   +----------+-------------------------+----------------------------+
%%
%%   The special behavior of this prefix defined in [RFC3513] must no
%%   longer be supported in new implementations (i.e., new implementations
%%   must treat this prefix as Global Unicast).
%%
%%   Existing implementations and deployments may continue to use this
%%   prefix.
classify_v6(<<2#1111111011:10, Subnet:54/bits, IF:8/bytes>>) -> {site_local, Subnet, IF};
%% RFC5156 Section 2.5.  Unique-Local
%%
%%   fc00::/7 are the unique-local addresses [RFC4193].  Addresses within
%%   this block should not appear by default on the public Internet.
%%   Procedures for advertising these addresses are further described in
%%   [RFC4193].
classify_v6(<<2#1111110:7, 1:1, GlobalID:5/bytes, Subnet:2/bytes, Interface:8/bytes>>) ->
    {unique_local, GlobalID, Subnet, Interface};
%% RFC5156 Section 2.6.  Documentation Prefix
%%
%%   The 2001:db8::/32 are the documentation addresses [RFC3849].  They
%%   are used for documentation purposes such as user manuals, RFCs, etc.
%%   Addresses within this block should not appear on the public Internet.
classify_v6(<<16#2001:16, 16#DB8:16, _:96>>=Addr) -> {documentation, Addr};
%% RFC5156 Section 2.7.  6to4
%%
%%   2002::/16 are the 6to4 addresses [RFC3056].  The 6to4 addresses may
%%   be advertised when the site is running a 6to4 relay or offering a
%%   6to4 transit service.  Running such a service [RFC3964] entails
%%   filtering rules specific to 6to4 [RFC3964].  IPv4 addresses
%%   disallowed in 6to4 prefixes are listed in section 5.3.1 of [RFC3964].
classify_v6(<<16#2002:16, Rest:14/bytes>>) -> {'6to4', Rest};
%% RFC5156 Section 2.8.  Teredo
%%
%%   2001::/32 are the Teredo addresses [RFC4380].  The Teredo addresses
%%   may be advertised when the site is running a Teredo relay or offering
%%   a Teredo transit service.
classify_v6(<<16#2001:16, 0:16, Rest:12/bytes>>) -> {teredo, Rest};
%% RFC5156 Section 2.10.  ORCHID
%%
%%   2001:10::/28 are Overlay Routable Cryptographic Hash IDentifiers
%%   (ORCHID) addresses [RFC4843].  These addresses are used as
%%   identifiers and are not routable at the IP layer.  Addresses within
%%   this block should not appear on the public Internet.
classify_v6(<<16#2001:16, 1:12, Rest:100/bits>>) -> {orchid, Rest};
%% RFC5156 2.13.  Multicast
%%
%%   ff00::/8 are multicast addresses [RFC4291].  They contain a 4-bit
%%   scope in the address field where only some values are of global scope
%%   [RFC4291].  Only addresses with global scope in this block may appear
%%   on the public Internet.
%%
%%   Multicast routes must not appear in unicast routing tables.
classify_v6(<<16#ff:8, Flags:4, Scope:4, Rest:14/bytes>>) ->
    {multicast, multicast_type(Flags), multicast_scope(Scope), Rest};
%% It must otherwise be a global address, or indistinguishable.
classify_v6(<<_:128>>=Addr) -> {global, Addr}.

%% RFC4291 Section 2.7
%%    scop is a 4-bit multicast scope value used to limit the scope of
%%    the multicast group.  The values are as follows:
%%
%%       0  reserved
%%       1  Interface-Local scope
%%       2  Link-Local scope
%%       3  reserved
%%       4  Admin-Local scope
%%       5  Site-Local scope
%%       6  (unassigned)
%%       7  (unassigned)
%%       8  Organization-Local scope
%%       9  (unassigned)
%%       A  (unassigned)
%%       B  (unassigned)
%%       C  (unassigned)
%%       D  (unassigned)
%%       E  Global scope
%%       F  reserved
%%
%%       Interface-Local scope spans only a single interface on a node
%%       and is useful only for loopback transmission of multicast.
%%
%%       Link-Local multicast scope spans the same topological region as
%%       the corresponding unicast scope.
%%
%%       Admin-Local scope is the smallest scope that must be
%%       administratively configured, i.e., not automatically derived
%%       from physical connectivity or other, non-multicast-related
%%       configuration.
%%
%%       Site-Local scope is intended to span a single site.
%%
%%       Organization-Local scope is intended to span multiple sites
%%       belonging to a single organization.
%%
%%       scopes labeled "(unassigned)" are available for administrators
%%       to define additional multicast regions.
multicast_scope(1) -> interface;
multicast_scope(2) -> link;
multicast_scope(4) -> admin;
multicast_scope(5) -> site;
multicast_scope(8) -> organization;
multicast_scope(16#E) -> global;
multicast_scope(S) -> S.

%% RFC3956 Section 3
%%   When the highest-order bit is 0, R = 1 indicates a multicast address
%%   that embeds the address on the RP.  Then P MUST be set to 1, and
%%   consequently T MUST be set to 1, as specified in [RFC3306].  In
%%   effect, this implies the prefix FF70::/12.  In this case, the last 4
%%   bits of the previously reserved field are interpreted as embedding
%%   the RP interface ID, as specified in this memo.
%%
%%   The behavior is unspecified if P or T is not set to 1, as then the
%%   prefix would not be FF70::/12.  Likewise, the encoding and the
%%   protocol mode used when the two high-order bits in "flgs" are set to
%%   11 ("FFF0::/12") is intentionally unspecified until such time that
%%   the highest-order bit is defined.  Without further IETF
%%   specification, implementations SHOULD NOT treat the FFF0::/12 range
%%   as Embedded-RP.
%%
%%   R = 0 indicates a multicast address that does not embed the address
%%   of the RP and follows the semantics defined in [ADDRARCH] and
%%   [RFC3306].  In this context, the value of "RIID" MUST be sent as zero
%%   and MUST be ignored on receipt.
%%
%% RFC3306 Section 4
%%         o  P = 0 indicates a multicast address that is not assigned
%%            based on the network prefix.  This indicates a multicast
%%            address as defined in [ADDRARCH].
%%
%%         o  P = 1 indicates a multicast address that is assigned based
%%            on the network prefix.
%%
%%         o  If P = 1, T MUST be set to 1, otherwise the setting of the T
%%            bit is defined in Section 2.7 of [ADDRARCH].
%%
%% RFC4291 Section 2.7
%%       T = 0 indicates a permanently-assigned ("well-known") multicast
%%       address, assigned by the Internet Assigned Numbers Authority
%%       (IANA).
%%
%%       T = 1 indicates a non-permanently-assigned ("transient" or
%%       "dynamically" assigned) multicast address.
%%
multicast_type(2#0111) -> embedded_address;
multicast_type(2#0011) -> prefixed;
multicast_type(2#0001) -> transient;
multicast_type(2#0000) -> permanent;
multicast_type(_) -> undefined.

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

-ifdef(EQC).
-define(F(Str,Args), lists:flatten(io_lib:format(Str,Args))).
-define(FULLADDR, "~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B").
-define(TRYMATCH(A,B),
        try
            A = B,
            true
        catch
            _:{badmatch,_} ->
                false
        end).

classify_test_() ->
    {timeout, 60000,
     ?_assertEqual(true,
                   eqc:quickcheck(eqc:testing_time(15,
                                                   ?QC_OUT(prop_classify()))))}.

gen_address() ->
    frequency([ {1, {loopback, "::1"}},
                {1, {unspecified, "::"}},
                {7, {ipv4_mapped, gen_ipv4_mapped()}},
                {2, {ipv4_compatible, gen_ipv4_compatible()}},
                {7, {link_local, gen_link_local()}},
                {2, {site_local, gen_site_local()}},
                {7, {unique_local, gen_unique_local()}},
                {2, {documentation, gen_doc()}},
                {2, {'6to4', gen_6to4()}},
                {2, {teredo, gen_teredo()}},
                {2, {orchid, gen_orchid()}},
                {7, {multicast, gen_multicast()}},
                {12, {global, gen_global()}}
              ]).

gen_ipv4_mapped() ->
    ?LET(V4, binary(4), <<16#ffff:96, V4/binary>>).

gen_ipv4_compatible() ->
    %% Make sure to exclude :: and ::1
    ?LET(V4, choose(2, 16#ffffffff),
         <<0:96, V4:32>>).

gen_link_local() ->
    ?LET(<<A:16,B:16,C:16,D:16>>, binary(8),
         ?F("fe80::~.16B:~.16B:~.16B:~.16B",[A,B,C,D])).

gen_site_local() ->
    ?LET(<<A:6,B:16,C:16,D:16,E:16,F:16,G:16,H:16>>, bitstring(118),
        ?F("~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B",
           [16#fec0+A, B, C, D, E, F, G, H])).

gen_unique_local() ->
    ?LET(<<A:8,B:16,C:16,D:16,E:16,F:16,G:16,H:16>>,
         bitstring(120),
         ?F(?FULLADDR,
            [16#fd00+A, B, C, D, E, F, G, H])).

gen_doc() ->
    ?LET(Parts,
         vector(6, choose(0,65535)),
         ?F("2001:DB8:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B", Parts)).

gen_6to4() ->
    ?LET(Parts, vector(7,choose(0,65535)),
         ?F("2002:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B",Parts)).

gen_teredo() ->
    ?LET(Parts, vector(6, choose(0,65535)),
         ?F("2001:0:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B", Parts)).

gen_orchid() ->
    ?LET({T, Parts}, {choose(0,15), vector(6, choose(0,65535))},
         ?F("2001:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B:~.16B",
            [(1 bsl 4) + T|Parts])).

gen_multicast() ->
    ?LET({Fl, Sc, Parts},
         {elements([0,1,3,7]), % See RFCs above
          elements([1,2,4,5,8,14,6,7,9,10,11,12,13]),
          vector(7, choose(0,65535))},
         ?F(?FULLADDR, [16#FF00 + (Fl bsl 4) + Sc|Parts])).

gen_global() ->
    ?SUCHTHAT(B,
              binary(16),
              not (
                %% unspecified
                <<0:128>> == B orelse
                %% loopback
                <<1:128>> == B orelse
                %% ipv4 mapped
                ?TRYMATCH(<<16#ffff:96,_/bits>>, B) orelse
                %% ipv4 compatible
                ?TRYMATCH(<<0:96,_/bits>>, B) orelse
                %% link local
                ?TRYMATCH(<<16#FE80:16,0:48,_/bits>>, B) orelse
                %% site local
                ?TRYMATCH(<<2#1111111011:10,_/bits>>, B) orelse
                %% unique local
                ?TRYMATCH(<<16#FD, _/bits>>, B) orelse
                %% documentation
                ?TRYMATCH(<<16#2001:16,16#DB8:16,_/bits>>, B) orelse
                %% 6to4
                ?TRYMATCH(<<16#2002:16,_/bits>>, B) orelse
                %% teredo
                ?TRYMATCH(<<16#2001:16,0:16,_/bits>>, B) orelse
                %% orchid
                ?TRYMATCH(<<16#2001:16,1:12,_/bits>>, B) orelse
                %% multicast
                ?TRYMATCH(<<16#ff,_/bits>>, B)
               )
              ).

prop_classify() ->
    ?FORALL({Type, Addr}, gen_address(),
            collect(Type,
                    ?WHENFAIL(
                       begin
                           io:format("~n-------~nAddress:: ~p~nExpected:: ~p~nActual:: ~p~n------~n",
                                     [Addr, Type, classify_v6(Addr)])
                       end,
                       begin
                           Class = classify_v6(Addr),
                           Type == Class orelse
                                           (is_tuple(Class) andalso
                                            Type == element(1, Class))
                       end))).
-endif. % EQC
-endif. % TEST
