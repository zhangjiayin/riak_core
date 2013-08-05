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
-module(riak_core_inet6).

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

-export([
         classify_address/1,
         maybe_routable/2
        ]).

%% @doc Guesses whether two addresses are routable based on their
%%      address class and configuration. Even if this returns true,
%%      two addresses may not be routable, additional information will
%%      need to be checked.
%% @end
-spec maybe_routable(ipv6_class(), ipv6_class()) -> boolean().
%% Loopback is only routable with itself.
maybe_routable(loopback, loopback) -> true;
maybe_routable(_, loopback) -> false;
maybe_routable(loopback, _) -> false;
%% Link-local are only routable to the same address
maybe_routable({link_local, A}, {link_local, A}) -> true;
maybe_routable({link_local, _}, _) -> false;
maybe_routable(_, {link_local,_}) -> false;
%% Unique-local addresses are only within an organization, not across
%% the Internet. They should only be routable to other unique-local
%% addresses.
maybe_routable({unique_local,_,_,_},{unique_local,_,_,_}) -> true;
maybe_routable({unique_local,_,_,_}, _) -> false;
maybe_routable(_, {unique_local,_,_,_}) -> false;
maybe_routable(_,_) ->
    %% At this point, we don't know any other restrictions, we have to
    %% guess true.
     true.

%% @doc Classifies an IPv6 address according to the numerous RFCs on
%%      the topic. This will return an atom (in the case of singleton
%%      addresses) or a tuple where the first element is atom of the
%%      class name.
-spec classify_address(string() | binary() | inet:ipv6_address()) -> ipv6_class().
classify_address(Addr) when is_list(Addr) ->
    {ok, Tuple} = inet_parse:address(Addr),
    classify_address(Tuple);
classify_address(Addr) when is_tuple(Addr) andalso tuple_size(Addr) == 8 ->
    %% Using bit-matching on addresses is easier and matches the RFCs
    %% better.
    classify_address(riak_core_inet:addr_to_binary(Addr));
%% RFC5156 Section 2.1.  Node-Scoped Unicast
%%
%%   ::1/128 is the loopback address [RFC4291].
%%
%%   ::/128 is the unspecified address [RFC4291].
%%
%%   These two addresses should not appear on the public Internet.
classify_address(<<1:128>>) -> loopback;
classify_address(<<0:128>>) -> unspecified;
%% RFC5156 Section 2.2.  IPv4-Mapped Addresses
%%
%%   ::FFFF:0:0/96 are the IPv4-mapped addresses [RFC4291].  Addresses
%%   within this block should not appear on the public Internet.
classify_address(<<0:80, 16#ffff:16, V4:4/bytes>>) -> {ipv4_mapped, V4};
%% RFC5156 Section 2.3.  IPv4-Compatible Addresses
%%
%%   ::<ipv4-address>/96 are the IPv4-compatible addresses [RFC4291].
%%   These addresses are deprecated and should not appear on the public
%%   Internet.
classify_address(<<0:96, V4:4/bytes>>) -> {ipv4_compatible, V4};
%% RFC5156 Section 2.4.  Link-Scoped Unicast
%%
%%   fe80::/10 are the link-local unicast [RFC4291] addresses.  Addresses
%%   within this block should not appear on the public Internet.
classify_address(<<2#1111111010:10, 0:54, IF:8/bytes>>) -> {link_local, IF};
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
classify_address(<<2#1111111011:10, Subnet:54/bits, IF:8/bytes>>) -> {site_local, Subnet, IF};
%% RFC5156 Section 2.5.  Unique-Local
%%
%%   fc00::/7 are the unique-local addresses [RFC4193].  Addresses within
%%   this block should not appear by default on the public Internet.
%%   Procedures for advertising these addresses are further described in
%%   [RFC4193].
classify_address(<<2#1111110:7, 1:1, GlobalID:5/bytes, Subnet:2/bytes, Interface:8/bytes>>) ->
    {unique_local, GlobalID, Subnet, Interface};
%% RFC5156 Section 2.6.  Documentation Prefix
%%
%%   The 2001:db8::/32 are the documentation addresses [RFC3849].  They
%%   are used for documentation purposes such as user manuals, RFCs, etc.
%%   Addresses within this block should not appear on the public Internet.
classify_address(<<16#2001:16, 16#DB8:16, _:96>>=Addr) -> {documentation, Addr};
%% RFC5156 Section 2.7.  6to4
%%
%%   2002::/16 are the 6to4 addresses [RFC3056].  The 6to4 addresses may
%%   be advertised when the site is running a 6to4 relay or offering a
%%   6to4 transit service.  Running such a service [RFC3964] entails
%%   filtering rules specific to 6to4 [RFC3964].  IPv4 addresses
%%   disallowed in 6to4 prefixes are listed in section 5.3.1 of [RFC3964].
classify_address(<<16#2002:16, Rest:14/bytes>>) -> {'6to4', Rest};
%% RFC5156 Section 2.8.  Teredo
%%
%%   2001::/32 are the Teredo addresses [RFC4380].  The Teredo addresses
%%   may be advertised when the site is running a Teredo relay or offering
%%   a Teredo transit service.
classify_address(<<16#2001:16, 0:16, Rest:12/bytes>>) -> {teredo, Rest};
%% RFC5156 Section 2.10.  ORCHID
%%
%%   2001:10::/28 are Overlay Routable Cryptographic Hash IDentifiers
%%   (ORCHID) addresses [RFC4843].  These addresses are used as
%%   identifiers and are not routable at the IP layer.  Addresses within
%%   this block should not appear on the public Internet.
classify_address(<<16#2001:16, 1:12, Rest:100/bits>>) -> {orchid, Rest};
%% RFC5156 2.13.  Multicast
%%
%%   ff00::/8 are multicast addresses [RFC4291].  They contain a 4-bit
%%   scope in the address field where only some values are of global scope
%%   [RFC4291].  Only addresses with global scope in this block may appear
%%   on the public Internet.
%%
%%   Multicast routes must not appear in unicast routing tables.
classify_address(<<16#ff:8, Flags:4, Scope:4, Rest:14/bytes>>) ->
    {multicast, multicast_type(Flags), multicast_scope(Scope), Rest};
%% It must otherwise be a global address, or indistinguishable.
classify_address(<<_:128>>=Addr) -> {global, Addr}.

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
                                     [Addr, Type, classify_address(Addr)])
                       end,
                       begin
                           Class = classify_address(Addr),
                           Type == Class orelse
                                           (is_tuple(Class) andalso
                                            Type == element(1, Class))
                       end))).
-endif.
