%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_ring_util).

-export([assign/2,
         check_ring/0,
         check_ring/1,
         multiclaim/2,
         multiclaim_rebalance/2,
         compare/2,
         diagonal/2,
         set_ring_node/2]).

%% @doc Forcibly assign a partition to a specific node
assign(Partition, ToNode) ->
    F = fun(Ring, _) ->
                {new_ring, riak_core_ring:transfer_node(Partition, ToNode, Ring)}
        end,
    {ok, NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
    %% TODO: ring_trans really should kick off a write of the ringfile, but
    %% currently doesn't. Do so manually.
    riak_core_ring_manager:do_write_ringfile(NewRing),
    ok.

%% @doc Check the local ring for any preflists that do not satisfy n_val
check_ring() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    check_ring(R).

%% @doc Check a ring for any preflists that do not satisfy n_val
check_ring(Ring) ->
    {ok, Props} = application:get_env(riak_core, default_bucket_props),
    {n_val, Nval} = lists:keyfind(n_val, 1, Props),
    Preflists = riak_core_ring:all_preflists(Ring, Nval),
    lists:foldl(fun(PL,Acc) ->
                        PLNodes = lists:usort([Node || {_,Node} <- PL]),
                        case length(PLNodes) of
                            Nval ->
                                Acc;
                            _ ->
                                ordsets:add_element(PL, Acc)
                        end
                end, [], Preflists).

%% Simulate what would happen if a new node was added and each
%% had a chance to claim partitions in order.  This is *not* what
%% happens in real life due to gossip.
multiclaim(Ring, []) ->
    Ring;
multiclaim(Ring, [Node | Nodes]) ->
    case riak_core_claim:default_wants_claim(Ring, Node) of
        {yes, _} ->
            NewRing = riak_core_claim:default_choose_claim(Ring, Node),
            multiclaim(multiclaim_rebalance(NewRing, 100), Nodes);
        no ->
            multiclaim(Ring, Nodes)
    end.    

%% Given a ring, let each node have a go at rebalancing until
%% nobody wants to change.
multiclaim_rebalance(R, 0) ->
    io:format("Warning: Could not balance with ~p nodes\n",
              [length(riak_core_ring:all_members(R))]),
    R;
multiclaim_rebalance(R, Tries) ->
    Choosers = lists:filter(fun(N1) ->
                                 riak_core_claim:default_wants_claim(R, N1) /= no
                         end,
                         lists:usort(riak_core_ring:all_members(R))),
    case Choosers of 
        [] ->
            R;
        _ ->
            NewRing = lists:foldl(fun(N2,R2) ->
                                          riak_core_claim:default_choose_claim(R2, N2)
                                  end, R, Choosers),
            multiclaim_rebalance(NewRing, Tries - 1)
    end.

%% Compare two rings and show for each node how many partitions it
%% now owns, how many it lost and how many it gained.
%% n.b. lost + gained >= owned
compare(OldR, NewR) ->
    OldOwners = riak_core_ring:all_owners(OldR),
    NewOwners = riak_core_ring:all_owners(NewR),
    Nodes = lists:usort(riak_core_ring:all_members(OldR) ++
                            riak_core_ring:all_members(NewR)),
    [begin
         Owned = [P || {P, N1} <- OldOwners, N1 == N],
         Owns = [P || {P, N1} <- NewOwners, N1 == N],
         {N,
          owns, length(Owns),
          lost, length(Owned -- Owns),
          gained, length(Owns -- Owned)}
         end || N <- Nodes].

%% Create a 'diagonal' ring for the nodes listed, partitions will
%% be assigned to each node in order.  Make sure Q rem length(Node) >= N
%% to make balanced ring.
diagonal(Q, Nodes) ->
    R0 = riak_core_ring:fresh(Q, hd(Nodes)),
    Ps = [P || {P,_} <- riak_core_ring:all_owners(R0)],
    {R, _} = lists:foldl(fun(P1, {R1, [NextN | RestN]}) ->
                                 {riak_core_ring:transfer_node(P1, NextN, R1),
                                  RestN ++ [NextN]}
                         end, {R0, Nodes}, Ps),
    R.

%% Set the ring node name
set_ring_node(R, Node) ->
    setelement(2, R, Node).
