%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_core_claim_util).
-export([dryrun/1, dryrun_new_weights/1, update_weights/1]).

update_weights(Weights) ->
    riak_core_ring_manager:ring_trans(
      fun(Ring, _) ->
              ThisNode = node(),
              case riak_core_ring:claimant(Ring) of
                  ThisNode ->
                      NewRing =
                          lists:foldl(fun({Member, Weight}, Ring0) ->
                                              set_weight(Member, Weight, Ring0)
                                      end, Ring, Weights),
                      {new_ring, NewRing};
                  Claimant ->
                      io:format("You can only change weights from the "
                                "claimant node: ~p~n", [Claimant]),
                      ignore
              end
      end, []),
    ok.

dryrun(Weights) when is_list(Weights) ->
    Labels = lists:seq(1, length(Weights)),
    Nodes = lists:zip(Labels, Weights),
    do_dryrun(Nodes);
dryrun(NumToAdd) when is_integer(NumToAdd) ->
    Labels = lists:seq(1, NumToAdd),
    Nodes = [{Label, 1} || Label <- Labels],
    do_dryrun(Nodes).

do_dryrun(Nodes) ->
    ThisNode = node(),
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    case riak_core_ring:claimant(Ring) of
        ThisNode ->
            do_dryrun(Nodes, Ring);
        Claimant ->
            io:format("Dry runs should be run on the claimant node: ~p~n",
                      [Claimant]),
            ok
    end.

do_dryrun(Nodes, Ring0) ->
    case riak_core_ring:pending_changes(Ring0) of
        [] ->
            io:format("Current ring:~n"),
            riak_core_ring:pretty_print(Ring0, [legend]),
            lists:foldl(
              fun({N, Weight}, Ring) ->
                      Node =
                          list_to_atom(lists:flatten(io_lib:format("sim~b@basho.com",[N]))),
                      io:format("~nAdding ~p~n", [Node]),
                      Ring2 = riak_core_ring:add_member(Node, Ring, Node),
                      Ring3 = set_weight(Node, Weight, Ring2),
                      Ring4 = dryrun_claim(Ring3),
                      Ring4
              end, Ring0, Nodes);
        _ ->
            io:format("Cannot perform a dryrun on a cluster with pending "
                      "transfers~n")
    end,
    ok.

dryrun_new_weights(Weights) ->
    {ok, Ring} = riak_core_ring_manager:get_raw_ring(),
    Ring2 = lists:foldl(fun({Node, Weight}, Ring0) ->
                                set_weight(Node, Weight, Ring0)
                        end, Ring, Weights),
    io:format("Current ring:~n"),
    riak_core_ring:pretty_print(Ring, [legend]),
    dryrun_claim(Ring2),
    ok.

dryrun_claim(Ring) ->
    Members = riak_core_ring:claiming_members(Ring),
    Ring2 = lists:foldl(
              fun(Node, Ring0) ->
                      riak_core_gossip:claim_until_balanced(Ring0, Node)
              end, Ring, Members),
    Owners1 = riak_core_ring:all_owners(Ring),
    Owners2 = riak_core_ring:all_owners(Ring2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Tally =
        lists:foldl(fun({_, PrevOwner, NewOwner}, Tally) ->
                            dict:update_counter({PrevOwner, NewOwner}, 1, Tally)
                    end, dict:new(), Next),
    riak_core_ring:pretty_print(Ring2, [legend]),
    io:format("Pending: ~p~n", [length(Next)]),
    io:format("Check: ~p~n", [riak_core_ring_util:check_ring(Ring2)]),
    [io:format("~b transfers from ~p to ~p~n", [Count, PrevOwner, NewOwner])
     || {{PrevOwner, NewOwner}, Count} <- dict:to_list(Tally)],
    Ring2.

set_weight(Member, Weight, Ring) ->
    riak_core_ring:update_member_meta(node(), Ring, Member, claim_weight,
                                      Weight).
