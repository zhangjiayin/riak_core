%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% Test basic cluster join/leave/remove behavior.
%%
%% This test is designed to run against a running set of singleton Riak nodes,
%% and will be skipped if the following environment variables are not set:
%% (example values corresponding to standard dev release follow)
%%   RIAK_TEST_NODE_1="dev1@127.0.0.1"
%%   RIAK_TEST_NODE_2="dev2@127.0.0.1"
%%   RIAK_TEST_NODE_3="dev3@127.0.0.1"
%%   RIAK_TEST_NODE_4="dev4@127.0.0.1"
%%   RIAK_TEST_COOKIE="riak"
%%   RIAK_EUNIT_NODE="eunit@127.0.0.1"

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-module(verify_cluster_join_leave).
-compile(export_all).

-define(OUT(S), ?debugFmt(S,[])).
-define(OUT(S,A), ?debugFmt(S,A)).

systest_test_() ->
    Envs =["RIAK_TEST_NODE_1", "RIAK_TEST_NODE_2", "RIAK_TEST_NODE_3",
           "RIAK_TEST_NODE_4", "RIAK_TEST_COOKIE", "RIAK_EUNIT_NODE"],
    Vals = [os:getenv(Env) || Env <- Envs],
    case lists:member(false, Vals) of
        true ->
            ?debugFmt("Skipping ~p tests~n", [?MODULE]),
            [];
        false ->
            [NodeV1, NodeV2, NodeV3, NodeV4, CookieV, ENodeV] = Vals,
            Node1 = list_to_atom(NodeV1),
            Node2 = list_to_atom(NodeV2),
            Node3 = list_to_atom(NodeV3),
            Node4 = list_to_atom(NodeV4),
            Cookie = list_to_atom(CookieV),
            ENode = list_to_atom(ENodeV),

            {spawn, [{setup,
                      fun() ->
                              os:cmd("epmd -daemon"),
                              net_kernel:start([ENode]),
                              erlang:set_cookie(node(), Cookie),
                              Nodes = [Node1, Node2, Node3, Node4],
                              lists:sort(Nodes)
                      end,
                      fun(_) ->
                              ok
                      end,
                      fun(Args) ->
                              [{timeout, 150, ?_test(join_leave_case(Args))}]
                      end
                     }]}
    end.

join_leave_case(Nodes) ->
    %% Ensure each node owns 100% of it's own ring
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    %% Join nodes
    [Node1|OtherNodes] = Nodes,
    [join(Node, Node1) || Node <- OtherNodes],

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),

    %% Ensure each node owns a portion of the ring
    [?assertEqual(Nodes, owners_according_to(Node)) || Node <- Nodes],

    %% Have node2 leave
    Node2 = lists:nth(2, Nodes),
    leave(Node2),
    ?assertEqual(ok, wait_until_unpingable(Node2)),

    %% Verify node2 no longer owns partitions, all node believe it invalid
    Remaining1 = Nodes -- [Node2],
    [?assertEqual(Remaining1, owners_according_to(Node)) || Node <- Remaining1],
    [?assertEqual(invalid, status_of_according_to(Node2, Node)) || Node <- Remaining1],

    %% Have node1 remove node3
    Node3 = lists:nth(3, Nodes),
    remove(Node1, Node3),
    ?assertEqual(ok, wait_until_unpingable(Node3)),

    %% Verify node3 no longer owns partitions, all node believe it invalid
    Remaining2 = Remaining1 -- [Node3],
    [?assertEqual(Remaining2, owners_according_to(Node)) || Node <- Remaining2],
    [?assertEqual(invalid, status_of_according_to(Node3, Node)) || Node <- Remaining2],

    ok.

owners_according_to(Node) ->    
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
    lists:usort(Owners).

status_of_according_to(Member, Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Status = riak_core_ring:member_status(Ring, Member),
    Status.

join(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, join, [[atom_to_list(OtherNode)]]).

leave(Node) ->
    rpc:call(Node, riak_kv_console, leave, [[]]).

remove(Node, OtherNode) ->
    rpc:call(Node, riak_kv_console, remove, [[atom_to_list(OtherNode)]]).

wait_until_nodes_ready(Nodes) ->
    [?assertEqual(ok, wait_until(Node, fun is_ready/1, 10)) || Node <- Nodes],
    ok.

is_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            lists:member(Node, riak_core_ring:ready_members(Ring));
        _ ->
            false
    end.

wait_until_no_pending_changes(Nodes) ->
    [?assertEqual(ok, wait_until(Node, fun are_no_pending/1, 10)) || Node <- Nodes],
    ok.

are_no_pending(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    riak_core_ring:pending_changes(Ring) =:= [].

wait_until_unpingable(Node) ->
    F = fun(N) ->
                net_adm:ping(N) =:= pang
        end,
    ?assertEqual(ok, wait_until(Node, F, 600)),
    ok.

wait_until(Node, Fun, Retry) ->
    Pass = Fun(Node),
    case {Retry, Pass} of
        {_, true} ->
            ok;
        {0, _} ->
            fail;
        _ ->
            timer:sleep(500),
            wait_until(Node, Fun, Retry-1)
    end.

-endif.
