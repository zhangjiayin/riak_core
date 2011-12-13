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
         ensure_vnodes_started/1,
         ensure_vnodes_started/2]).

%% @doc Forcibly assign a partition to a specific node
assign(Partition, ToNode) ->
    F = fun(Ring, _) ->
                {new_ring, riak_core_ring:transfer_node(Partition, ToNode, Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, undefined),
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


ensure_vnodes_started(Ring) ->
    case riak_core:vnode_modules() of
        [] ->
            ok;
        AppMods ->
            case ensure_vnodes_started(AppMods, Ring, []) of
                [] ->
                    Legacy = riak_core_gossip:legacy_gossip(),
                    Ready = riak_core_ring:ring_ready(Ring),
                    FutureIndices = riak_core_ring:future_indices(Ring, node()),
                    Status = riak_core_ring:member_status(Ring, node()),
                    case {Legacy, Ready, FutureIndices, Status} of
                        {true, _, _, _} ->
                            riak_core_ring_manager:refresh_my_ring();
                        {_, true, [], leaving} ->
                            riak_core_ring_manager:ring_trans(
                              fun(Ring2, _) -> 
                                      Ring3 = riak_core_ring:exit_member(node(), Ring2, node()),
                                      {new_ring, Ring3}
                              end, []),
                            %% Shutdown if we are the only node in the cluster
                            case riak_core_ring:random_other_node(Ring) of
                                no_node ->
                                    riak_core_ring_manager:refresh_my_ring();
                                _ ->
                                    ok
                            end;
                        {_, _, _, invalid} ->
                            riak_core_ring_manager:refresh_my_ring();
                        {_, _, _, exiting} ->
                            %% Deliberately do nothing.
                            ok;
                        {_, _, _, _} ->
                            ok
                    end;
                _ -> ok
            end
    end.

ensure_vnodes_started([], _Ring, Acc) ->
    lists:flatten(Acc);
ensure_vnodes_started([{App, Mod}|T], Ring, Acc) ->
    ensure_vnodes_started(T, Ring, [ensure_vnodes_started({App,Mod},Ring)|Acc]).

%% For a given App, Mod, ensure the locally owned vnodes are started
ensure_vnodes_started({App,Mod}, Ring) ->
    Startable = startable_vnodes(Mod, Ring),
    %% NOTE: This following is a hack.  There's a basic
    %%       dependency/race between riak_core (want to start vnodes
    %%       right away to trigger possible handoffs) and riak_kv
    %%       (needed to support those vnodes).  The hack does not fix
    %%       that dependency: internal techdebt todo list #A7 does.

    {Pid, Mref} = spawn_monitor(fun() ->
    %%                 Use a registered name as a lock to prevent the same
    %%                 vnode module from being started twice.
                       RegName = list_to_atom(
                                   "riak_core_ring_handler_ensure_"
                                   ++ atom_to_list(Mod)),
                       try register(RegName, self())
                       catch error:badarg ->
                               exit({shutdown, already_starting})
                       end,
                       wait_for_app(App, 100, 100),
                       [Mod:start_vnode(I) || I <- Startable],
                       exit(normal)
               end),

    %% Check how long to wait for startup
    VnodesStartupTimeout = app_helper:get_env(riak_core,
                                              vnode_startup_timeout,
                                              600000),
    receive
	{'DOWN', Mref, _Type, Pid, Reason} when Reason == normal;
                                                Reason == {shutdown, already_starting} ->
           lager:info("Ensured ~p vnodes started", [App]),
           ok;
	{'DOWN', Mref, _Type, Pid, Reason} ->
            lager:error("Unable to ensure all ~p vnodes started: ~p",
                        [App, Reason])
    after
        VnodesStartupTimeout ->
            lager:error("Timed out starting ~p vnodes after ~p milliseconds.  "
                        "Increase {riak_core, vnode_startup_timeout} if needed",
                       [VnodesStartupTimeout])
    end,
    Startable.

startable_vnodes(Mod, Ring) ->
    AllMembers = riak_core_ring:all_members(Ring),
    case {length(AllMembers), hd(AllMembers) =:= node()} of
        {1, true} ->
            riak_core_ring:my_indices(Ring);
        _ ->
            {ok, ModExcl} = riak_core_handoff_manager:get_exclusions(Mod),
            Excl = ModExcl -- riak_core_ring:disowning_indices(Ring, node()),
            case riak_core_ring:random_other_index(Ring, Excl) of
                no_indices ->
                    case length(Excl) =:= riak_core_ring:num_partitions(Ring) of
                        true ->
                            [];
                        false ->
                            riak_core_ring:my_indices(Ring)
                    end;
                RO ->
                    [RO | riak_core_ring:my_indices(Ring)]
            end
    end.
