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
-module(riak_core_pl_cache).
-behavior(gen_server).

%% API
-export([invalidate/0,
         preflist/4,
         start_link/0]).

%% Callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% -------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

%% @doc Invalidate the cache.
-spec invalidate() -> ok.
invalidate() -> gen_server:cast(?MODULE, invalidate).

%% @doc Attempt to retrieve the preflist from the cache.  Otherwise
%% generate from ring and send async command to write to cache.
-spec preflist(primary | p_and_f | ann, binary(), pos_integer(), atom()) ->
                      riak_core_apl:preflist() | riak_core_apl:preflist2().
preflist(Type, Idx, N, Service) ->
    I = chash:index_to_int(Idx),
    K = {I, N, Service},
    try
        case ets:lookup(?MODULE, K) of
            [{K,Preflist}] -> Preflist;
            [] -> calc_and_write(Type, Idx, K)
        end
    catch _:Err ->
            %% The reason for try/catch here is that you don't want
            %% preflist requestor to fail because the ETS table or
            %% it's owner don't exist.
            lager:error("error fetching preflist for ~p with reason ~p ~p",
                        [K, Err, erlang:get_stacktrace()]),
            calc_and_write(Type, Idx, K)
    end.

%% @doc Start a new cache server.  Meant to be called by the
%% supervisor.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% -------------------------------------------------------------------
%% Callbacks
%% -------------------------------------------------------------------

init([]) ->
    ets:new(?MODULE, [protected, named_table, {read_concurrency, true}]),
    {ok, none}.

handle_call(_Msg, _From, _State) ->
    {noreply, _State}.

%% TODO pass ring vclock to determine if write really needs to happen.
handle_cast({cache_entry, _RingVClock, Key, Preflist}, _State) ->
    ets:insert(?MODULE, {Key, Preflist}),
    {noreply, _State};
handle_cast(invalidate, _State) ->
    %% TODO debug level?
    lager:info("Preflist cache was invalidated"),
    ets:delete_all_objects(?MODULE),
    {noreply, _State}.

handle_info(_Msg, _State) -> {noreply, _State}.

terminate(_Reason, _State) -> ignored.

code_change(_OldVsn, _State, _Extra) -> {ok, _State}.

%% -------------------------------------------------------------------
%% Private
%% -------------------------------------------------------------------

%% @private
%%
%% @doc Calculate the preflist from the ring and send a request to
%% file a new cache entry.
-spec calc_and_write(primary | both, binary(), {non_neg_integer(), pos_integer(), atom()}) ->
                            riak_core_apl:preflist().
calc_and_write(Type, Idx, {_I, N, Service}=Key) ->
    Preflist = preflist_from_ring(Type, Idx, N, Service),
    gen_server:cast(?MODULE, {cache_entry, vclock, Key, Preflist}),
    Preflist.

%% @private
%%
%% @doc Calculate preflist from the ring.
-spec preflist_from_ring(ann | primary | both, binary(), pos_integer(), atom()) ->
                                riak_core_apl:preflist()
                                    | riak_core_apl:preflist2().
preflist_from_ring(ann, Idx, N, Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Up = riak_core_node_watcher:nodes(Service),
    riak_core_apl:get_apl_ann(Idx, N, Ring, Up);
preflist_from_ring(p_and_f, Idx, N, Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Up = riak_core_node_watcher:nodes(Service),
    riak_core_apl:get_apl(Idx, N, Ring, Up);
preflist_from_ring(primary, Idx, N, Service) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Up = riak_core_node_watcher:nodes(Service),
    riak_core_apl:get_primary_apl(Idx, N, Ring, Up).
