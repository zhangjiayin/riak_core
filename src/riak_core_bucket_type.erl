%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_bucket_type).

-export([status/1,
         defaults/0,
         create/2,
         update/2,
         get/1,
         reset/1,
         iterator/0,
         itr_next/1,
         itr_done/1,
         itr_value/1]).

-type bucket_type()        :: binary().
-type bucket_type_props()  :: [{term(), term()}].
-type bucket_type_status() :: undefined | ok | propogating | {partial, ok | propogating, [node()]}.

-define(DEFAULT_TYPE, <<"default">>).

%% @doc returns the bucket types status. the status indicates whether or not the bucket type
%% has propogated to all nodes. Calling this function will result in an error if the claimant
%% is unreachable. If other nodes are unreachable, it will be indicated by the `partial` status tuple.
%% The third element is the list of nodes that were unreachable.
%%
%% One of the following statuses will be returned:
%%
%%   * `ok' - most recent write has propogated to all nodes. using or updating this bucket type is safe.
%%   * `propogating' - most recent write has not propogated to all nodes. If the last write was creation
%%                     then future updates on the type and using the type in applications is not recommonded
%%                     until `ok' is returned. If the last write was an update then using the type in applications
%%                     is recommended but further updates on the type should wait until `ok' is returned.
%%   * `{partial, ok, _}' - some nodes are unreachable but the most recent write has propogated to all
%%                          reachable nodes. It is not recommended to update a type when this status is
%%                          returned. The type may be used by applications safely.
%%   * `{partial, propogating, _}` - some nodes are unreachable and the most recent write is still propogating
%%                                   to the reachable nodes. It is not recommended to update a type when this
%%                                   status is returned. If the most recent write was creation the type should
%%                                   not be used by applications, otherwise it is safe.
%%
-spec status(bucket_type()) -> bucket_type_status().
status(?DEFAULT_TYPE) ->
    ok;
status(BucketType) when is_binary(BucketType)->
    case ?MODULE:get(BucketType) of
        undefined -> undefined;
        Props -> status(BucketType, Props)
    end.

status(BucketType, Props) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    Members = riak_core_ring:all_members(R),
    Claimant = riak_core_ring:claimant(R),
    {AllProps, BadNodes} = rpc:multicall(Members -- [Claimant], riak_core_bucket_type, get, [BucketType]),
    NotUpdated = [P || P <- AllProps, P =:= undefined orelse lists:ukeysort(1, P) =/= lists:ukeysort(1, Props)],
    case {NotUpdated, BadNodes} of
        {[], []} ->
            ok;
        {_, []} ->
            propogating;
        {[], _} ->
            {partial, ok, BadNodes};
        {_, _} ->
            {partial, propogating, BadNodes}
    end.

%% @doc The hardcoded defaults for all bucket types.
-spec defaults() -> bucket_type_props().
defaults() ->
      [{n_val,3},
       {allow_mult,false},
       {last_write_wins,false},
       {precommit, []},
       {postcommit, []},
       {chash_keyfun, {riak_core_util, chash_std_keyfun}}].

%% @doc Create a new bucket type. Properties provided will be merged with default
%% bucket properties set in config. Creation is serialized through the claimant.
%% This is safe for future updates as long as the claimaint does not change or when the creation
%% propogates to all nodes.
-spec create(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
create(?DEFAULT_TYPE, _Props) ->
    {error, already_exists};
create(BucketType, Props) when is_binary(BucketType) ->
    riak_core_claimant:create_bucket_type(BucketType, Props).

%% @doc Update an existing bucket type. Properties provided will be
%% merged with existing properties.  Updates are serialized through the claimant.
%% {@see status/1} for information on when it is safe to update.
-spec update(bucket_type(), bucket_type_props()) -> ok | {error, term()}.
update(?DEFAULT_TYPE, _Props) ->
    {error, no_default_update}; %% default props are in the app.config
update(BucketType, Props) when is_binary(BucketType)->
    riak_core_claimant:update_bucket_type(BucketType, Props).

%% @doc Return the properties associated with the given bucket type
-spec get(bucket_type()) -> undefined | bucket_type_props().
get(BucketType) when is_binary(BucketType) ->
    get(BucketType, undefined).

%% @private
get(BucketType, Default) ->
    riak_core_claimant:get_bucket_type(BucketType, Default).

reset(BucketType) ->
    update(BucketType, defaults()).

%% @doc Return an iterator that can be used to walk iterate through all existing bucket types
-spec iterator() -> riak_core_metadata:iterator().
iterator() ->
    riak_core_claimant:bucket_type_iterator().

%% @doc Advance the iterator to the next bucket type. itr_done/1 should always be called
%% before this function
-spec itr_next(riak_core_metadata:iterator()) ->
                      riak_core_metadata:iterator().
itr_next(It) ->
    riak_core_metadata:itr_next(It).

%% @doc Returns true if there are no more bucket types to iterate over
-spec itr_done(riak_core_metadata:iterator()) -> boolean().
itr_done(It) ->
    riak_core_metadata:itr_done(It).

%% @doc Returns the type and properties that the iterator points too. Any siblings,
%% are resolved at this time. itr_done/1 should be checked before calling this function.
-spec itr_value(riak_core_metadata:iterator()) ->
                       {bucket_type(), bucket_type_props()}.
itr_value(It) ->
    {BucketType, Props} = riak_core_metadata:itr_key_values(It),
    {BucketType, Props}.
