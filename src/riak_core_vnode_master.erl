%% -------------------------------------------------------------------
%%
%% riak_vnode_master: dispatch to vnodes
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

%% @doc dispatch to vnodes

-module(riak_core_vnode_master).
-include_lib("riak_core_vnode.hrl").
-behaviour(gen_server).
-export([start_link/1, start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-record(idxrec, {idx, pid, monref}).
-record(state, {idxtab, sup_name, vnode_mod, legacy}).

-define(DEFAULT_TIMEOUT, 5000).


start_link(VNodeMod) -> 
    start_link(VNodeMod, undefined).

start_link(VNodeMod, LegacyMod) -> 
    gen_server:start_link({local, VNodeMod}, ?MODULE, 
                          [VNodeMod,LegacyMod], []).

%% @private
init([VNodeMod, LegacyMod]) ->
    %% Get the current list of vnodes running in the supervisor. We use this
    %% to rebuild our ETS table for routing messages to the appropriate
    %% vnode.
    VnodePids = [Pid || {_, Pid, worker, _}
                            <- supervisor:which_children(riak_core_vnode_sup)],
    IdxTable = ets:new(VNodeMod, [{keypos, 2}]),

    %% In case this the vnode master is being restarted, scan the existing
    %% vnode children and work out which module and index they are responsible
    %% for.  During startup it is possible that these vnodes may be shutting
    %% down as we check them if there are several types of vnodes active.
    PidIdxs = lists:flatten(
                [try 
                     [{Pid, riak_core_vnode:get_mod_index(Pid)}] 
                 catch
                     _:_Err ->
                         []
                 end || Pid <- VnodePids]),

    %% Populate the ETS table with processes running this VNodeMod (filtered
    %% in the list comprehension)
    F = fun(Pid, Idx) ->
                Mref = erlang:monitor(process, Pid),
                #idxrec { idx = Idx, pid = Pid, monref = Mref }
        end,
    IdxRecs = [F(Pid, Idx) || {Pid, {Mod, Idx}} <- PidIdxs, Mod =:= VNodeMod],
    true = ets:insert_new(IdxTable, IdxRecs),
    {ok, #state{idxtab=IdxTable,
                vnode_mod=VNodeMod,
                legacy=LegacyMod}}.

handle_cast({Partition, start_vnode}, State) ->
    get_vnode(Partition, State),
    {noreply, State};
handle_cast(Req=?VNODE_REQ{index=Idx}, State) ->
    Pid = get_vnode(Idx, State),
    gen_fsm:send_event(Pid, Req),
    {noreply, State};
handle_cast(Other, State=#state{legacy=Legacy}) when Legacy =/= undefined ->
    case catch Legacy:rewrite_cast(Other) of
        {ok, ?VNODE_REQ{}=Req} ->
            handle_cast(Req, State);
        _ ->
            {noreply, State}
    end.

handle_call(Req=?VNODE_REQ{index=Idx, sender={server, undefined, undefined}}, From, State) ->
    Pid = get_vnode(Idx, State),
    gen_fsm:send_event(Pid, Req?VNODE_REQ{sender={server, undefined, From}}),
    {noreply, State};
handle_call({spawn, 
             Req=?VNODE_REQ{index=Idx, sender={server, undefined, undefined}}}, From, State) ->
    Pid = get_vnode(Idx, State),
    Sender = {server, undefined, From},
    spawn_link(
      fun() -> gen_fsm:send_all_state_event(Pid, Req?VNODE_REQ{sender=Sender}) end),
    {noreply, State};
handle_call(all_nodes, _From, State) ->
    {reply, lists:flatten(ets:match(State#state.idxtab, {idxrec, '_', '$1', '_'})), State};
handle_call({Partition, get_vnode}, _From, State) ->
    Pid = get_vnode(Partition, State),
    {reply, {ok, Pid}, State};
handle_call(Other, From, State=#state{legacy=Legacy}) when Legacy =/= undefined ->
    case catch Legacy:rewrite_call(Other, From) of
        {ok, ?VNODE_REQ{}=Req} ->
            handle_call(Req, From, State);
        _ ->
            {noreply, State}
    end.

handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    delmon(MonRef, State),
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> 
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->  {ok, State}.

%% @private
idx2vnode(Idx, _State=#state{idxtab=T}) ->
    case ets:match(T, {idxrec, Idx, '$1', '_'}) of
        [[VNodePid]] -> VNodePid;
        [] -> no_match
    end.

%% @private
delmon(MonRef, _State=#state{idxtab=T}) ->
    ets:match_delete(T, {idxrec, '_', '_', MonRef}).

%% @private
add_vnode_rec(I,  _State=#state{idxtab=T}) -> ets:insert(T,I).

%% @private
get_vnode(Idx, State=#state{vnode_mod=Mod}) ->
    case idx2vnode(Idx, State) of
        no_match ->
            {ok, Pid} = riak_core_vnode_sup:start_vnode(Mod, Idx),
            MonRef = erlang:monitor(process, Pid),
            add_vnode_rec(#idxrec{idx=Idx,pid=Pid,monref=MonRef}, State),
            Pid;
        X -> X
    end.
