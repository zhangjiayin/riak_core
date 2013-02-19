%% -------------------------------------------------------------------
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
%% @doc Manages proxy processes that forward all messages to another
%% process.

-module(process_proxy).
-export([start_link/2, init/1, stop/1]).

%% @doc Starts a proxy process registered as `RegName' and forwarding to `ProxyTo'
-spec start_link(RegName::atom(), ProxyTo::(pid() | atom())) -> {ok, pid()} | {error, timeout}.
start_link(RegName, ProxyTo) ->
    proc_lib:start_link(?MODULE, init, [[self(), RegName, ProxyTo]]).

%% @private Internal process initialization
-spec init(list()) -> no_return().
init([ParentPid, RegName, ProxyTo]) ->
    erlang:register(RegName, self()),
    proc_lib:init_ack(ParentPid, {ok, self()}),
    loop(ProxyTo).

%% @doc Stops the proxy process
-spec stop(Name::atom()) -> stop.
stop(Name) ->
    Name ! stop.

-spec loop(pid() | atom()) -> no_return.
loop(ProxyTo) ->
    receive
        stop ->
            exit(normal);
        M ->
            ProxyTo ! M,
            loop(ProxyTo)
    end.
