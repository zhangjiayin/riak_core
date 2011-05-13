%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
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
-module(riak_core).
-export([stop/0, stop/1]).
-export([register_vnode_module/1, vnode_modules/0]).
-export([add_guarded_event_handler/3, add_guarded_event_handler/4]).
-export([delete_guarded_event_handler/3]).

-export_type([quorum_type/0,
              symbolic_rw_val/0,
              integer_rw_val/0,
              n_val/0, 
              r_val/0,
              w_val/0,
              rw_val/0,
              dw_val/0,
              pw_val/0,
              pr_val/0,
              pos_quorum_val/0,
              non_neg_quorum_val/0,
              quorum_val/0]).

-type quorum_type() :: r | w | rw | dw | pw | pr.
-type symbolic_rw_val() :: one | quorum | all | default.
-type pos_rw_val() :: pos_integer().
-type non_neg_rw_val() :: non_neg_integer().
-type integer_rw_val() :: pos_rw_val() | non_neg_rw_val().
-type n_val()  :: non_neg_rw_val().
-type quorum_val_t(IntType) :: symbolic_rw_val() | IntType.
-type pos_quorum_val() :: quorum_val_t(pos_rw_val()).
-type non_neg_quorum_val() :: quorum_val_t(non_neg_rw_val()).
-type r_val()  :: non_neg_quorum_val().
-type w_val()  :: non_neg_quorum_val().
-type rw_val() :: pos_quorum_val().
-type dw_val() :: non_neg_quorum_val().
-type pw_val() :: non_neg_quorum_val().
-type pr_val() :: non_neg_quorum_val().
-type quorum_val() :: pos_quorum_val() | non_neg_quorum_val().

%% @spec stop() -> ok
%% @doc Stop the riak application and the calling process.
stop() -> stop("riak stop requested").

-ifdef(TEST).
stop(Reason) ->
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    % if we're in test mode, we don't want to halt the node, so instead
    % we just stop the application.
    application:stop(riak_core).
-else.
-spec stop(term()) -> no_return().
stop(Reason) ->
    % we never do an application:stop because that makes it very hard
    %  to really halt the runtime, which is what we need here.
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    init:stop().
-endif.

-spec vnode_modules() -> [] | [{atom(),module()}].
vnode_modules() ->
    case application:get_env(riak_core, vnode_modules) of
        undefined -> [];
        {ok, Mods} -> Mods
    end.

-spec register_vnode_module(module()) -> ok.
register_vnode_module(VNodeMod) when is_atom(VNodeMod)  ->
    {ok, App} = case application:get_application(self()) of
        {ok, AppName} -> {ok, AppName};
        undefined -> app_for_module(VNodeMod)                        
    end,
    case application:get_env(riak_core, vnode_modules) of
        undefined ->
            application:set_env(riak_core, vnode_modules, [{App,VNodeMod}]);
        {ok, Mods} ->
            application:set_env(riak_core, vnode_modules, [{App,VNodeMod}|Mods])
    end,
    riak_core_ring_events:force_sync_update().
    
%% @spec add_guarded_event_handler(HandlerMod, Handler, Args) -> AddResult
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = list()
%%       AddResult = ok | {error, Reason::term()}
add_guarded_event_handler(HandlerMod, Handler, Args) ->
    add_guarded_event_handler(HandlerMod, Handler, Args, undefined).

%% @spec add_guarded_event_handler(HandlerMod, Handler, Args, ExitFun) -> AddResult
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = list()
%%       ExitFun = fun(Handler, Reason::term())
%%       AddResult = ok | {error, Reason::term()}
%%
%% @doc Add a "guarded" event handler to a gen_event instance.  
%%      A guarded handler is implemented as a supervised gen_server 
%%      (riak_core_eventhandler_guard) that adds a supervised handler in its 
%%      init() callback and exits when the handler crashes so it can be
%%      restarted by the supervisor.
add_guarded_event_handler(HandlerMod, Handler, Args, ExitFun) ->
    riak_core_eventhandler_sup:start_guarded_handler(HandlerMod, Handler, Args, ExitFun).

%% @spec delete_guarded_event_handler(HandlerMod, Handler, Args) -> Result
%%       HandlerMod = module()
%%       Handler = module() | {module(), term()}
%%       Args = term()
%%       Result = term() | {error, module_not_found} | {'EXIT', Reason}
%%       Reason = term()
%%
%% @doc Delete a guarded event handler from a gen_event instance.
%% 
%%      Args is an arbitrary term which is passed as one of the arguments to 
%%      Module:terminate/2.
%%
%%      The return value is the return value of Module:terminate/2. If the 
%%      specified event handler is not installed, the function returns 
%%      {error,module_not_found}. If the callback function fails with Reason, 
%%      the function returns {'EXIT',Reason}.
delete_guarded_event_handler(HandlerMod, Handler, Args) ->
    riak_core_eventhandler_sup:stop_guarded_handler(HandlerMod, Handler, Args).

app_for_module(Mod) ->
    app_for_module(application:which_applications(), Mod).

app_for_module([], _Mod) ->
    undefined;
app_for_module([{App,_,_}|T], Mod) ->
    {ok, Mods} = application:get_key(App, modules),
    case lists:member(Mod, Mods) of
        true -> {ok, App};
        false -> app_for_module(T, Mod)
    end.
