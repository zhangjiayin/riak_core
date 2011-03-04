%% -------------------------------------------------------------------
%%
%% riak_core_web: setup Riak's HTTP interface
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

%% @doc Convenience functions for setting up the HTTP interface
%%      of Riak.  This module loads parameters from the application
%%      environment:
%%
%%<dl><dt>  web_ip
%%</dt><dd>   IP address that the Webmachine node should listen to
%%</dd><dt> web_port
%%</dt><dd>   port that the Webmachine node should listen to
%%</dd><dt> web_logdir
%%</dt><dd>   directory under which the access log will be stored
%%</dd></dl>
-module(riak_core_web).

-export([bindings/1]).

bindings(http) ->
    case riak_core_config:http() of
        error ->
            [];
        ConfigPairs ->
            binding_configs(http, ConfigPairs)
    end;
bindings(https) ->
    case riak_core_config:https() of
        error ->
            [];
        ConfigPairs ->
            binding_configs(https, ConfigPairs)
    end.

binding_configs(Scheme, ConfigPairs) ->
    [binding_config(Scheme, Pair) || Pair <- ConfigPairs].

binding_config(Scheme, Binding) ->
  {Ip, Port} = Binding,
  Name = spec_name(Scheme, Ip, Port),
  Config = spec_from_binding(Scheme, Name, Binding),
  
  {Name,
    {webmachine_mochiweb, start, [Config]},
      permanent, 5000, worker, dynamic}.
  
spec_from_binding(http, Name, Binding) ->
  {Ip, Port} = Binding,
  NoDelay = riak_core_config:disable_http_nagle(),
  lists:flatten([{name, Name},
                  {ip, Ip},
                  {port, Port},
                  {nodelay, NoDelay}],
                  common_config());

spec_from_binding(https, Name, Binding) ->
  {Ip, Port} = Binding,
  SslOpts = riak_core_config:ssl(),
  NoDelay = riak_core_config:disable_http_nagle(),
  lists:flatten([{name, Name},
                  {ip, Ip},
                  {port, Port},
                  {ssl, true},
                  {ssl_opts, SslOpts},
                  {nodelay, NoDelay}],
                  common_config()).

spec_name(Scheme, Ip, Port) ->
  atom_to_list(Scheme) ++ "_" ++ Ip ++ ":" ++ integer_to_list(Port).

common_config() ->
  [{log_dir, riak_core_config:http_logdir()},
    {backlog, 128},
    {dispatch, [{[], riak_core_wm_urlmap, []}]}].
