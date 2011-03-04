%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc A module to provide access to riak_core configuration information.
%% @type riak_core_bucketprops() = [{Propkey :: atom(), Propval :: term()}]

-module(riak_core_config).

-author('Kelly McLaughlin <kelly@basho.com>').

-export([choose_claim_fun/0,
         cluster_name/0,
         default_bucket_props/0,
         default_bucket_props/1,
         disable_http_nagle/0,
         gossip_interval/0,
         handoff_concurrency/0,
         handoff_ip/0,
         handoff_port/0,
         http_ip_and_port/0,
         http_logdir/0,
         https/0,
         ring_state_dir/0,
         ring_creation_size/0,      
         target_n_val/0,
         ssl/0,
         vnode_inactivity_timeout/0,
         wants_claim_fun/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_TIMEOUT, 60000).

%% ===================================================================
%% Public API
%% ===================================================================

%% @spec choose_claim_fun() -> {atom(), atom()} | undefined
%% @doc Get the choose_claim_fun environment variable.
choose_claim_fun() ->
    get_riak_core_env(choose_claim_fun).

%% @spec cluster_name() -> string() | undefined
%% @doc Get the cluster_name environment variable.
cluster_name() ->
    get_riak_core_env(cluster_name).

%% @spec default_bucket_props() -> BucketProps::riak_core_bucketprops() | undefined
%% @doc Get the default_bucket_props environment variable.
default_bucket_props() ->
    get_riak_core_env(default_bucket_props, []).

%% @spec default_bucket_props(BucketProps::riak_core_bucketprops()) -> ok
%% @doc Set the default_bucket_props environment variable.
default_bucket_props(BucketProps) ->
    set_riak_core_env(default_bucket_props, BucketProps).

%% @spec disable_http_nagle() -> boolean()
%% @doc Get the disable_http_nagle environment variable.
disable_http_nagle() ->
    get_riak_core_env(disable_http_nagle, false).

%% @spec gossip_interval() -> integer() | undefined
%% @doc Get the gossip_interval environment variable.
gossip_interval() ->
    get_riak_core_env(gossip_interval).

%% @spec handoff_concurrency() -> integer()
%% @doc Get the handoff_concurrency environment variable.
handoff_concurrency() ->
    get_riak_core_env(handoff_concurrency, 4).

%% @spec handoff_ip() -> string() | undefined
%% @doc Get the handoff_ip environment variable.
handoff_ip() ->
    get_riak_core_env(handoff_ip).

%% @spec handoff_port() -> integer() | undefined
%% @doc Get the handoff_port environment variable.
handoff_port() ->
    get_riak_core_env(handoff_port).

%% @spec http() -> [{string(), integer()} | ...] | error
%% @doc Get the list IP addresses and port numbers for the http environment variable.
http() ->
    case get_riak_core_env(http) of
        [] ->
            error;
        undefined ->
            %% Fallback to pre-0.14 HTTP config.                                                                                                                        
            %% TODO: Remove in 0.16                                                                                                                                     
            WebIp = get_riak_core_env(web_ip),
            WebPort = get_riak_core_env(web_port),
            if
                WebIp == undefined ->
                    error;
                WebPort == undefined ->
                    error;
                true ->
                    error_logger:warning_msg(
                      "app.config is using old-style {web_ip, ~p} and"
                      " {web_port, ~p} settings in its riak_core configuration.~n"
                      "These are now deprecated, and will be removed in a"
                      " future version of Riak.~n"
                      "Please migrate to the new-style riak_core configuration"
                      " of {http, [{~p, ~p}]}.~n",
                      [WebIp, WebPort, WebIp, WebPort]),
                    [{WebIp, WebPort}]
            end;
        HttpConfig ->
            HttpConfig
    end.

%% @spec http_ip_and_port() -> {string(), integer()} | error
%% @doc Get the first configured HTTP IP address and port number.
http_ip_and_port() ->
    case http() of
        [{WebIp, WebPort} | _] ->            
            {WebIp, WebPort};
        error ->
            error
    end.


http_logdir() ->
    get_riak_core_env(http_logdir, "log").

%% @spec https() -> [{string(), integer()} | ...] | error
%% @doc Get the list IP addresses and port numbers for the https environment variable.
https() ->
    case get_riak_core_env(https) of
        [] ->
            error;
        undefined ->
            error;
        HttpsConfig ->
            HttpsConfig
    end.

%% @spec ring_creation_size() -> integer() | undefined
%% @doc Get the ring_creation_size environment variable.
ring_creation_size() ->
    get_riak_core_env(ring_creation_size).  

%% @spec ring_state_dir() -> string() | undefined
%% @doc Get the ring_state_dir environment variable.
ring_state_dir() ->
    get_riak_core_env(ring_state_dir).

%% @spec ssl() -> [{atom(), string()}, {atom(), string()}] | undefined
%% @doc Get the ssl environment variable.
ssl() ->
    get_riak_core_env(ssl, [{certfile, "etc/cert.pem"}, {keyfile, "etc/key.pem"}]).

%% @spec target_n_val() -> integer() | undefined
%% @doc Get the target_n_val environment variable.
target_n_val() ->
    get_riak_core_env(target_n_val).

%% @spec vnode_inactivity_timeout() -> integer()
%% @doc Get the vnode_inactivity_timeout environment variable.
vnode_inactivity_timeout() ->
    get_riak_core_env(vnode_inactivity_timeout, ?DEFAULT_TIMEOUT).

%% @spec wants_claim_fun() -> {atom(), atom()} | undefined
%% @doc Get the wants_claim_fun environment variable.
wants_claim_fun() ->
    get_riak_core_env(wants_claim_fun).


%% @private
get_riak_core_env(Key) ->
    get_riak_core_env(Key, undefined).

%% @private
get_riak_core_env(Key, Default) ->
    case application:get_env(riak_core, Key) of
	{ok, Value} ->
            Value;
        _ ->
            Default
    end.

%% @private
set_riak_core_env(Key, Value) ->
    application:set_env(riak_core, Key, Value).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

riak_core_config_test_() ->
    { setup,
      fun setup/0,
      fun cleanup/1,
      [
       fun choose_claim_fun_test_case/0,
       fun cluster_name_test_case/0,
       fun default_bucket_props_test_case/0,
       fun disable_http_nagle_test_case/0,
       fun gossip_interval_test_case/0,
       fun handoff_concurrency_test_case/0,
       fun handoff_ip_test_case/0,
       fun handoff_port_test_case/0,
       fun http_ip_and_port_test_case/0,
       fun http_logdir_test_case/0,
       fun http_test_case/0,
       fun https_test_case/0,
       fun non_existent_var_test_case/0,
       fun ring_creation_size_test_case/0,
       fun ring_state_dir_test_case/0,
       fun ssl_test_case/0,
       fun target_n_val_test_case/0,
       fun vnode_inactivity_timeout_test_case/0,
       fun wants_claim_fun_test_case/0
      ]
    }.

choose_claim_fun_test_case() ->
    ?assertEqual({riak_core_claim, default_choose_claim}, choose_claim_fun()),
    set_riak_core_env(choose_claim_fun, {riak_core_claim, never_choose_claim}),
    ?assertEqual({riak_core_claim, never_choose_claim}, choose_claim_fun()).

cluster_name_test_case() ->
    ?assertEqual("default", cluster_name()).

default_bucket_props_test_case() ->
    DefaultBucketProps = [{allow_mult,false},
                          {chash_keyfun,{riak_core_util,chash_std_keyfun}},
                          {last_write_wins,false},
                          {n_val,3},
                          {postcommit,[]},
                          {precommit,[]}],
    ?assertEqual([], default_bucket_props()),
    set_riak_core_env(default_bucket_props, DefaultBucketProps),
    ?assertEqual(DefaultBucketProps, default_bucket_props()).

disable_http_nagle_test_case() ->
    ?assertEqual(false, disable_http_nagle()),
    set_riak_core_env(disable_http_nagle, true),
    ?assertEqual(true, disable_http_nagle()).

gossip_interval_test_case() ->
    %% Explicitly set the value because other
    %% unit tests change the default.
    set_riak_core_env(gossip_interval, 60000),
    ?assertEqual(60000, gossip_interval()).

handoff_concurrency_test_case() ->
    ?assertEqual(4, handoff_concurrency()),
    set_riak_core_env(handoff_concurrency, 16),
    ?assertEqual(16, handoff_concurrency()).

handoff_ip_test_case() ->
    ?assertEqual("0.0.0.0", handoff_ip()),
    set_riak_core_env(handoff_ip, "127.0.0.1"),
    ?assertEqual("127.0.0.1", handoff_ip()).

handoff_port_test_case() ->
    ?assertEqual(8099, handoff_port()),
    set_riak_core_env(handoff_port, 1000),
    ?assertEqual(1000, handoff_port()).

http_logdir_test_case() ->
    ?assertEqual("log", http_logdir()),
    set_riak_core_env(http_logdir, "/var/log"),
    ?assertEqual("/var/log", http_logdir()).

http_ip_and_port_test_case() ->
    application:set_env(riak_core, http, undefined),
    set_riak_core_env(web_ip, undefined),
    set_riak_core_env(web_port, undefined),
    ?assertEqual(error, http_ip_and_port()),
    %% Test the pre-0.14 style config
    set_riak_core_env(web_ip, "127.0.0.1"),
    set_riak_core_env(web_port, 8098),
    ?assertEqual({"127.0.0.1", 8098}, http_ip_and_port()),
    %% Test the config for 0.14 and later
    set_riak_core_env(http, [{"localhost", 9000}]),
    ?assertEqual({"localhost", 9000}, http_ip_and_port()).

http_test_case() ->
    set_riak_core_env(http, undefined),
    set_riak_core_env(web_ip, undefined),
    set_riak_core_env(web_port, undefined),
    ?assertEqual(error, http()),
    %% Test the pre-0.14 style config
    set_riak_core_env(web_ip, "127.0.0.1"),
    set_riak_core_env(web_port, 8098),
    ?assertEqual([{"127.0.0.1", 8098}], http()),
    %% Test the config for 0.14 and later
    set_riak_core_env(http, [{"localhost", 9000}, {"127.0.0.1", 10000}]),
    ?assertEqual([{"localhost", 9000}, {"127.0.0.1", 10000}], http()).

https_test_case() ->
    set_riak_core_env(https, undefined),
    ?assertEqual(error, https()),
    set_riak_core_env(https, [{"localhost", 9000}, {"127.0.0.1", 10000}]),
    ?assertEqual([{"localhost", 9000}, {"127.0.0.1", 10000}], https()).

non_existent_var_test_case() ->
    ?assertEqual(undefined, get_riak_core_env(bogus)).

ring_creation_size_test_case() ->
    %% Explicitly set the value because other
    %% unit tests change the default.
    set_riak_core_env(ring_creation_size, 64),
    ?assertEqual(64, ring_creation_size()).

ring_state_dir_test_case() ->
    ?assertEqual("data/ring", ring_state_dir()).

ssl_test_case() ->
    ?assertEqual([{certfile, "etc/cert.pem"}, {keyfile, "etc/key.pem"}], ssl()),
    set_riak_core_env(ssl, [{certfile, "testcert"}, {keyfile, "testkey"}]),
    ?assertEqual([{certfile, "testcert"}, {keyfile, "testkey"}], ssl()).

target_n_val_test_case() ->
    ?assertEqual(4, target_n_val()).
    
vnode_inactivity_timeout_test_case() ->
    ?assertEqual(?DEFAULT_TIMEOUT, vnode_inactivity_timeout()),
    set_riak_core_env(vnode_inactivity_timeout, 100000),
    ?assertEqual(100000, vnode_inactivity_timeout()).

wants_claim_fun_test_case() ->
    ?assertEqual({riak_core_claim, default_wants_claim}, wants_claim_fun()),
    set_riak_core_env(wants_claim_fun, {riak_core_claim, never_wants_claim}),
    ?assertEqual({riak_core_claim, never_wants_claim}, wants_claim_fun()).    

setup() ->   
    application:load(riak_core).

cleanup(_Pid) ->
    application:unload(riak_core).
    
-endif.
