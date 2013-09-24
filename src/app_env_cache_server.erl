-module(app_env_cache_server).

-behaviour(gen_server).

%% API
-export([start_link/0,
         set_env/2,
         set_env/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {config}).

%-define(USE_MOCHIGLOBAL, true).
%-define(USE_FAST_MOCHIGLOBAL, true).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    Res = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    app_helper:cache_env(riak_core),
    Res.

set_env(App, Key, Val) ->
    gen_server:call(?MODULE, {set_env, App, Key, Val}).

set_env(App, KeyVals) ->
    gen_server:call(?MODULE, {set_env, App, KeyVals}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({set_env, App, Key, Val}, _From, State) ->
    application:set_env(App, Key, Val),
    cache_put(App, Key, Val),
    {reply, ok, State};
handle_call({set_env, App, KeyVals}, _From, State) ->
    [application:set_env(App, Key, Val) || {Key, Val} <- KeyVals],
    cache_put(App, KeyVals),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-ifdef(USE_MOCHIGLOBAL).
cache_put(App, Key, Val) ->
    Name = list_to_atom(atom_to_list(App) ++ ":" ++ atom_to_list(Key)),
    mochiglobal:put(Name, Val).
cache_put(App, KeyVals) ->
    [cache_put(App, Key, Val) || {Key, Val} <- KeyVals].
-else.
-ifdef(USE_FAST_MOCHIGLOBAL).

cache_put(App, Key, Val) ->
    Mod = list_to_atom("mochiglobal:" ++ atom_to_list(App)
                       ++ ":" ++ atom_to_list(Key)),
    Mod:term().

cache_put(App, KeyVals) ->
    [cache_put(App, Key, Val) || {Key, Val} <- KeyVals].
-else.
cache_put(App, Key, Val) ->
    cache_put(App, [{Key, Val}]).

cache_put(App, KeyVals) ->
    Cache = app_env_cache:get_all(),
    NewCache = lists:foldl(fun({Key, Val}, Dict) ->
                    dict:store({App, Key}, Val, Dict) end,
                           Cache, KeyVals),
    Bin = compile(NewCache),
    code:purge(app_env_cache),
    code:load_binary(app_env_cache, "app_env_cache.erl", Bin),
    lager:info("Put Vals ~p = ~p", [App, KeyVals]),
    ok.

-spec compile(any()) -> binary().
compile(T) ->
    {ok, _Module, Bin} = compile:forms(forms(T),
                                       [verbose, report_errors]),
    Bin.

-spec forms(any()) -> [erl_syntax:syntaxTree()].
forms(T) ->
    [erl_syntax:revert(X) || X <- term_to_abstract(T)].

-spec term_to_abstract(any()) -> [erl_syntax:syntaxTree()].
term_to_abstract(T) ->
    S = erl_syntax,
    D = S:variable('D'),
    R = S:variable('R'),
    Dict = S:abstract(T),
    App = S:variable('App'),
    Key = S:variable('Key'),
    Val = S:variable('Val'),
    Ok = S:atom(ok),
    AppKey = S:tuple([App, Key]),
    OkVal = S:tuple([Ok, Val]),
    DictFind = S:application(S:atom(dict), S:atom(find),
                             [AppKey, D]),
    GetAll = S:application(S:atom(app_env_cache), S:atom(get_all), []),
    [S:attribute(S:atom(module),[S:atom(app_env_cache)]),
     S:attribute(S:atom(export),
                 [S:list([S:arity_qualifier(S:atom(get_env),
                                            S:integer(2)),
                          S:arity_qualifier(S:atom(get_all),
                                            S:integer(0))])]),
     S:function(S:atom(get_all), [S:clause([], none, [Dict])]),
     S:function(
            S:atom(get_env),
            [S:clause([App, Key],
                      none,
                      [S:match_expr(D, GetAll),
                       S:match_expr(R, DictFind),
                       S:case_expr(R,
                                   [S:clause([OkVal], none, [OkVal]),
                                    S:clause([S:underscore()],
                                             none,
                                             [S:atom(not_found)])
                                   ])
                      ])
            ]
            )
    ].

-endif.
-endif.
