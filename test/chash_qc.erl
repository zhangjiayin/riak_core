%%% @author Joseph DeVivo <>
%%% @copyright (C) 2013, Joseph DeVivo
%%% @doc
%%%
%%% @end
%%% Created : 17 Jan 2013 by Joseph DeVivo <>

-module(chash_qc).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space
-define(NODES, ['node1@127.0.0.1', 'node2@127.0.0.1', 
                'node3@127.0.0.1', 'node4@127.0.0.1', 
                'node5@127.0.0.1', 'node6@127.0.0.1',
                'node7@127.0.0.1', 'node8@127.0.0.1']).
-record(state,{hashes=[]}).

%% @doc Returns the state in which each test case starts. (Unless a different 
%%      initial state is supplied explicitly to, e.g. commands/2.)
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% @doc Command generator, S is the current state
-spec command(S :: eqc_statem:symbolic_state()) -> eqc_gen:gen(eqc_statem:call()).
command(_S) ->
    oneof([{call, ?MODULE, fresh, [nat(), node_gen()]}
           ]).

%% @doc Next state transformation, S is the current state. Returns next state.
-spec next_state(S :: eqc_statem:symbolic_state(), V :: eqc_statem:var(), 
                 C :: eqc_statem:call()) -> eqc_statem:symbolic_state().
next_state(S, V, {call, ?MODULE, fresh, _}) ->
    S#state{hashes=S#state.hashes ++ [V]};
next_state(S, _V, {call, _, _, _}) ->
    S.

%% @doc Precondition, checked before command is added to the command sequence. 
-spec precondition(S :: eqc_statem:symbolic_state(), C :: eqc_statem:call()) -> boolean().
precondition(_S, {call, _, fresh, [Num, _]}) ->
    Num > 0;
precondition(_S, {call, _, _, _}) ->
    true.
                  


%% @doc <i>Optional callback</i>, used to test a precondition during test execution.
%% -spec dynamic_precondition(S :: eqc_statem:dynamic_state(), C :: eqc_statem:call()) -> boolean().
%% dynamic_precondition(_S, {call, _, _, _}) ->
%%   true.

%% @doc Postcondition, checked after command has been evaluated
%%      Note: S is the state before next_state(S,_,C) 
-spec postcondition(S :: eqc_statem:dynamic_state(), C :: eqc_statem:call(), 
                    Res :: term()) -> boolean().
postcondition(_S, {call, ?MODULE, fresh, [NumPartitions, NodeName]}, 
              {Model, Res}) ->
    length(Model) == NumPartitions andalso 
    lists:all(fun({Index, Node}) -> Node == NodeName end, Model); 
postcondition(_S, {call, _, _, _}, _Res) ->
    true.

%% @doc <i>Optional callback</i>, Invariant, checked for each visited state 
%%      during test execution.
%% -spec invariant(S :: eqc_statem:dynamic_state()) -> boolean().
%% invariant(_S) ->
%%   true.

%% @doc <i>Optional callback</i>, Returns true if operation is blocking 
%%      in current state. 
%% -spec blocking(S :: eqc_statem:symbolic_state(), C :: eqc_statem:call()) -> boolean().
%% blocking(_S, {call, _, _, _}) ->
%%   false.

%% @doc Default generated property
-spec prop_chash() -> eqc:property().
prop_chash() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S, Res} = run_commands(?MODULE,Cmds),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                Res == ok)
            end).

node_gen() ->
    elements(?NODES).

fresh(Num, Node) ->
    Inc = chash:ring_increment(Num),
    Model = [{Index * Inc, Node} ||
                Index <- lists:seq(0,(Num-1))],
    { Model, chash:fresh(Num, Node) }.










