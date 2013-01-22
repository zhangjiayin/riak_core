-module(handoff_manager_eqc).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state,{
          handoffs,
          inbound_count,
          max_concurrency
         }).

handoff_mgr_test_() ->
     {timeout, 600,
      ?_assertEqual(true, quickcheck(?QC_OUT(numtests(100, prop_handoff_mgr()))))}.

prop_handoff_mgr() ->
    ?FORALL(Cmds,commands(?MODULE),
            ?TRAPEXIT(
               aggregate(command_names(Cmds),
                         begin
                             setup(),
                             {H,S,Res} = run_commands(?MODULE,Cmds),
                             teardown(),
                             pretty_commands(?MODULE, Cmds, {H, S, Res},
                                             Res == ok)
                         end))).

setup() ->
    teardown(),
    Vars = [{ring_creation_size, 8},
            {ring_state_dir, "<nostore>"},
            {cluster_name, "test"},
            {handoff_concurrency, 2},
            {disable_outbound_handoff, false},
            {disable_inbound_handoff, false},
            %% Don't allow rolling start of vnodes as it will cause a
            %% race condition with `all_nodes'.
            {vnode_rolling_start, 0}],
    OldVars = [begin
                   Old = app_helper:get_env(riak_core, AppKey),
                   ok = application:set_env(riak_core, AppKey, Val),
                   {AppKey, Old}
               end || {AppKey, Val} <- Vars],
    %% our mock vnode don't need async pools for this test
    application:set_env(riak_core, core_vnode_eqc_pool_size, 0),
    riak_core_ring_events:start_link(), %% TODO: do we reaLly need ring events
    riak_core_ring_manager:start_link(test),
    riak_core_vnode_sup:start_link(),
    riak_core_vnode_proxy_sup:start_link(),
    riak_core_vnode_manager:start_link(),
    riak_core_handoff_manager:start_link(),
    riak_core_handoff_sender_sup:start_link(),
    riak_core_handoff_receiver_sup:start_link(),
    riak_core:register([{vnode_module, mock_vnode}]),

    meck:new(riak_core_handoff_sender),
    meck:new(riak_core_handoff_receiver),
    meck:expect(riak_core_handoff_sender, start_link,
                fun(_TargetNode, _Mod, _TypeAndOpts, _Vnode) ->
                        Pid = spawn_link(fun() ->
                                           timer:sleep(20000)
                                         end),
                        {ok, Pid}
                end),
    meck:expect(riak_core_handoff_receiver, start_link,
                fun(_SslOpts) ->
                        Pid = spawn_link(fun() ->
                                           timer:sleep(20000)
                                         end),
                        {ok, Pid}
                end),
    OldVars.

teardown() ->
    stop_pid(whereis(riak_core_ring_events)),
    stop_pid(whereis(riak_core_vnode_sup)),
    stop_pid(whereis(riak_core_vnode_proxy_sup)),
    stop_pid(whereis(riak_core_vnode_manager)),
    stop_pid(whereis(riak_core_handoff_manager)),
    stop_pid(whereis(riak_core_handoff_sender_sup)),
    stop_pid(whereis(riak_core_handoff_receiver_sup)),
    riak_core_ring_manager:stop(),
    catch meck:unload(riak_core_handoff_sender),
    catch meck:unload(riak_core_handoff_receiver).

initial_state() ->
    #state{handoffs=[],inbound_count=0,max_concurrency=2}.

precondition(_S,{call,_,_,_}) ->
    true.

next_state(S,
           _V,
           {call,riak_core_handoff_manager,kill_handoffs,[]}) ->
    S#state{handoffs=[],max_concurrency=0};
next_state(S,
           _V,
           {call,riak_core_handoff_manager,get_concurrency,[]}) ->
    S;
next_state(S=#state{handoffs=HS},
           _V,
           {call,riak_core_handoff_manager,set_concurrency,[Limit]}) ->
    %% this is a bit dirty since during test generation we don't
    %% actually have return values, however, since we don't rely
    %% on the state at all during this time we can get away with it
    ValidHandoffs = valid_handoffs(HS),
    case (length(ValidHandoffs) > Limit) of
        true ->
            {Kept, _Discarded} = lists:split(Limit, ValidHandoffs),
            S#state{max_concurrency=Limit,handoffs=Kept};
        false ->
            S#state{max_concurrency=Limit}
    end;
next_state(S=#state{handoffs=HS,inbound_count=IC},
           V,
           {call,riak_core_handoff_manager,add_inbound,_}) ->
    %% we don't know the index of the inbound so we just give it a unique
    %% id to hold its return value (we also hold a single reutrn value instead of a list).
    S#state{handoffs=HS++[{{inbound, IC}, V}],inbound_count=IC+1};
next_state(S=#state{handoffs=HS},
           V,
           {call,riak_core_handoff_manager,add_outbound,[mock_vnode, Idx, _Node, _VNodePid]}) ->
    case lists:keyfind(Idx, 1, HS) of
        false ->
            S#state{handoffs=HS ++ [{Idx, [V]}]};
        {Idx, ReturnVals} ->
            case has_outbound(ReturnVals) of
                true ->
                    S#state{handoffs=lists:keyreplace(Idx, 1, HS, {Idx, [V | ReturnVals]})};
                false ->
                    %% if all we have are max_concurrency return values for this index
                    %% to correctly model how handoff_manager kills handoffs when
                    %% max concurrency is lowered we need to treat this like the first
                    %% call to add_outbound/4
                    S#state{handoffs=lists:keydelete(Idx, 1, HS) ++ [{Idx, [V]}]}
            end
    end;
next_state(S,
           _V,
           {call,application,_,_}) ->
    S.


%% add_outbound/4
postcondition(#state{max_concurrency=MaxConcurrency},
              {call,riak_core_handoff_manager,add_outbound,[mock_vnode, _Index, _Node, _VnodePid]},
              {error, max_concurrency}) ->
    case application:get_env(riak_core, disable_outbound_handoff) of
        {ok, true} ->
            current_concurrency() =< MaxConcurrency;
        _ ->
            current_concurrency() =:= MaxConcurrency
    end;
postcondition(#state{handoffs=Outbounds},
              {call,riak_core_handoff_manager,add_outbound,[mock_vnode, Index, _Node, _VnodePid]},
              {ok, _SenderPid}) ->
    ValidOutbounds = valid_outbounds(Outbounds),
    case lists:keyfind(Index, 1, ValidOutbounds) of
        {Index, _} -> %% got back the same sender or we replaced sender w/ a new one
            ExpectedCount = length(ValidOutbounds);
        false -> %% new sender
            ExpectedCount = length(ValidOutbounds)+1
    end,
    Senders = supervisor:count_children(riak_core_handoff_sender_sup),
    SenderCount = proplists:get_value(active, Senders),
    SenderCount =:= ExpectedCount;
%% add_inbound/1
postcondition(#state{max_concurrency=MaxConcurrency},
              {call,riak_core_handoff_manager,add_inbound,[_]},
              {error, max_concurrency}) ->
    case application:get_env(riak_core, disable_inbound_handoff) of
        {ok, true} ->
            current_concurrency() =< MaxConcurrency;
        _ ->
            current_concurrency() =:= MaxConcurrency
    end;
postcondition(#state{handoffs=HS},
              {call,riak_core_handoff_manager,add_inbound,[_]},
              {ok, _ReceiverPid}) ->
    ExpectedCount = length(valid_inbounds(HS))+1,
    Receivers = supervisor:count_children(riak_core_handoff_receiver_sup),
    ReceiverCount = proplists:get_value(active, Receivers),
    ReceiverCount =:= ExpectedCount;
%% set_concurrency/1
postcondition(_S,
              {call,riak_core_handoff_manager,set_concurrency,[MaxConcurrency]},
              ok) ->
    current_concurrency() =< MaxConcurrency;
%% get_concurrency/0
postcondition(#state{max_concurrency=ExpectedConcurrency},
              {call,riak_core_handoff_manager,get_concurrency,[]},
              MaxConcurrency) ->
    ExpectedConcurrency =:= MaxConcurrency andalso current_concurrency() =< MaxConcurrency;
%% kill_handoffs/0
postcondition(_S,
              {call,riak_core_handoff_manager,kill_handoffs,[]},
              _V) ->
    current_concurrency() =:= 0;
postcondition(_S,{call,application,_,_},_) ->
    true.


command(_S) ->
    HandoffCalls = [
                    %% add_outbound/4
                    ?LET(Index, index(),
                         {call, riak_core_handoff_manager, add_outbound,
                          [mock_vnode,
                           Index, %% pick an index from the ring to do handoff between
                           node(), %% handoff to the local node since we fake actual handoff
                           {call,?MODULE,vnode_pid,[Index]}]}),

                    %% add_inbound/1 -- SSLOpts don't matter since we mock the receiver
                    {call, riak_core_handoff_manager, add_inbound, [[]]}
                   ],
    ConcurrencyCalls = [
                        %% set_concurrency/1
                        {call, riak_core_handoff_manager, set_concurrency, [max_concurrency()]},

                        %% get_concurrency/0
                        {call, riak_core_handoff_manager, get_concurrency, []},

                        %% kill_handoffs/0
                        {call, riak_core_handoff_manager, kill_handoffs, []}
                       ],
    EnableDisableCalls = [
                          %% enable/disable add_outbound/4
                          {call, application, set_env, [riak_core, disable_outbound_handoff, bool()]},

                          %% enable/disable add_inbound/1
                          {call, application, set_env, [riak_core, disable_inbound_handoff, bool()]}
                         ],
    frequency([{15, oneof(HandoffCalls)},
               {4, oneof(ConcurrencyCalls)},
               {1, oneof(EnableDisableCalls)}]).

index() ->
    %% indexes of the 16-partition ring used by riak_core_ring_manager
    %% started in test-mode
    elements([0,91343852333181432387730302044767688728495783936,
              182687704666362864775460604089535377456991567872,
              274031556999544297163190906134303066185487351808,
              365375409332725729550921208179070754913983135744,
              456719261665907161938651510223838443642478919680,
              548063113999088594326381812268606132370974703616,
              639406966332270026714112114313373821099470487552,
              730750818665451459101842416358141509827966271488,
              822094670998632891489572718402909198556462055424,
              913438523331814323877303020447676887284957839360,
              1004782375664995756265033322492444576013453623296,
              1096126227998177188652763624537212264741949407232,
              1187470080331358621040493926581979953470445191168,
              1278813932664540053428224228626747642198940975104,
              1370157784997721485815954530671515330927436759040]).

max_concurrency() ->
    choose(0, 10).

stop_pid(undefined) ->
    ok;
stop_pid(Pid) ->
    unlink(Pid),
    exit(Pid, shutdown),
    ok = wait_for_pid(Pid).

wait_for_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    receive
        {'DOWN',Mref,process,_,_} ->
            ok
    after
        5000 ->
            {error, didnotexit}
    end.

current_concurrency() ->
    Receivers=supervisor:count_children(riak_core_handoff_receiver_sup),
    Senders=supervisor:count_children(riak_core_handoff_sender_sup),
    ActiveReceivers=proplists:get_value(active,Receivers),
    ActiveSenders=proplists:get_value(active,Senders),
    (ActiveReceivers+ActiveSenders).

valid_outbounds(Outbounds) ->
    lists:filter(
      fun({{inbound, _}, _}) ->
              false;
         ({_Idx, ReturnVals}) ->
              has_outbound(ReturnVals)
      end,
      Outbounds).

valid_inbounds(Inbounds) ->
    lists:filter(
      fun({{inbound, _}, {ok, _}}) ->
              true;
         (_) ->
              false
      end,
      Inbounds).

valid_handoffs(HS) ->
    lists:filter(
      fun({{inbound, _}, {ok, _}}) ->
              true;
         ({{inbound, _}, _}) ->
              false;
         ({_Idx, ReturnVals}) ->
              has_outbound(ReturnVals)
      end,
      HS).

%% check if return values from add_outbound/4 contain
%% a {ok, SenderPid}
has_outbound(ReturnVals) ->
    lists:keymember(ok, 1, ReturnVals).

vnode_pid(Idx) ->
    {ok, Pid} = riak_core_vnode_manager:get_vnode_pid(Idx, mock_vnode),
    Pid.

-endif.
