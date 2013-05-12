-module(riak_core_vnode_handoff_helper).
-compile(export_all).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {finished,
                timer}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

mark_handoff_complete(Idx, Prev, New, Mod) ->
    gen_server:call(?MODULE, {mark_handoff_complete, Idx, Prev, New, Mod}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{finished=[],
                timer=undefined}}.

handle_call({mark_handoff_complete, Idx, Prev, New, Mod}, From, State) ->
    Finished = [{From, Idx, Prev, New, Mod} | State#state.finished],
    State2 = State#state{finished=Finished},
    State3 = maybe_set_timer(100, State2),
    {noreply, State3};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    Ring = mark_finished(State),
    inform_vnodes(Ring, State),
    State2 = maybe_set_timer(100, State),
    State3 = State2#state{finished=[]},
    {noreply, State3};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

mark_finished(#state{finished=Finished}) ->
    Result = riak_core_ring_manager:ring_trans(
      fun(Ring, _) ->
              Owners = dict:from_list(riak_core_ring:all_owners(Ring)),
              Complete = [{Idx, Mod} || {_From, Idx, Prev, New, Mod} <- Finished,
                                        should_mark_handoff_complete(Idx, Prev, New, Mod, Ring, Owners)],
              case Complete of
                  [] ->
                      ignore;
                  _ ->
                      Ring2 = riak_core_ring:handoff_complete(Ring, Complete),
                      %% Optimization. Only alter the local ring without
                      %% triggering a gossip, thus implicitly coalescing
                      %% multiple vnode handoff completion events. In the
                      %% future we should decouple vnode handoff state from
                      %% the ring structure in order to make gossip independent
                      %% of ring size.
                      {set_only, Ring2}
              end
      end, []),

    case Result of
        {ok, NewRing} ->
            NewRing = NewRing;
        _ ->
            {ok, NewRing} = riak_core_ring_manager:get_my_ring()
    end,
    NewRing.

mark_handoff_complete(Idx, Prev, New, Mod, Ring, Owners) ->
    %% Owner = chashbin:index_owner(Idx, CHBin),
    Owner = dict:fetch(Idx, Owners),
    {_, NextOwner, Status} = riak_core_ring:next_owner(Ring, Idx, Mod),
    NewStatus = riak_core_ring:member_status(Ring, New),

    case {Owner, NextOwner, NewStatus, Status} of
        {Prev, New, _, awaiting} ->
            Ring2 = riak_core_ring:handoff_complete(Ring, Idx, Mod),
            {true, Ring2};
        _ ->
            {false, Ring}
    end.

should_mark_handoff_complete(Idx, Prev, New, Mod, Ring, Owners) ->
    %% Owner = chashbin:index_owner(Idx, CHBin),
    Owner = dict:fetch(Idx, Owners),
    {_, NextOwner, Status} = riak_core_ring:next_owner(Ring, Idx, Mod),
    NewStatus = riak_core_ring:member_status(Ring, New),

    case {Owner, NextOwner, NewStatus, Status} of
        {Prev, New, _, awaiting} ->
            true;
        _ ->
            false
    end.

inform_vnodes(Ring, #state{finished=Finished}) ->
    %% CHBin = chashbin:create(riak_core_ring:chash(Ring)),
    Owners = dict:from_list(riak_core_ring:all_owners(Ring)),
    [begin
         %% Owner = chashbin:index_owner(Idx, CHBin),
         Owner = dict:fetch(Idx, Owners),
         {_, NextOwner, Status} = riak_core_ring:next_owner(Ring, Idx, Mod),
         NewStatus = riak_core_ring:member_status(Ring, New),

         Reply = case {Owner, NextOwner, NewStatus, Status} of
                     {_, _, invalid, _} ->
                         %% Handing off to invalid node, don't give-up data.
                         continue;
                     {Prev, New, _, _} ->
                         forward;
                     {Prev, _, _, _} ->
                         %% Handoff wasn't to node that is scheduled in next, so no change.
                         continue;
                     {_, _, _, _} ->
                         shutdown
                 end,
         gen_server:reply(From, Reply)
     end || {From, Idx, Prev, New, Mod} <- Finished].

maybe_set_timer(Duration, State=#state{timer=undefined}) ->
    set_timer(Duration, State);
maybe_set_timer(Duration, State=#state{timer=Timer}) ->
    case erlang:read_timer(Timer) of
        false ->
            set_timer(Duration, State);
        _ ->
            State
    end.

set_timer(Duration, State) ->
    Timer = erlang:send_after(Duration, self(), timeout),
    State#state{timer=Timer}.
