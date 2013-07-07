-module(riak_core_metadata_broadcast).

-behaviour(gen_server).

%% API
-export([start_link/2,
         broadcast/2]).

-export([debug_get_peers/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          %% Initially trees rooted at each node are the same.
          %% Portions of that tree belonging to this node are
          %% shared in this set
          common_eagers :: ordset:ordset(),
          
          %% Unless there are less than 3 nodes, the node in the 
          %% single element, lazy set, belonging to this node is
          %% shared among all trees (since they are intially the same).
          %% If the number of nodes is less than 3, `common_lazy'
          %% is `undefined'
          %% TODO: change to a set and just make it empty, same meaning
          %%       makes it easer to write HOF that operations on both commons
          %%       and leaves room to make the number of random choices bigger
          %%       in bigger clusters
          common_lazy   :: undefined | node(),
          
          %% A mapping of sender node (root of each spanning tree)
          %% to this node's portion of the tree. Elements are
          %% added to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the 
          %% root of a message will never have a key added to
          %% `eager_sets'
          eager_sets    :: [{node(), ordset:ordset()}],

          %% A Mapping of sender node (root of each spanning tree)
          %% to this node's set of lazy peers. Elements are added
          %% to this structure as messages rooted at a node
          %% propogate to this node. Nodes that are never the root
          %% of a message will never have a key added to `lazy_sets'
          lazy_sets     :: [{node(), ordset:ordset()}],

          %% Messages that may need retransmission or scheduled lazy pushes
          %% Messages are added to this set when a node is sent an eager push
          %% and when a node will be sent a lazy push sometime in the future
          %% Messages are removed when eager pushes are acked or lazy pushes
          %% are acknowledged via graft or ignores. Messages are keyed by the
          %% key being updated, the new vector clock, and the round the message 
          %% was received in (0 if the node was the sender)
          outstanding   :: [{{metadata_key(), vclock:vclock(), non_neg_integer()},
                             {[{node(), eager | lazy}], node(), metadata()}}]
         }).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the broadcast server on this node. `InitEagers' are the
%%      initial peers of this node for all broadcast trees. `InitLazy'
%%      is a random peer not in `InitEagers' that will be used as the initial
%%      lazy peer shared by all trees for this node. If the number of nodes
%%      in the cluster is less than 3, `InitLazy' should be `undefined'.
-spec start_link([node()], node()) -> {ok, pid()} | ignore | {error, term}.
start_link(InitEagers, InitLazy) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [InitEagers, InitLazy], []).

%% @doc Broadcast an update that originated on this node
-spec broadcast(metadata_key(), metadata()) -> ok.
broadcast(Key, Metadata) ->
    gen_server:cast(?SERVER, {broadcast, Key, Metadata}).

%%%===================================================================
%%% Debug API
%%%===================================================================
-spec debug_get_peers(node(), node()) -> {ordset:ordset(), ordset:ordset()}.
debug_get_peers(Node, Root) ->
    gen_server:call({?SERVER, Node}, {get_peers, Root}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init([[node()] | node()]) -> {ok, #state{}} |
                                  {ok, #state{}, non_neg_integer() | infinity} |
                                  ignore |
                                  {stop, term()}.
init([InitEagers, InitLazy]) ->
    schedule_tick(),
    {ok, #state{
       common_eagers = ordsets:del_element(node(), ordsets:from_list(InitEagers)),
       common_lazy   = InitLazy,
       eager_sets    = orddict:new(),
       lazy_sets     = orddict:new(),
       outstanding   = orddict:new() 
      }}.

%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({get_peers, Root}, _From, State) ->
    EagerPeers = all_eager_peers(Root, State),
    LazyPeers = all_lazy_peers(Root, State),    
    {reply, {EagerPeers, LazyPeers}, State}.

%% @private
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast({broadcast, Key, Metadata}, State) ->
    State1 = eager_push(Key, Metadata, State),
    State2 = schedule_lazy_push(Key, Metadata, State1),
    {noreply, State2};
handle_cast({broadcast, Key, Metadata, Round, Root, From}, State) ->
    Result = riak_core_metadata_manager:merge(Key, Metadata),
    State1 = handle_broadcast(Key, Metadata, Result, Round, Root, From, State),
    {noreply, State1};
handle_cast({i_have, Key, VClock, Round, Root, From}, State) ->
    Stale = riak_core_metadata_manager:is_stale(Key, VClock),
    State1 = handle_ihave(Stale, Key, VClock, Round, Root, From, State),
    {noreply, State1};
handle_cast({ack, Key, VClock, Round, From}, State) ->
    %% TODO: shoudl we also make sure From is added to the eager
    %% set in case we got a prune from a previous message?
    State1 = ack_outstanding(eager, Key, VClock, Round, From, State),
    {noreply, State1};
handle_cast({prune, From, Root, Key, VClock, Round}, State) ->
    %% prune serves as an ack for the sent eager push that was
    %% a duplicate message
    State1 = ack_outstanding(eager, Key, VClock, Round, From, State),
    State2 = add_lazy(From, Root, State1),
    {noreply, State2};
handle_cast({graft, Key, VClock, Round, Root, From}, State) ->
    State1 = ack_outstanding(lazy, Key, VClock, Round, From, State),

    %% TODO: should we add the eager here or should we always add the eager
    %%       on ack. that way we don't complete the link unless the message
    %%       is sucessful?
    State2 = add_eager(From, Root, State1),

    %% TODO: what if vclock is different?
    %% TODO: refactor
    case riak_core_metadata_manager:get(Key) of
        not_found -> ok;
        Metadata ->
            %% TODO: mark as outstanding? (see TODO above)
            send({broadcast, Key, Metadata, Round, Root, node()}, From)
    end,
    {noreply, State2};
handle_cast({ignored_i_have, Key, VClock, Round, From}, State) ->
    State1 = ack_outstanding(lazy, Key, VClock, Round, From, State),
    {noreply, State1}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_info(tick, State) ->
    schedule_tick(),
    %% TODO: use returned state
    send_outstanding(State),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec terminate(term(), #state{}) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
handle_broadcast(Key, Metadata, stale, Round, Root, From, State) ->
    prune(From, Root, Key, Metadata, Round, State);
handle_broadcast(Key, Metadata, _Update, Round, Root, From, State) ->
    State1 = ack(Key, Metadata, Round, Root, From, State),
    State2 = eager_push(Key, Metadata, Round+1, Root, From, State1),
    schedule_lazy_push(Key, Metadata, Round, Root, From, State2).

handle_ihave(true, Key, VClock, Round, _Root, From, State) ->
    send({ignored_i_have, Key, VClock, Round, node()}, From),
    State;
handle_ihave(false, Key, VClock, Round, Root, From, State) ->
    %% TODO: instead of grafting immediately we should add to a missing
    %%       set and use tick to send grafts. this will reduce message
    %%       overhead because we will only try to graft from a single node
    %%       per {Key, VClock} (or perhaps make the number adjustable)
    %%       to not introduce heavier message load when healing (at the cost
    %%       of some convergence time/last-hop time)    
    %%       Also waiting a period for an in-flight/or pending gossip message
    %%       improves the situation even more
    send({graft, Key, VClock, Round, Root, node()}, From),
    add_eager(From, Root, State).

     
ack(Key, #metadata_v0{vclock=VClock}, Round, Root, From, State) ->   
    send({ack, Key, VClock, Round, node()}, From),
    add_eager(From, Root, State).

prune(From, Root, Key, #metadata_v0{vclock=VClock}, Round, State) ->
    send({prune, node(), Root, Key, VClock, Round}, From),
    add_lazy(From, Root, State).

eager_push(Key, Metadata, State) ->
    eager_push(Key, Metadata, 0, node(), node(), State).

eager_push(Key, Metadata, Round, Root, From, State) ->
    EagerPeers = eager_peers(Root, From, State),
    send({broadcast, Key, Metadata, Round, Root, node()}, EagerPeers),
    add_outstanding(eager, Key, Metadata, Round, Root, EagerPeers, State).

schedule_lazy_push(Key, Metadata, State) ->
    schedule_lazy_push(Key, Metadata, 0, node(), node(), State).

schedule_lazy_push(Key, Metadata, Round, Root, From, State) ->
    LazyPeers = lazy_peers(Root, From, State),
    add_outstanding(lazy, Key, Metadata, Round, Root, LazyPeers, State).

add_outstanding(Type, Key, Metadata=#metadata_v0{vclock=VClock}, Round, Root, Peers,
                State=#state{outstanding=AllOutstanding}) ->
    PeersList = ordsets:from_list(Peers),
    OutstandingKey = outstanding_key(Key, VClock, Round),
    {Existing, _, _} = existing_outstanding(OutstandingKey, AllOutstanding),
    NewOutstanding = lists:zip(PeersList, lists:duplicate(length(PeersList), Type)),
    UpdatedOutstanding = set_outstanding(OutstandingKey,
                                         Root,
                                         Metadata,
                                         ordsets:union(ordsets:from_list(NewOutstanding),
                                                    Existing),
                                         AllOutstanding),
    State#state{outstanding=UpdatedOutstanding}.

ack_outstanding(Type, Key, VClock, Round, From, State=#state{outstanding=All}) ->
    OutstandingKey = outstanding_key(Key, VClock, Round),
    {Existing, Root, Metadata} = existing_outstanding(OutstandingKey, All),
    NewOutstanding = ordsets:del_element({From, Type}, Existing),
    UpdatedOutstanding = set_outstanding(OutstandingKey, Root, Metadata, NewOutstanding, All),
    State#state{outstanding=UpdatedOutstanding}.

send_outstanding(#state{outstanding=All}) ->
    %% TODO: change to a fold, for now we don't modify state, 
    %%       but we may want to add send counts, etc in the future
    %%       and some functions possibly modify the state (although they
    %%       really wont) anyways
    [send_outstanding(OutstandingKey, Outstanding) ||
        {OutstandingKey, Outstanding} <- orddict:to_list(All)],
    %% TODO: return state
    ok.

send_outstanding(OutstandingKey, {OutstandingPeers, Root, Metadata}) ->
    [send_outstanding(OutstandingKey, Root, Metadata, P) || P <- OutstandingPeers].

send_outstanding({Key, VClock, Round}, Root, _Metadata, {Peer, lazy}) ->
    send({i_have, Key, VClock, Round, Root, node()}, Peer);
send_outstanding({_Key, _VClock, _Round}, _Root, _Metadata, {_Peer, eager}) ->
    %% TODO: implement me
    ok.

set_outstanding(Key, Root, Metadata, Outstanding, All) ->
    case ordsets:size(Outstanding) of
        0 -> orddict:erase(Key, All);             
        _ -> orddict:store(Key, {Outstanding, Root, Metadata}, All)
    end.

existing_outstanding(Key, Outstanding) ->
    case orddict:find(Key, Outstanding) of
        error -> {ordsets:new(), undefined, undefined};
        {ok, Existing} -> Existing
    end.

outstanding_key(Key, VClock, Round) ->
    {Key, VClock, Round}.

send(Msg, Peers) when is_list(Peers) ->
    [send(Msg, P) || P <- Peers];
send(Msg, P) ->
    io:format("send ~p -> ~p: ~p~n", [node(), P, Msg]),
    gen_server:cast({?SERVER, P}, Msg).

add_eager(From, Root, State) ->
    %% TODO: should this remove all outstanding lazys for From? or should we check when
    %%       folding if the node is still a lazy? see comment in add_lazy/3
    %%       AT LEAST FOR NOW NO, ITS JUST EXTRA MESSAGES (BUT NEED TO CHECK SAFETY)
    update_peers(From, Root, fun ordsets:add_element/2, fun ordsets:del_element/2, State).

add_lazy(From, Root, State) ->
    %% TODO: should this remove all outstanding eagers? or should we check when folding
    %%       if the node is still an eager? see comment in add_eager/3
    %%       AT LEAST FOR NOW NO, ITS JUST EXTRA MESSAGES (BUT NEED TO CHECK SAFETY)
    update_peers(From, Root, fun ordsets:del_element/2, fun ordsets:add_element/2, State).

update_peers(From, Root, EagerUpdate, LazyUpdate, State) ->
    CurrentEagers = all_eager_peers(Root, State),
    CurrentLazys = all_lazy_peers(Root, State),
    NewEagers = EagerUpdate(From, CurrentEagers),
    NewLazys  = LazyUpdate(From, CurrentLazys),
    set_peers(Root, NewEagers, NewLazys, State).

set_peers(Root, Eagers, Lazys, State=#state{eager_sets=EagerSets,lazy_sets=LazySets}) ->
    NewEagers = orddict:store(Root, Eagers, EagerSets),
    NewLazys = orddict:store(Root, Lazys, LazySets),
    State#state{eager_sets=NewEagers, lazy_sets=NewLazys}.

eager_peers(Root, From, State) ->
    AllPeers = all_eager_peers(Root, State),
    ordsets:del_element(From, AllPeers).

lazy_peers(Root, From, State) ->
    AllPeers = all_lazy_peers(Root, State),
    ordsets:del_element(From, AllPeers).

all_eager_peers(Root, #state{eager_sets = EagerSets, common_eagers = Common}) ->
    all_peers(Root, EagerSets, Common).

all_lazy_peers(Root, #state{lazy_sets = LazySets, common_lazy = undefined}) ->
    all_peers(Root, LazySets, ordsets:new());
all_lazy_peers(Root, #state{lazy_sets = LazySets, common_lazy = CommonLazy}) ->
    all_peers(Root, LazySets, ordsets:add_element(CommonLazy, ordsets:new())).

all_peers(Root, Sets, Default) ->
    case orddict:find(Root, Sets) of
        error -> Default;             
        {ok, Peers} -> Peers
    end.

schedule_tick() ->
    %% TODO: make configurable
    TickMs = 10000,
    erlang:send_after(TickMs, ?MODULE, tick).



