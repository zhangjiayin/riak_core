%%% @doc Ring visualization resource.
%%% @author Uwe Dauernheim <uwe@dauernheim.net>
-module(riak_core_wm_ring_resource).

-author("Uwe Dauernheim <uwe@dauernheim.net>").

-export([ init/1
	, to_html/2
	]).

-include_lib("webmachine/include/webmachine.hrl").

-record(chstate, {
    nodename, % the Node responsible for this chstate
    vclock,   % for this chstate object, entries are {Node, Ctr}
    chring,   % chash ring of {IndexAsInt, Node} mappings
    meta      % dict of cluster-wide other data (primarily bucket N-value, etc)
}). 

init([]) ->
    io:format("init~n"),
  {ok, undefined}.

to_html(ReqData, Context) ->
    io:format("to html~n"),
    {_Down, Rings} = get_rings(),
    Bleh = lists:map(fun({N, R}) ->
                %{_Pri, Sec, Stopped} = partitions(N, R)
                {_, Sec, _} = partitions(N, R),
                {N, Sec}
        end, Rings),
    io:format("Bleh ~p~n", [Bleh]),
    Result = template(get_ring(), get_down(), Bleh),
  {Result, ReqData, Context}.

get_ring() ->
  {ok, State} = riak_core_ring_manager:get_my_ring(),
  {_, Ring} = State#chstate.chring,
  Ring.

get_down() ->
    try riak_kv_status:ringready() of
        {error, {nodes_down, Down}} ->
            Down;
        _ -> []
    catch
        _:_ ->
            []
    end.

template(Ring, Down, Bleh) ->
    X = json_pp:print(mochijson2:encode({struct, Bleh})),
    io:format("X ~p~n", [X]),
    Data = lists:map(fun({ID, Node}) -> {struct, [{id, ID}, {node, Node}]} end, Ring),
    DownData = {struct, [{down, Down}]},
    {ok, Bin} = file:read_file("/home/andrew/riak/deps/riak_core/priv/www/ring.html"),
    io_lib:format(binary_to_list(Bin),
        [json_pp:print(mochijson2:encode(Data)),
            json_pp:print(mochijson2:encode(DownData)), X]).


%% Retrieve the rings for all other nodes by RPC
get_rings() ->
    {RawRings, Down} = riak_core_util:rpc_every_member(
                         riak_core_ring_manager, get_my_ring, [], 30000),
    Rings = orddict:from_list([{riak_core_ring:owner_node(R), R} || {ok, R} <- RawRings]),
    {lists:sort(Down), Rings}.

%% Return a list of active primary partitions, active secondary partitions (to be handed off)
%% and stopped partitions that should be started
partitions(Node, Ring) ->
    Owners = riak_core_ring:all_owners(Ring),
    Owned = ordsets:from_list(owned_partitions(Owners, Node)),
    Active = ordsets:from_list(active_partitions(Node)),
    Stopped = ordsets:subtract(Owned, Active),
    Secondary = ordsets:subtract(Active, Owned),
    Primary = ordsets:subtract(Active, Secondary),
    {Primary, Secondary, Stopped}.

%% Return the list of partitions owned by a node
owned_partitions(Owners, Node) ->
    [P || {P, Owner} <- Owners, Owner =:= Node].

%% Get a list of active partition numbers - regardless of vnode type
active_partitions(Node) ->
    lists:foldl(fun({_,P}, Ps) ->
                        ordsets:add_element(P, Ps)
                end, [], running_vnodes(Node)).

%% Get a list of running vnodes for a node
running_vnodes(Node) ->
    Pids = vnode_pids(Node),
    [rpc:call(Node, riak_core_vnode, get_mod_index, [Pid], 30000) || Pid <- Pids].

%% Get a list of vnode pids for a node
vnode_pids(Node) ->
    [Pid || {_,Pid,_,_} <- supervisor:which_children({riak_core_vnode_sup, Node})].


