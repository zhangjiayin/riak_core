-module(riak_core_metadata).

-export([put/2, get/1, get/2, fetch_broadcast_tree/2]).

-include("riak_core_metadata.hrl").

-spec put(metadata_key(), metadata_value()) -> ok.
put(Key, Value) ->
    Updated = riak_core_metadata_manager:put(Key, Value),
    ok = riak_core_metadata_broadcast:broadcast(Key, Updated).

-spec get(metadata_key()) -> not_found | [metadata_value()].
get(Key) ->
    case riak_core_metadata_manager:get(Key) of
        #metadata_v0{values=Values} -> Values;
        NotFound -> NotFound
    end.

-spec get(metadata_key(), metadata_value()) -> [metadata_value()].
get(Key, Default) ->
    case ?MODULE:get(Key) of
        Result when is_list(Result) -> Result;
        _ -> [Default]
    end.

-spec fetch_broadcast_tree(node(), [node()]) -> [{node(), {[node()], [node()]}}].
fetch_broadcast_tree(Root, Nodes) ->
    [begin
         Peers = try riak_core_metadata_broadcast:debug_get_peers(Node, Root)
                 catch _:_ -> down
                 end,
         {Node, Peers} 
     end || Node <- Nodes].
