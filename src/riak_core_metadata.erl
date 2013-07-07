-module(riak_core_metadata).

-export([put/2, get/1, fetch_broadcast_tree/2]).

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

-spec fetch_broadcast_tree(node(), [node()]) -> [{node(), {[node()], [node()]}}].
fetch_broadcast_tree(Root, Nodes) ->
    [{Node, riak_core_metadata_broadcast:debug_get_peers(Node, Root)} || Node <- Nodes].
