-type sender_type() :: fsm | server | raw.
-type sender() :: {sender_type(), reference(), pid()} |
                  %% TODO: Double-check that these special cases are kosher
                  {server, undefined, undefined} | % special case in
                                                   % riak_core_vnode_master.erl
                  {fsm, undefined, pid()} |        % special case in
                                                   % riak_kv_util:make_request/2.erl
                  ignore.


-record(riak_vnode_req_v1, {
          index :: chash:partition(),
          sender=ignore :: sender(),
          request}).

-type vnode_req(T) :: #riak_vnode_req_v1{request::T}.

-type vnode_fold_fun() :: fun((term(), term(), term()) -> term()).

-record(riak_core_fold_req_v1, {
          foldfun :: vnode_fold_fun(),
          acc0 :: term()}).



-define(VNODE_REQ, #riak_vnode_req_v1).
-define(FOLD_REQ, #riak_core_fold_req_v1).

