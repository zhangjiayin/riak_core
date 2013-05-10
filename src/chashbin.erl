-module(chashbin).
-compile(export_all).

%% 20 bytes for SHA, 2 bytes for node id
-define(UNIT, 176).
-define(ENTRY, binary-unit:?UNIT).

create({Size, Owners}) ->
    Nodes1 = [Node || {_, Node} <- Owners],
    Nodes2 = lists:usort(Nodes1),
    Nodes3 = lists:zip(Nodes2, lists:seq(1, length(Nodes2))),
    Bin = create_bin(Owners, Nodes3, <<>>),
    {Size, Bin, list_to_tuple(Nodes2)}.

create_bin([], _, Bin) ->
    Bin;
create_bin([{Idx, Owner}|Owners], Nodes, Bin) ->
    {Owner, Id} = lists:keyfind(Owner, 1, Nodes),
    Bin2 = <<Bin/binary, Idx:160/integer, Id:16/integer>>,
    create_bin(Owners, Nodes, Bin2).

to_list({Size, Bin, Nodes}) ->
    L = [{Idx, element(Id, Nodes)}
         || <<Idx:160/integer, Id:16/integer>> <= Bin],
    {Size, L}.

to_list_filter(Pred, {_, Bin, Nodes}) ->
    [{Idx, element(Id,Nodes)}
     || <<Idx:160/integer, Id:16/integer>> <= Bin,
        Pred({Idx, element(Id,Nodes)})].

responsible_index(<<HashKey:160/integer>>, CHBin) ->
    responsible_index(HashKey, CHBin);
responsible_index(HashKey, {Size, _, _}) ->
    Inc = chash:ring_increment(Size),
    (((HashKey div Inc) + 1) rem Size) * Inc.

responsible_position(<<HashKey:160/integer>>, CHBin) ->
    responsible_position(HashKey, CHBin);
responsible_position(HashKey, {Size, _, _}) ->
    Inc = chash:ring_increment(Size),
    ((HashKey div Inc) + 1) rem Size.

index_position(<<Idx:160/integer>>, CHBin) ->
    index_position(Idx, CHBin);
index_position(Idx, {Size, _, _}) ->
    Inc = chash:ring_increment(Size),
    (Idx div Inc) rem Size.

index_owner(Idx, CHBin) ->
    case itr_value(exact_iterator(Idx, CHBin)) of
        {Idx, Owner} ->
            Owner;
        Other ->
            %% Match the behavior for riak_core_ring:index_owner/2
            throw({badmatch, Idx, Other})
    end.

num_partitions({Size, _, _}) ->
    Size.

%%%%
%% iterator
%%%

-type chashbin() :: {pos_integer(), binary(), tuple(node())}.
-type iterator() :: {pos_integer(), chashbin()}.

-spec iterator(integer(), chashbin()) -> iterator().
iterator(first, CHBin) ->
    {0, 0, CHBin};
iterator(<<Idx:160/integer>>, CHBin) ->
    iterator(Idx, CHBin);
iterator(Idx, CHBin) ->
    Pos = responsible_position(Idx, CHBin),
    {Pos, Pos, CHBin}.

exact_iterator(<<Idx:160/integer>>, CHBin) ->
    exact_iterator(Idx, CHBin);
exact_iterator(Idx, CHBin) ->
    Pos = index_position(Idx, CHBin),
    {Pos, Pos, CHBin}.

itr_value({Pos, _, {_, Bin, Nodes}}) ->
    <<_:Pos/binary-unit:?UNIT, Idx:160/integer, Id:16/integer, _/binary>> = Bin,
    Owner = element(Id, Nodes),
    {Idx, Owner}.

itr_next({Pos, Start, CHBin={Size, _, _}}) ->
    Pos2 = (Pos + 1) rem Size,
    case Pos2 of
        Start ->
            done;
        _ ->
            {Pos2, Start, CHBin}
    end.

itr_pop(N, {Pos, Start, CHBin={Size, Bin, Nodes}}) ->
    L =
        case Bin of
            <<_:Pos/?ENTRY, Bin2:N/?ENTRY, _/binary>> ->
                [{Idx, element(Id, Nodes)}
                 || <<Idx:160/integer, Id:16/integer>> <= Bin2];
            _ ->
                Left = (N + Pos) - Size,
                Skip = Pos - Left,
                <<Bin3:Left/?ENTRY, _:Skip/?ENTRY, Bin2/binary>> = Bin,
                L1 = [{Idx, element(Id, Nodes)}
                      || <<Idx:160/integer, Id:16/integer>> <= Bin2],
                L2 = [{Idx, element(Id, Nodes)}
                      || <<Idx:160/integer, Id:16/integer>> <= Bin3],
                L1 ++ L2
        end,
    Pos2 = (Pos + N) rem Size,
    Itr2 = {Pos2, Start, CHBin},
    {L, Itr2}.

itr_next_while(Pred, Itr) ->
    case Pred(itr_value(Itr)) of
        false ->
            Itr;
        true ->
            itr_next_while(Pred, itr_next(Itr))
    end.

key(Pos, Bin) ->
    <<_:Pos/binary-unit:?UNIT, Key:160/integer, _/binary>> = Bin,
    Key.

r1(N, W) ->
    Idx = <<0:160/integer>>,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    parallel_time(N, W, fun() ->
                                riak_core_apl:get_apl(Idx, 3, Ring, UpNodes)
                        end).

r2(N, W) ->
    Idx = <<0:160/integer>>,
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    parallel_time(N, W, fun() ->
                                %% riak_core_apl:get_apl(Idx, 3, riak_kv)
                                riak_core_apl:get_apl_chbin(Idx, 3, CHBin, UpNodes)
                        end).

parallel_time(N, W, F) ->
    Work = fun() ->
                   loop(N, F)
           end,
    timer:tc(fun spawn_wait/2, [W, Work]).
   
tt() ->
    tt(100000).

tt(N) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owners = riak_core_ring:all_owners(Ring),
    {First, _} = hd(Owners),
    {Last, _} = hd(lists:reverse(Owners)),
    {X1, Y1} = go(N, First),
    {X2, Y2} = go(N, Last),
    {X1, Y1, X2, Y2}.

go(N) ->
    DIdx = 68507889249886074290797726533575766546371837952,
    go(N, DIdx).

go(N, DIdx) ->
    Idx = <<DIdx:160/integer>>,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    UpNodes = riak_core_node_watcher:nodes(riak_kv),
    T1 = time(N, fun() ->
                         riak_core_apl:get_apl(Idx, 3, Ring, UpNodes)
                 end),
    T2 = time(N, fun() ->
                         %% riak_core_apl:get_apl(Idx, 3, riak_kv)
                         riak_core_apl:get_apl_chbin(Idx, 3, CHBin, UpNodes)
                 end),
    {T1, T2}.

apl2(N, DIdx) ->
    Idx = <<DIdx:160/integer>>,
    loop(N, fun() ->
                    riak_core_apl2:get_apl(Idx, 3, riak_kv)
            end).

time(N, F) ->
    timer:tc(fun loop/2, [N, F]).

loop(0, _) ->
    ok;
loop(N, F) ->
    F(),
    loop(N-1, F).

spawn_wait(N, F) ->
    spawn_n(N, F),
    wait(N).

spawn_n(0, _) ->
    ok;
spawn_n(N, F) ->
    Self = self(),
    spawn_link(fun() ->
                       F(),
                       Self ! done
               end),
    spawn_n(N-1, F).

wait(0) ->
    ok;
wait(N) ->
    receive
        done ->
            wait(N-1)
    end.

do_test(N) ->
    [begin
         {{Min1, Max1}} = do_test(Size, NumNodes, N),
         io:format("~5s  ~4s  ~5s  ~5s~n", [integer_to_list(Size),
                                            integer_to_list(NumNodes),
                                            integer_to_list(Min1),
                                            integer_to_list(Max1)])
         %% {{Min1, Max1}, {Min2, Max2}} = do_test(Size, NumNodes, N),
         %% io:format("~5s  ~4s  ~5s  ~5s  ~5s  ~5s~n", [integer_to_list(Size),
         %%                                              integer_to_list(NumNodes),
         %%                                              integer_to_list(Min1),
         %%                                              integer_to_list(Max1),
         %%                                              integer_to_list(Min2),
         %%                                              integer_to_list(Max2)])
     end || Size <- [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384],
            NumNodes <- [1, 10, 100, 1000]],
    ok.

do_test(Size, NumNodes, N) ->
    {Ring, Nodes} = fake_ring(Size, NumNodes),
    Owners = riak_core_ring:all_owners(Ring),
    CHBin = chashbin:create(riak_core_ring:chash(Ring)),
    Idx1 = <<0:160/integer>>,
    [_,_,_,{IdxInt, _}|_] = lists:reverse(Owners),
    Idx2 = <<IdxInt:160/integer>>,
    UpNodes = Nodes,
    %% UpNodes = lists:usort(Nodes),
    %% T1 = none,
    {T1,_} = time(N, fun() ->
                             riak_core_apl:get_apl(Idx1, 3, Ring, UpNodes)
                     end),
    %% {T2,_} = time(N, fun() ->
    %%                          riak_core_apl:get_apl_chbin(Idx1, 3, CHBin, UpNodes)
    %%                  end),
    {T3,_} = time(N, fun() ->
                             riak_core_apl:get_apl(Idx2, 3, Ring, UpNodes)
                     end),
    %% {T4,_} = time(N, fun() ->
    %%                          riak_core_apl:get_apl_chbin(Idx2, 3, CHBin, UpNodes)
    %%                  end),
    %% T2 = eprof:profile(fun() ->
    %%                            time(N, fun() ->
    %%                                            riak_core_apl:get_apl_chbin(Idx, 3, CHBin, UpNodes)
    %%                                    end)
    %%                    end),
    %% {T1, T2}.
    {{round(T1 div N), round(T3 div N)}}.
    %% {{round(T1 div N), round(T3 div N)},
    %%  {round(T2 div N), round(T4 div N)}}.

fake_ring(Size, NumNodes) ->
    Inc = chash:ring_increment(Size),
    Nodes = [list_to_atom("dev" ++ integer_to_list(X) ++ "@127.0.0.1") || _ <- lists:seq(0,Size div NumNodes),
                                                                          X <- lists:seq(1,NumNodes)],
    Nodes2 = lists:sublist(Nodes, Size),
    Indices = lists:seq(0, (Size-1)*Inc, Inc),
    Owners = lists:zip(Indices, Nodes2),
    Ring = riak_core_ring:fresh(Size, hd(Nodes2)),
    Ring2 = lists:foldl(fun({Idx, Owner}, RingAcc) ->
                                riak_core_ring:transfer_node(Idx, Owner, RingAcc)
                        end, Ring, Owners),
    {Ring2, Nodes2}.

htest(N, NumNodes) ->
    Nodes = [list_to_atom("dev" ++ integer_to_list(X) ++ "@127.0.0.1") || X <- lists:seq(1,NumNodes)],
    time(N, fun() ->
                    [erlang:phash(Node, (1 bsl 27) - 1) || Node <- Nodes],
                    %% [erlang:phash2(Node) || Node <- Nodes],
                    ok
            end).
