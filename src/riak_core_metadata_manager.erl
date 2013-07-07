-module(riak_core_metadata_manager).

-behaviour(gen_server).

%% API
-export([start_link/0,
         put/2,
         merge/2,
         get/1,
         is_stale/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_core_metadata.hrl").

-define(SERVER, ?MODULE). 
-define(DETS_TABLE, riak_core_metadata).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Starts the server
-spec start_link() -> {ok, pid()} | ignore | {error, term}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% TODO: options timeout/default timeout
-spec put(metadata_key(), metadata_value()) -> metadata().
put(Key, Value) ->
    gen_server:call(?SERVER, {put, Key, Value}).

%% TODO: options timeout/default timeout
%% TODO: can probably merge this w/ above function
-spec merge(metadata_key(), metadata()) -> stale | metadata().
merge(Key, Metadata) ->
    gen_server:call(?SERVER, {put, Key, Metadata}).

-spec get(term()) -> metadata() | not_found.
get(Key) ->
    gen_server:call(?SERVER, {get, Key}).

-spec is_stale(term(), vclock:vclock()) -> boolean().
is_stale(Key, VClock) ->
    gen_server:call(?SERVER, {is_stale, Key, VClock}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec init([]) -> {ok, #state{}} |
                  {ok, #state{}, non_neg_integer() | infinity} |
                  ignore |
                  {stop, term()}.
init([]) ->
    Filename = lists:flatten("metadata-", [node()]),
    {ok, ?DETS_TABLE} = dets:open_file(?DETS_TABLE, [{file, Filename}]),                                 {ok, #state{}}.

%% @private
%% @doc Handling call messages
-spec handle_call(term(), {pid(), term()}, #state{}) ->
                         {reply, term(), #state{}} |
                         {reply, term(), #state{}, non_neg_integer()} |
                         {noreply, #state{}} |
                         {noreply, #state{}, non_neg_integer()} |
                         {stop, term(), term(), #state{}} |
                         {stop, term(), #state{}}.
handle_call({put, Key, Metadata=#metadata_v0{}}, _From, State) ->
    {Result, NewState} = read_modify_merge(Key, Metadata, State),
    {reply, Result, NewState};
handle_call({put, Key, Value}, _From, State) ->
    {Updated, NewState} = read_modify_overwrite(Key, Value, State),
    {reply, Updated, NewState};
handle_call({get, Key}, _From, State) ->
    Result = read(Key, State),
    {reply, Result, State};
handle_call({is_stale, Key, VClock}, _From, State) ->
    Result = read(Key, State),
    IsStale = vclock_stale(VClock, Result),
    {reply, IsStale, State}.

%% @private
%% @doc Handling cast messages
-spec handle_cast(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec handle_info(term(), #state{}) -> {noreply, #state{}} |
                                       {noreply, #state{}, non_neg_integer()} |
                                       {stop, term(), #state{}}.
handle_info(_Info, State) ->
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
read_modify_merge(Key, Value, State) ->
    read_modify_write(Key, Value, fun merge/4, State).

read_modify_overwrite(Key, Value, State) ->
    read_modify_write(Key, Value, fun overwrite/4, State).

read_modify_write(Key, Value, WriteFun, State) ->
    case read(Key, State) of
        not_found -> insert(Key, Value, State);             
        Existing -> WriteFun(Key, Value, Existing, State)
    end.

insert(Key, Metadata=#metadata_v0{}, State) ->
    store(Key, Metadata, State);
insert(Key, Value, State) ->
    ToStore = #metadata_v0 {
      values  = [Value],
      vclock = vclock:increment(node(), vclock:fresh())
     },
    store(Key, ToStore, State).

overwrite(Key, Value, #metadata_v0{vclock=VClock}, State) ->
    ToStore = #metadata_v0 {
      values = [Value],
      vclock = vclock:increment(node(), VClock)
     },
    store(Key, ToStore, State).

merge(Key,
      New=#metadata_v0{vclock=NewVClock},
      Existing=#metadata_v0{vclock=ExistingVClock},
      State) ->
    %% If vclock we have is a descendant of the one just sent
    %% then the message is a duplicate
    IsUpdate = not vclock:descends(ExistingVClock, NewVClock),
    merge(IsUpdate, Key, New, Existing, State).

merge(false, _Key, _New, _Existing, State) ->
    {stale, State};
merge(true, Key, New, Existing, State) ->
    ToStore = merge_meta(New, Existing),
    store(Key, ToStore, State).

merge_meta(New=#metadata_v0{vclock=NewVClock},
           Existing=#metadata_v0{vclock=ExistingVClock}) ->    
    merge_meta(vclock:descends(NewVClock, ExistingVClock), New, Existing).

%% Does update dominate (since we know existing vclock doesn't
%% descend it) If so, value is overwritten, otherwise
%% conflicts are generated
merge_meta(true, New, _Existing) ->
    New;
merge_meta(false,
           #metadata_v0{values=New,vclock=NewVClock},
           #metadata_v0{values=Existing,vclock=ExistingVClock}) ->
    MergedVClock = vclock:merge([NewVClock, ExistingVClock]),
    Updated = New ++ Existing,
    #metadata_v0{values = Updated,
                 vclock = MergedVClock}.

vclock_stale(_VClock, not_found) ->
    false;
vclock_stale(VClock, #metadata_v0{vclock=Existing}) ->
    vclock:descends(Existing, VClock).
    
store(Key, Metadata, State) ->
    dets:insert(?DETS_TABLE, [{Key, Metadata}]),
    {Metadata, State}.
        
read(Key, _State) ->    
    case dets:lookup(?DETS_TABLE, Key) of
        [] -> not_found;
        [{Key, Metadata}] -> Metadata
    end.



