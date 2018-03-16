%%======================================================================
%%
%% Leo Konsul
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% @doc The konsul's worker process
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_konsul_ring.erl
%% @end
%%======================================================================
-module(leo_konsul_ring).

-behaviour(gen_server).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2, stop/1]).
-export([create/2,
         lookup/3, lookup/4,
         first/2, last/2, force_sync/1,
         diff_redundancies/1,
         checksum/1,
         dump/1, subscribe/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).

-ifdef(TEST).
-define(CURRENT_TIME, 65432100000).
-define(DEF_SYNC_MIN_INTERVAL, 100).
-define(DEF_SYNC_MAX_INTERVAL, 500).
-define(DEF_TIMEOUT, timer:seconds(3)).
-define(DEF_TIMEOUT_LONG, timer:seconds(3)).
-else.
-define(CURRENT_TIME, leo_date:now()).
-define(DEF_SYNC_MIN_INTERVAL, 250).
-define(DEF_SYNC_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT, timer:seconds(30)).
-define(DEF_TIMEOUT_LONG, timer:seconds(120)).
-endif.

-define(DEF_NUM_OF_DIV, 32).

-record(redundancy_params, {
          cluster_id = null :: cluster_id(),
          n = 0 :: non_neg_integer(),
          r = 0 :: non_neg_integer(),
          w = 0 :: non_neg_integer(),
          d = 0 :: non_neg_integer(),
          num_of_rack_awarenesses = 0 :: non_neg_integer(),
          first_vnode_id = 0 :: vnode_id(),
          last_vnode_id = 0 :: vnode_id(),
          routing_table = [] :: leo_gb_trees:tree()
         }).

-record(state, {
          id :: atom(),
          cur  = #ring_info{} :: #ring_info{},
          prev = #ring_info{} :: #ring_info{},
          redundancy_params = #redundancy_params{} :: #redundancy_params{},
          min_interval = ?DEF_SYNC_MIN_INTERVAL :: non_neg_integer(),
          max_interval = ?DEF_SYNC_MAX_INTERVAL :: non_neg_integer(),
          timestamp = 0 :: non_neg_integer(),
          member_list_checksum = {-1, -1} :: {integer(), integer()},
          ring_checksum = {-1,-1} :: {integer(), integer()}
         }).

-compile({inline, [lookup_fun/2,
                   lookup_fun_1/3
                  ]}).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
start_link(ClusterId, SystemConf) ->
    Id = ?id_ring(ClusterId),
    gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId, SystemConf], []).

%% start_link(ClusterId) ->
%%     Id = ?id_ring(ClusterId),
%%     gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId], []).

%% @doc Stop the server
stop(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Create the Rings.
-spec(create(ClusterId, Ver) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::?VER_CUR|?VER_PREV).
create(ClusterId, Ver) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, {create, Ver}, ?DEF_TIMEOUT_LONG).


%% @doc Look up the redundant-nodes by address-id
-spec(lookup(ClusterId, Tbl, AddrId) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Tbl::atom(),
                            AddrId::non_neg_integer()).
lookup(ClusterId, Tbl, AddrId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, {lookup, Tbl, AddrId}, ?DEF_TIMEOUT).


-spec(lookup(ClusterId, Tbl, AddrId, TotalNumOfRedundancies) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Tbl::atom(),
                            AddrId::non_neg_integer(),
                            TotalNumOfRedundancies::pos_integer()).
lookup(ClusterId, Tbl, AddrId, TotalNumOfRedundancies) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, {lookup, Tbl, AddrId, TotalNumOfRedundancies}, ?DEF_TIMEOUT).


%% @doc Retrieve the first record of the redundant-nodes
-spec(first(ClusterId, Tbl) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Tbl::atom()).
first(ClusterId, Tbl) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, {first, Tbl}, ?DEF_TIMEOUT).


%% @doc Retrieve the last record of the redundant-nodes
-spec(last(ClusterId, Tbl) ->
             {ok, #redundancies{}} |
             not_found when ClusterId::cluster_id(),
                            Tbl::atom()).
last(ClusterId, Tbl) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, {last, Tbl}, ?DEF_TIMEOUT).


%% @doc Force RING to synchronize with the manager-node
-spec(force_sync(ClusterId) ->
             ok |
             {error, any()} when ClusterId::cluster_id()).
force_sync(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, force_sync, ?DEF_TIMEOUT_LONG).


-spec(diff_redundancies(ClusterId) ->
             {ok, DiffRedundancies} | {error, any()} when ClusterId::cluster_id(),
                                                          DiffRedundancies::[#diff_redundancies{}]).
diff_redundancies(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, diff_redundancies, ?DEF_TIMEOUT).


%% @doc Retrieve the checksums
-spec(checksum(ClusterId) ->
             {ok, {PrevChecksum, CurChecksum}} when ClusterId::cluster_id(),
                                                    PrevChecksum::integer(),
                                                    CurChecksum::integer()).
checksum(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, checksum, ?DEF_TIMEOUT).


%% @doc Dump the current ring-info
-spec(dump(ClusterId) ->
             ok | {error, any()} when ClusterId::cluster_id()).
dump(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, dump, ?DEF_TIMEOUT).


%% @doc Sucscrive changing records of the member-tables
-spec(subscribe(ClusterId) ->
             ok | {error, any()} when ClusterId::cluster_id()).
subscribe(ClusterId) ->
    Id = ?id_ring(ClusterId),
    gen_server:call(Id, subscribe, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Id, ClusterId, SystemConf]) ->
    sync(),
    {ok, #state{id = Id,
                redundancy_params =
                    #redundancy_params{
                       cluster_id = ClusterId,
                       n = SystemConf#?SYSTEM_CONF.n,
                       r = SystemConf#?SYSTEM_CONF.r,
                       w = SystemConf#?SYSTEM_CONF.w,
                       d = SystemConf#?SYSTEM_CONF.d,
                       num_of_rack_awarenesses = SystemConf#?SYSTEM_CONF.num_of_rack_replicas},
                timestamp = ?timestamp()}}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({create, Ver},_From,
            #state{redundancy_params = RedundancyParams} = State) when Ver == ?VER_CUR;
                                                                       Ver == ?VER_PREV ->
    #redundancy_params{
       cluster_id = ClusterId} = RedundancyParams,
    {Reply, State_1} = create_fun(ClusterId, Ver, State),
    {reply, Reply, State_1};

handle_call({create,_Ver}, _From, State) ->
    {reply, {error, invalid_version}, State};

handle_call({lookup, Tbl,_AddrId},_From, State) when Tbl /= ?RING_TBL_CUR,
                                                     Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};

handle_call({lookup, Tbl, AddrId},_From, State) ->
    RedParams = ring_info(Tbl, State),
    Reply = lookup_fun(AddrId, RedParams),
    {reply, Reply, State};

handle_call({lookup, Tbl, AddrId, TotalNumOfRedundancies},_From, State) ->
    RedParams = ring_info(Tbl, State),
    Reply = lookup_fun(AddrId, RedParams, TotalNumOfRedundancies),
    {reply, Reply, State};

handle_call({first, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                            Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({first, Tbl},_From, State) ->
    RedParams = ring_info(Tbl, State),
    FirstVNodeId = RedParams#redundancy_params.first_vnode_id,
    Reply = lookup_fun(FirstVNodeId, RedParams),
    {reply, Reply, State};


handle_call({last, Tbl},_From, State) when Tbl /= ?RING_TBL_CUR,
                                           Tbl /= ?RING_TBL_PREV ->
    {reply, {error, invalid_table}, State};
handle_call({last, Tbl},_From, State) ->
    RedParams = ring_info(Tbl, State),
    LastVNodeId = RedParams#redundancy_params.last_vnode_id,
    Reply = lookup_fun(LastVNodeId, RedParams),
    {reply, Reply, State};


handle_call(force_sync,_From, State) ->
    NewState = sync_fun(State),
    {reply, ok, NewState};

handle_call(diff_redundancies,_From, #state{
                                        redundancy_params = #redundancy_params{cluster_id = ClusterId},
                                        prev = #ring_info{routing_table = PrevRing}} = State) ->
    PrevRing_1 = leo_gb_trees:to_list(PrevRing),
    RedParams = ring_info(?RING_TBL_CUR, State),
    Reply = case leo_konsul_member:find_all(ClusterId) of
                {ok, MembersCur} ->
                    MembersCur_1 = [{MN, MS} || #?MEMBER{node = MN,
                                                         state = MS} <- MembersCur],
                    diff_redundancies_fun(PrevRing_1, MembersCur_1, RedParams, []);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call(checksum,_From, #state{member_list_checksum = MemberLHashPair,
                                   ring_checksum = RingHashPair} = State) ->
    {reply, {ok, [{member, MemberLHashPair},
                  {ring, RingHashPair}]}, State};

handle_call(dump,_From, #state{
                           redundancy_params = #redundancy_params{cluster_id = ClusterId},
                           cur  = #ring_info{routing_table = CurRoutingTbl},
                           prev = #ring_info{routing_table = PrevRoutingTbl}} = State) ->
    try
        CurRoutingTbl_1 = leo_gb_trees:to_list(CurRoutingTbl),
        PrevRoutingTbl_1 = leo_gb_trees:to_list(PrevRoutingTbl),

        LogDir = ?log_dir(),
        filelib:ensure_dir(LogDir),
        ok = leo_file:file_unconsult(
               lists:append([LogDir,
                             ?DUMP_FILE_RING_CUR,
                             atom_to_list(ClusterId),
                             ".",
                             integer_to_list(leo_date:now())]), CurRoutingTbl_1),
        ok = leo_file:file_unconsult(
               lists:append([LogDir,
                             ?DUMP_FILE_RING_PREV,
                             atom_to_list(ClusterId),
                             ".",
                             integer_to_list(leo_date:now())]), PrevRoutingTbl_1),
        ok
    catch
        _:_ ->
            void
    end,
    {reply, ok, State};

handle_call(subscribe,_From, State) ->
    {reply, ok, State};

handle_call(_Handle, _From, State) ->
    {reply, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(sync, State) ->
    case catch maybe_sync(State) of
        {'EXIT', _Reason} ->
            {noreply, State};
        NewState ->
            {noreply, NewState}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason, _State) ->
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve the number of replicas
%% @private
update_state(#state{redundancy_params = RedundancyParams} = State) ->
    {ok, Options} = leo_konsul_api:get_options(
                      RedundancyParams#redundancy_params.cluster_id),
    NumOfReplica = leo_misc:get_value(?PROP_N, Options, 0),

    case (NumOfReplica > 0) of
        true ->
            RQuorum = leo_misc:get_value(?PROP_R, Options, 0),
            WQuorum = leo_misc:get_value(?PROP_W, Options, 0),
            DQuorum = leo_misc:get_value(?PROP_D, Options, 0),
            NumOfRackAwarenesses = leo_misc:get_value(?PROP_RACK_N, Options, 0),
            State#state{redundancy_params =
                            RedundancyParams#redundancy_params{
                              n = NumOfReplica,
                              r = RQuorum,
                              w = WQuorum,
                              d = DQuorum,
                              num_of_rack_awarenesses = NumOfRackAwarenesses}};
        false ->
            State
    end.


%% @doc Synchronize
%% @private
-spec(sync() ->
             ok | any()).
sync() ->
    Time = erlang:phash2(term_to_binary(leo_date:clock()),
                         (?DEF_SYNC_MAX_INTERVAL - ?DEF_SYNC_MIN_INTERVAL)
                        ) + ?DEF_SYNC_MIN_INTERVAL,
    catch timer:apply_after(Time, gen_server, cast, [self(), sync]).


%% @doc Heatbeat and synchronize with master's ring-table
%% @private
-spec(maybe_sync(#state{}) ->
             #state{}).
maybe_sync(State) ->
    %% Requests a next synchronization
    sync(),

    %% Updates the state
    %%   and executes ring-sync if not equal the members' hash
    %%     between the current hash in the state and the raw-hash
    case update_state(State) of
        #state{min_interval = MinInterval,
               timestamp = Timestamp,
               redundancy_params =
                   #redundancy_params{
                      n = NumOfReplicas}} = State_1 when NumOfReplicas > 0 ->
            ThisTime = ?timestamp(),
            case ((ThisTime - Timestamp) < MinInterval) of
                true ->
                    State#state{timestamp = ThisTime};
                false ->
                    sync_fun(State_1)
            end;
        _ ->
            State
    end.


%% @private
-spec(sync_fun(State) ->
             State when State::#state{}).
sync_fun(#state{redundancy_params = #redundancy_params{cluster_id = ClusterId},
                member_list_checksum = {MemberLHashCur, MemberLHashPrev}} = State) ->
    MemberLHashCur = leo_konsul_member:checksum(ClusterId, ?VER_CUR),
    MemberLHashPrev = leo_konsul_member:checksum(ClusterId, ?VER_PREV),

    State_1 = case (MemberLHashCur /= MemberLHashCur) of
                  %% If not euqual members' hash between the current hash in the state
                  %% and the raw-hash, needs re-generationg RING.cur
                  true when MemberLHashCur > 0 ->
                      case create_fun(ClusterId, ?VER_CUR, State) of
                          {ok, NewState_1} ->
                              NewState_1;
                          _ ->
                              State
                      end;
                  _ ->
                      State
              end,
    State_2 = case (MemberLHashPrev /= MemberLHashPrev) of
                  %% Needs re-generationg RING.prev
                  true when MemberLHashCur > 0 ->
                      case create_fun(ClusterId, ?VER_PREV, State_1) of
                          {ok, NewState_2} ->
                              NewState_2;
                          _ ->
                              State_1
                      end;
                  false ->
                      State_1
              end,
    State_2.


%% @doc Create a RING(routing-table)
%% @private
-spec(create_fun(ClusterId, Ver, State) ->
             {ok, State} | {{error, any()}, State} when ClusterId::cluster_id(),
                                                        Ver::version(),
                                                        State::#state{}).
create_fun(ClusterId, Ver, State) ->
    case leo_konsul_member:get_available_members(ClusterId, Ver) of
        {ok, Members} ->
            case create_fun_1(ClusterId, Ver, Members, []) of
                {ok, {_,[]}} ->
                    {error, not_found};
                {ok, {Members_1, VNodeList}} ->
                    RedundancyParams = State#state.redundancy_params,
                    MemberDict = lists:foldl(fun(#?MEMBER{node = Node} = M, D) ->
                                                     dict:append(Node, M, D)
                                             end, dict:new(), Members_1),
                    case gen_routing_table(RedundancyParams, MemberDict, VNodeList) of
                        {ok, Ring} ->
                            MemberLHashCur = leo_konsul_member:checksum(ClusterId, ?VER_CUR),
                            MemberLHashPrev = leo_konsul_member:checksum(ClusterId, ?VER_PREV),

                            RingChecksum = erlang:crc32(term_to_binary(Ring)),
                            RingInfo = #ring_info{checksum = RingChecksum,
                                                  first_vnode_id = lists:min(leo_gb_trees:keys(Ring)),
                                                  last_vnode_id = lists:max(leo_gb_trees:keys(Ring)),
                                                  members = Members_1,
                                                  routing_table = Ring},
                            {CurRingHash, PrevRingHash} = State#state.ring_checksum,
                            State_1 = State#state{member_list_checksum =
                                                      {MemberLHashCur, MemberLHashPrev}},
                            State_2 = case Ver of
                                          ?VER_CUR ->
                                              State_1#state{
                                                cur = RingInfo,
                                                ring_checksum = {RingChecksum, PrevRingHash}};
                                          ?VER_PREV ->
                                              State_1#state{
                                                prev = RingInfo,
                                                ring_checksum = {CurRingHash, RingChecksum}}
                                      end,
                            {ok, State_2};
                        {error, Cause} ->
                            {{error, Cause}, State}
                    end;
                Error ->
                    {Error, State}
            end;
        {error, not_found} when Ver == ?VER_PREV ->
            %% overwrite current-ring to prev-ring
            case leo_konsul_member:overwrite(ClusterId, ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV) of
                ok ->
                    create_fun(ClusterId, Ver, State);
                Error ->
                    {Error, State}
            end;
        Error ->
            {Error, State}
    end.

%% @private
-spec(create_fun_1(ClusterId, Ver, Members, Acc) ->
             {ok, {Members, VNodeList}} when ClusterId::cluster_id(),
                                             Ver::?VER_CUR|?VER_PREV,
                                             Members::[#?MEMBER{}],
                                             Acc::[#?MEMBER{}],
                                             VNodeList::vnode_list()).
create_fun_1(ClusterId, Ver, [], Acc) ->
    case leo_konsul_member:bulk_insert(ClusterId, Ver, Acc) of
        ok ->
            case create_fun_2(ClusterId, Acc, []) of
                {ok, VNodeList} ->
                    VNodeList_1 = lists:sort(VNodeList),
                    {ok, {Acc, VNodeList_1}};
                Other ->
                    Other
            end;
        Error ->
            Error
    end;
create_fun_1(ClusterId, Ver, [#?MEMBER{state = ?STATE_RESERVED}|Rest], Acc) ->
    create_fun_1(ClusterId, Ver, Rest, Acc);
create_fun_1(ClusterId, Ver, [#?MEMBER{node = Node,
                                       state = State} = Member|Rest], Acc) ->
    %% Modify/Add a member into 'member.table on Mnesia'
    Ret = case leo_konsul_member:find_by_node(ClusterId, Ver, Node) of
              {ok, Member_1} when State == ?STATE_ATTACHED ->
                  {ok, Member_1#?MEMBER{state = ?STATE_RUNNING}};
              {ok, Member_1} ->
                  {ok, Member_1};
              not_found ->
                  {ok, Member#?MEMBER{state = ?STATE_RUNNING}};
              {error, Cause} ->
                  {error, Cause}
          end,
    case Ret of
        {ok, Member_2} ->
            create_fun_1(ClusterId, Ver, Rest, [Member_2|Acc]);
        Error ->
            Error
    end.

%% @private
-spec(create_fun_2(ClusterId, Members, Acc) ->
             {ok, VNodeList} | {error, any()} when ClusterId::cluster_id(),
                                                   Members::[#?MEMBER{}],
                                                   Acc::[#?MEMBER{}],
                                                   VNodeList::vnode_list()).
create_fun_2(_ClusterId, [], Acc) ->
    case leo_konsul_chash:add_from_list(Acc) of
        {ok, VNodeList} ->
            {ok, VNodeList};
        Error ->
            Error
    end;
create_fun_2(ClusterId, [#?MEMBER{alias = []} = Member|Rest], Acc) ->
    {ok, Member_1} = ?alias(Member),

    case leo_konsul_member:insert(ClusterId, Member_1) of
        ok ->
            create_fun_2(ClusterId, Rest, [Member_1|Acc]);
        Error ->
            Error
    end;
create_fun_2(ClusterId, [Member|Rest], Acc) ->
    create_fun_2(ClusterId, Rest, [Member|Acc]).


%% @params
-spec(gen_routing_table(RedundancyParams, MemberDict, VNodeList) ->
             {ok, RoutingTbl} | {error, any()} when RedundancyParams::#redundancy_params{},
                                                    MemberDict::dict:dict(),
                                                    VNodeList::vnode_list(),
                                                    RoutingTbl::leo_gb_trees:tree()).
gen_routing_table(#redundancy_params{n = N},_MemberDict,_VNodeList) when N < 1 ->
    {error, 'invalid_num_of_replicas'};
gen_routing_table(#redundancy_params{n = NumOfReplicas,
                                     num_of_rack_awarenesses = NumOfRackAwarenesses},
                  MemberDict, VNodeList) ->
    gen_routing_table_1(VNodeList, VNodeList,
                        NumOfReplicas, NumOfRackAwarenesses, MemberDict, []).

%% @private
gen_routing_table_1([],_,_,_,_,Acc) ->
    Ring = leo_gb_trees:from_orddict(
             orddict:from_list(
               lists:reverse(Acc))),
    {ok, Ring};
gen_routing_table_1([{VNodeId, {_Node,_RackId}}|Rest] = VNodeList, VNodeList_1,
                    NumOfReplicas, NumOfRackAwarenesses, MemberDict, Acc) ->
    case gen_routing_table_2(VNodeList, VNodeList_1,
                             dict:new(), NumOfReplicas,
                             dict:new(),NumOfRackAwarenesses,
                             MemberDict, []) of
        {ok, Redundancies} ->
            VNodeIdFrom =
                case Acc of
                    [] ->
                        0;
                    [{PrevVNodeId,_}|_] ->
                        PrevVNodeId + 1
                end,
            gen_routing_table_1(Rest, VNodeList_1,
                                NumOfReplicas, NumOfRackAwarenesses, MemberDict,
                                [{VNodeId, {VNodeIdFrom, Redundancies}}|Acc]);
        Error ->
            Error
    end.


%% @private
gen_routing_table_2(VNodeList, VNodeList_1,
                    NodeDict, NumOfReplicas,
                    RackDict, NumOfRackAwarenesses,
                    MemberDict, Acc) ->
    NodeDictLen = dict:size(NodeDict),
    MemberDictLen = dict:size(MemberDict),

    case (NodeDictLen == MemberDictLen) of
        true ->
            %% @DEBUG
            %% ?debugVal(dict:to_list(RackDict)),
            {ok, lists:reverse(Acc)};
        false when VNodeList == [] ->
            gen_routing_table_2(VNodeList_1, VNodeList_1,
                                NodeDict, NumOfReplicas,
                                RackDict, NumOfRackAwarenesses,
                                MemberDict, Acc);
        false ->
            [{VNodeId, {Node, RackId}}|Rest] = VNodeList,

            %% Check the redundanct nodes to eliminate duplicate nodes
            case dict:find(Node, NodeDict) of
                {ok,_} ->
                    gen_routing_table_2(Rest, VNodeList_1,
                                        NodeDict, NumOfReplicas,
                                        RackDict, NumOfRackAwarenesses,
                                        MemberDict, Acc);
                _ ->
                    %% Check rack-awareness
                    RackDictLen = dict:size(RackDict),
                    RackExists = dict:find(RackId, RackDict) /= error,

                    case (NumOfRackAwarenesses > 0 andalso
                          NumOfRackAwarenesses + 1 - RackDictLen > 0) of
                        true when RackExists == true ->
                            gen_routing_table_2(Rest, VNodeList_1,
                                                NodeDict, NumOfReplicas,
                                                RackDict, NumOfRackAwarenesses,
                                                MemberDict, Acc);
                        _ ->
                            NodeDict_1 = dict:append(Node, VNodeId, NodeDict),
                            MemberState = case dict:find(Node, MemberDict) of
                                              {ok, #?MEMBER{state = ?STATE_ATTACHED}} ->
                                                  true;
                                              {ok, #?MEMBER{state = ?STATE_RUNNING}} ->
                                                  true;
                                              _ ->
                                                  false
                                          end,
                            {ReadRepairState, Role} =
                                case NodeDictLen of
                                    0 ->
                                        {true, ?CNS_ROLE_LEADER};
                                    N when N < NumOfReplicas ->
                                        {true, ?CNS_ROLE_FOLLOWER_1};
                                    _ ->
                                        {false, ?CNS_ROLE_OBSERBER}
                                end,
                            RackDict_1 = dict:append(RackId, {Node, Role}, RackDict),
                            gen_routing_table_2(Rest, VNodeList_1,
                                                NodeDict_1, NumOfReplicas,
                                                RackDict_1, NumOfRackAwarenesses,
                                                MemberDict, [#redundant_node{
                                                                node = Node,
                                                                vnode_id = VNodeId,
                                                                available = MemberState,
                                                                can_read_repair = ReadRepairState,
                                                                role = Role} | Acc])
                    end
            end
    end.


%% @doc Retrieve redundancies by vnode-id
%% @private
-spec(lookup_fun(AddrId, RedundancyParams) ->
             not_found | {ok, #redundancies{}} when AddrId::non_neg_integer(),
                                                    RedundancyParams::#redundancy_params{}).
lookup_fun(AddrId, RedundancyParams) ->
    lookup_fun(AddrId, RedundancyParams, RedundancyParams#redundancy_params.n).

-spec(lookup_fun(AddrId, RedundancyParams, TotalNumOfRedundancies) ->
             not_found | {ok, #redundancies{}} when AddrId::non_neg_integer(),
                                                    RedundancyParams::#redundancy_params{},
                                                    TotalNumOfRedundancies::pos_integer()).
lookup_fun(AddrId, #redundancy_params{n = NumOfReplicas,
                                      r = RQuorum,
                                      w = WQuorum,
                                      d = DQuorum,
                                      num_of_rack_awarenesses = NumOfRackAwarenesses} = RedundancyParams,
           TotalNumOfRedundancies) ->
    case lookup_fun_1(RedundancyParams, TotalNumOfRedundancies, AddrId) of
        {ok, {VNodeId, VNodeIdFrom, Redundancies}} ->
            {ok, #redundancies{
                    id = AddrId,
                    vnode_id_from = VNodeIdFrom,
                    vnode_id_to = VNodeId,
                    nodes = Redundancies,
                    n = NumOfReplicas,
                    r = RQuorum,
                    w = WQuorum,
                    d = DQuorum,
                    level_2 = NumOfRackAwarenesses,
                    ring_hash = -1} %% @TODO
            };
        Error ->
            Error
    end.

%% @private
lookup_fun_1(#redundancy_params{
                first_vnode_id = FirstVNodeId,
                last_vnode_id = LastVNodeId,
                routing_table = RoutingTbl}, TotalNumOfRedundancies, AddrId) ->
    %% Retrieves a new address-id if its value nears first-vnode-id or last-vnode-id
    AddrId_1 = case (FirstVNodeId > AddrId) of
                   true ->
                       FirstVNodeId;
                   false when LastVNodeId < AddrId ->
                       FirstVNodeId;
                   false ->
                       AddrId
               end,

    %% Retrieves redundancies by address-id
    case catch leo_gb_trees:lookup(AddrId_1, RoutingTbl) of
        {'EXIT',_} ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        nil ->
            {error, ?ERROR_COULD_NOT_GET_REDUNDANCIES};
        {VNodeId, {VNodeIdFrom, Redundancies}} ->
            {ok, {VNodeId, VNodeIdFrom,
                  lists:sublist(Redundancies, TotalNumOfRedundancies)}}
    end.


%% @private
diff_redundancies_fun([],_MembersCur,_RedundancyParams, Acc) ->
    {ok, lists:reverse(Acc)};
diff_redundancies_fun([{VNodeIdToPrev, {_, RedNodesPrev}}|Rest],
                      MembersCur, #redundancy_params{n = NumOfReplicas} = RedParams, Acc) ->
    case lookup_fun(VNodeIdToPrev, RedParams) of
        {ok, #redundancies{
                vnode_id_to = VNodeIdToCur,
                nodes = RedNodesCur}} ->
            %% Adjusts Previous Redundant Nodes
            RedNodesPrev_1 =
                case (length(RedNodesPrev) > NumOfReplicas) of
                    true ->
                        lists:sublist(RedNodesPrev, NumOfReplicas);
                    false ->
                        RedNodesPrev
                end,
            RedNodesPrev_2 =
                lists:filter(fun(#redundant_node{node = N}) ->
                                     case lists:keyfind(N, 1, MembersCur) of
                                         {_, ?STATE_DETACHED} ->
                                             false;
                                         _ ->
                                             true
                                     end
                             end, RedNodesPrev_1),

            %% Retrieves the pair-list of 'sender' and 'receiver'
            Acc_1 =
                case (RedNodesPrev_2 == RedNodesCur) of
                    true ->
                        Acc;
                    false ->
                        F = fun(L1, L2) ->
                                    case (length(L1) < length(L2)) of
                                        %% In case of existing detached node(s)
                                        true ->
                                            [#redundant_node{node = N1}|_] = L1,
                                            [N1];
                                        false ->
                                            lists:foldl(
                                              fun(#redundant_node{node = N1}, SoFar) ->
                                                      case lists:any(fun(#redundant_node{node = N2}) ->
                                                                             N1 == N2
                                                                     end, L2) of
                                                          true ->
                                                              SoFar;
                                                          false ->
                                                              [N1|SoFar]
                                                      end
                                              end, [], L1)
                                    end
                            end,
                        Senders = F(RedNodesPrev_2, RedNodesCur),
                        Receivers = F(RedNodesCur, RedNodesPrev_2),
                        [#diff_redundancies{vnode_id = VNodeIdToCur,
                                            senders = Senders,
                                            receivers = Receivers
                                           } | Acc]
                end,
            diff_redundancies_fun(Rest, MembersCur, RedParams, Acc_1);
        Other ->
            {error, Other}
    end.


%% @doc Retrieve ring-info
%% @private
-spec(ring_info(?RING_TBL_CUR|?RING_TBL_PREV, #state{}) ->
             #redundancy_params{}).
ring_info(?RING_TBL_CUR, #state{
                            cur = #ring_info{first_vnode_id = FirstVNodeId,
                                             last_vnode_id = LastVNodeId,
                                             routing_table = RoutingTbl},
                            redundancy_params = RedParams}) ->
    RedParams#redundancy_params{first_vnode_id = FirstVNodeId,
                                last_vnode_id = LastVNodeId,
                                routing_table = RoutingTbl};
ring_info(?RING_TBL_PREV, #state{
                             prev = #ring_info{first_vnode_id = FirstVNodeId,
                                               last_vnode_id = LastVNodeId,
                                               routing_table = RoutingTbl},
                             redundancy_params = RedParams}) ->
    RedParams#redundancy_params{first_vnode_id = FirstVNodeId,
                                last_vnode_id = LastVNodeId,
                                routing_table = RoutingTbl}.
