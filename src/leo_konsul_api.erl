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
%% ---------------------------------------------------------------------
%% Leo Konsul - API
%%
%% @doc Leo Konsul API
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_konsul_api.erl
%% @end
%%======================================================================
-module(leo_konsul_api).

-include("leo_konsul.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Ring-related
-export([start/2,
         create/1, create/2, create/3, create/4,
         set_options/2, get_options/1, get_option/2,

         join/2, reserve/2,
         leave/2, leave/3,
         suspend/2, suspend/3,

         checksum/2, synchronize/3, synchronize/4,
         get_ring/1, get_ring/2, dump/2
        ]).
%% Redundancy-related
-export([get_redundancies_by_key/2, get_redundancies_by_key/3,
         get_redundancies_by_key/4,
         get_redundancies_by_addr_id/2, get_redundancies_by_addr_id/3,
         get_redundancies_by_addr_id/4,
         collect_redundancies_by_key/3,
         part_of_collect_redundancies_by_key/5,
         range_of_vnodes/2, rebalance/1,
         get_alias/3
        ]).
%% Member-related
-export([has_member/2, has_charge_of_node/3,
         get_members/1, get_members/2, get_all_ver_members/1,
         get_member_by_node/2, get_members_count/1,
         get_members_by_status/2, get_members_by_status/3,
         update_member/2, update_members/2,
         update_member_by_node/3, update_member_by_node/4,

         delete_member_by_node/2,
         table_info/1,
         force_sync_workers/1,
         get_cluster_status/1,
         get_cluster_tbl_checksums/1
        ]).

%% Multi-DC-replciation-related
-export([get_remote_clusters/1, get_remote_clusters_by_limit/1,
         get_remote_members/1, get_remote_members/2
        ]).

%% Request type
-type(method() :: put | get | delete | head | default).

%%--------------------------------------------------------------------
%% API-1  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Launch a node in a cluster
start(ClusterId, Options) ->
    leo_konsul_sup:start_child(ClusterId, Options).


%% @doc Create the RING
-spec(create(ClusterId) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId) ->
    case create_cur(ClusterId) of
        {ok, Members, HashValues} ->
            case create_prev(ClusterId) of
                {ok,_,_} ->
                    {ok, Members, HashValues};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec(create(ClusterId, Ver) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver) when Ver == ?VER_CUR;
                            Ver == ?VER_PREV ->
    case leo_konsul_ring:create(ClusterId, Ver) of
        ok ->
            case leo_cluster_tbl_member:find_by_cluster_id(
                   ?member_table(Ver), ClusterId) of
                {ok, Members} ->
                    {ok, HashRing} = checksum(ClusterId, ?CHECKSUM_RING),
                    ok = leo_misc:set_env(?APP, ?PROP_RING_HASH,
                                          erlang:element(1, HashRing)),
                    {ok, HashMember} = checksum(ClusterId, ?CHECKSUM_MEMBER),
                    {ok, Members, [{?CHECKSUM_RING,   HashRing},
                                   {?CHECKSUM_MEMBER, HashMember}]};
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
create(_,_) ->
    {error, invalid_args}.

-spec(create(ClusterId, Ver, Members) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver, Members) ->
    create(ClusterId, Ver, Members, []).

-spec(create(ClusterId, Ver, Members, Options) ->
             {ok, Members, HashValues} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Members::[#?MEMBER{}],
                                 Options::[{atom(), any()}],
                                 HashValues::[{atom(), integer()}]).
create(ClusterId, Ver, [], []) ->
    create(ClusterId, Ver);
create(ClusterId, Ver, [], Options) ->
    ok = set_options(ClusterId, Options),
    create(ClusterId, Ver);
create(ClusterId, Ver, [#?MEMBER{node = Node} = Member|T], Options) when Ver == ?VER_CUR;
                                                                         Ver == ?VER_PREV ->
    %% Add a member as "joined node" into member-table
    case leo_cluster_tbl_member:lookup(ClusterId, Node) of
        not_found ->
            leo_cluster_tbl_member:insert(Member#?MEMBER{cluster_id = ClusterId,
                                                         node = Node,
                                                         state = ?STATE_ATTACHED});
        _ ->
            void
    end,
    create(ClusterId, Ver, T, Options);
create(_,_,_,_) ->
    {error, invalid_version}.


%% @private
create_cur(ClusterId) ->
    case leo_cluster_tbl_member:table_size(
           ?MEMBER_TBL_CUR, ClusterId) of
        {error, Reason} ->
            {error, Reason};
        0 ->
            {error, member_not_found};
        _ ->
            create(ClusterId, ?VER_CUR)
    end.

%% @private
create_prev(ClusterId) ->
    case leo_cluster_tbl_member:table_size(
           ?MEMBER_TBL_PREV, ClusterId) of
        {error, Reason} ->
            {error, Reason};
        0 ->
            case leo_cluster_tbl_member:overwrite(
                   ?MEMBER_TBL_CUR, ?MEMBER_TBL_PREV, ClusterId) of
                ok ->
                    create_prev(ClusterId);
                Error ->
                    Error
            end;
        _ ->
            create(ClusterId, ?VER_PREV)
    end.


%% @doc set routing-table's options.
-spec(set_options(ClusterId, Options) ->
             ok when ClusterId::cluster_id(),
                     Options::[{atom(), any()}]).
set_options(ClusterId, Options) ->
    ok = leo_misc:set_env(?APP, ?id_system_conf(ClusterId), Options),
    ok.


%% @doc get routing-table's options.
-spec(get_options(ClusterId) ->
             {ok, Options} when ClusterId::cluster_id(),
                                Options::[{atom(), any()}]).
get_options(ClusterId) ->
    case leo_misc:get_env(?APP, ?id_system_conf(ClusterId)) of
        undefined ->
            %% @TODO
            case catch leo_cluster_tbl_conf:get(ClusterId) of
                {ok, #?SYSTEM_CONF{} = SystemConf} ->
                    Options = record_to_tuplelist(SystemConf),
                    ok = set_options(ClusterId, Options),
                    {ok, Options};
                _ ->
                    Options = record_to_tuplelist(
                                #?SYSTEM_CONF{}),
                    {ok, Options}
            end;
        Ret ->
            Ret
    end.


%% @doc get routing-table's options.
-spec(get_option(ClusterId, Item) ->
             Value when ClusterId::cluster_id(),
                        Item::atom(),
                        Value::any()).
get_option(ClusterId, Item) ->
    {ok, Options} = get_options(ClusterId),
    leo_misc:get_value(Item, Options, 0).


%% @doc record to tuple-list for converting system-conf
%% @private
-spec(record_to_tuplelist(Value) ->
             [{atom(), any()}] when Value::any()).
record_to_tuplelist(Value) ->
    lists:zip(
      record_info(fields, ?SYSTEM_CONF), tl(tuple_to_list(Value))).


%% @doc join a node.
-spec(join(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
join(ClusterId, Member) ->
    case leo_konsul_member:join(ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc reserve a node during in operation
-spec(reserve(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
reserve(ClusterId, Member) ->
    case leo_konsul_member:reserve(ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc leave a node.
-spec(leave(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
leave(ClusterId, Node) ->
    leave(ClusterId, Node, leo_date:clock()).

-spec(leave(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
leave(ClusterId, Node, Clock) ->
    case leo_konsul_member:leave(ClusterId, Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc suspend a node. (disable)
-spec(suspend(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
suspend(ClusterId, Node) ->
    suspend(ClusterId, Node, leo_date:clock()).

-spec(suspend(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
suspend(ClusterId, Node, Clock) ->
    case leo_konsul_member:suspend(ClusterId, Node, Clock) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc get routing_table's checksum.
-spec(checksum(ClusterId, Type) ->
             {ok, integer()} |
             {ok, {integer(), integer()}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Type::checksum_type()).
checksum(ClusterId, ?CHECKSUM_MEMBER) ->
    {ok, HashPairL} = leo_konsul_ring:checksum(ClusterId),
    MemberLHashPair = leo_misc:get_value('member', HashPairL, {-1,-1}),
    {ok, MemberLHashPair};
checksum(ClusterId, ?CHECKSUM_RING) ->
    {ok, HashPairL} = leo_konsul_ring:checksum(ClusterId),
    RingHashPair = leo_misc:get_value('ring', HashPairL, {-1,-1}),
    {ok, RingHashPair};
checksum(ClusterId, ?CHECKSUM_SYS_CONF) ->
    {ok, SysConf} = get_options(ClusterId),
    {ok, erlang:crc32(term_to_binary(SysConf))};
checksum(_,_) ->
    {error, invalid_type}.


%% @doc synchronize member-list and routing-table.
-spec(synchronize(ClusterId, SyncTarget, SyncData, Options) ->
             {ok, [{atom(), any()}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}],
                                 Options::[{atom(), any()}]).
synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData, Options) ->
    %% set configurations
    case Options of
        [] -> void;
        _ ->
            ok = set_options(ClusterId, Options)
    end,

    %% Synchronize current and previous members
    %%   Then Synchronize ring
    case synchronize(ClusterId, ?SYNC_TARGET_MEMBER, SyncData) of
        {ok,_ChecksumMembers} ->
            %% @TODO Creates a new RING both CURRENT and PREVIOUS (2018-02-15)
            %% case synchronize_1(ClusterId, ?SYNC_TARGET_RING_CUR, ?VER_CUR) of
            %%     ok ->
            %%         case synchronize_1(ClusterId, ?SYNC_TARGET_RING_PREV, ?VER_PREV) of
            %%             ok ->
            %%                 {ok, ChecksumRing} = checksum(ClusterId, ?CHECKSUM_RING),
            %%                 {ok, [{?CHECKSUM_MEMBER, ChecksumMembers},
            %%                       {?CHECKSUM_RING,   ChecksumRing}
            %%                      ]};
            %%             Error ->
            %%                 Error
            %%         end;
            %%     Error ->
            %%         Error
            %% end;
            ok;
        Error ->
            Error
    end.

%% @doc synchronize member-list and routing-table.
-spec(synchronize(ClusterId, SyncTarget, SyncData) ->
             {ok, integer()} |
             {ok, [{atom(), any()}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 SyncTarget::sync_target(),
                                 SyncData::[{atom(), any()}]).
synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData) ->
    synchronize(ClusterId, ?SYNC_TARGET_BOTH, SyncData, []);

synchronize(ClusterId, ?SYNC_TARGET_MEMBER = SyncTarget, SyncData) ->
    case synchronize_1(ClusterId, SyncTarget, ?VER_CUR,  SyncData) of
        ok ->
            case synchronize_1(ClusterId, SyncTarget, ?VER_PREV, SyncData) of
                ok ->
                    checksum(ClusterId, ?CHECKSUM_MEMBER);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;

synchronize(_ClusterId, Target, []) when ?SYNC_TARGET_RING_CUR  == Target;
                                         ?SYNC_TARGET_RING_PREV == Target ->
    %% @TODO Creates a new RING (2018-02-15)
    %% synchronize_1(ClusterId, Target, ?sync_target_to_ver(Target));
    ok;
synchronize(ClusterId, Target, SyncData) when ?SYNC_TARGET_RING_CUR  == Target;
                                              ?SYNC_TARGET_RING_PREV == Target ->
    {ok,_ChecksumMembers} = synchronize(ClusterId, ?SYNC_TARGET_MEMBER, SyncData),
    %% @TODO Creates a new RING (2018-02-15)
    %% case synchronize_1(ClusterId, Target, ?sync_target_to_ver(Target)) of
    %%     ok ->
    %%         {ok, ChecksumRing} = checksum(ClusterId, ?CHECKSUM_RING),
    %%         {ok, [{?CHECKSUM_MEMBER, ChecksumMembers},
    %%               {?CHECKSUM_RING,   ChecksumRing}
    %%              ]};
    %%     Error ->
    %%         Error
    %% end;
    ok;
synchronize(_,_,_) ->
    {error, invalid_target}.

%% @private
synchronize_1(ClusterId, ?SYNC_TARGET_MEMBER, Ver, SyncData) ->
    case leo_misc:get_value(Ver, SyncData, []) of
        [] ->
            ok;
        NewMembers ->
            Table = ?member_table(Ver),
            %% @TODO
            case leo_cluster_tbl_member:find_by_cluster_id(Table, ClusterId) of
                {ok, OldMembers} ->
                    leo_konsul_member:bulk_update(
                      ClusterId, Table, OldMembers, NewMembers);
                not_found ->
                    leo_konsul_member:bulk_update(
                      ClusterId, Table, [], NewMembers);
                Error ->
                    Error
            end
    end.


%% @TODO
%% @doc Retrieve Ring
-spec(get_ring(ClusterId) ->
             {ok, [tuple()]} when ClusterId::cluster_id()).
get_ring(ClusterId) ->
    get_ring(ClusterId, ?SYNC_TARGET_RING_CUR).

-spec(get_ring(ClusterId, SyncTarget) ->
             {ok, [tuple()]} when ClusterId::cluster_id(),
                                  SyncTarget::sync_target()).
get_ring(_ClusterId, ?SYNC_TARGET_RING_CUR) ->
    %% @TODO (2018-01-18)
    %% case leo_cluster_tbl_ring:find_by_cluster_id(
    %%        table_info(?VER_CUR), ClusterId) of
    %%     {ok, VNodeList} ->
    %%         {ok, VNodeList};
    %%     Other ->
    %%         Other
    %% end;
    {ok, []};
get_ring(_ClusterId, ?SYNC_TARGET_RING_PREV) ->
    %% @TODO (2018-01-18)
    %% case leo_cluster_tbl_ring:find_by_cluster_id(
    %%        table_info(?VER_PREV), ClusterId) of
    %%     {ok, VNodeList} ->
    %%         {ok, VNodeList};
    %%     Other ->
    %%         Other
    %% end.
    {ok, []}.


%% @doc Dump table-records.
-spec(dump(ClusterId, Type) ->
             ok when ClusterId::cluster_id(),
                     Type::member|ring|both).
dump(ClusterId, both) ->
    catch dump(ClusterId, member),
    catch dump(ClusterId, ring),
    ok;
dump(ClusterId, member) ->
    leo_konsul_member:dump(ClusterId);
dump(ClusterId, ring) ->
    leo_konsul_ring:dump(ClusterId);
dump(_,_) ->
    ok.


%%--------------------------------------------------------------------
%% API-2  FUNCTIONS (leo_routing_tbl_provide_server)
%%--------------------------------------------------------------------
%% @doc Retrieve redundancies from the ring-table.
-spec(get_redundancies_by_key(ClusterId, Key) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Key::binary()).
get_redundancies_by_key(ClusterId, Key) ->
    get_redundancies_by_key(ClusterId, default, Key).

-spec(get_redundancies_by_key(ClusterId, Method, Key) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 Key::binary()).
get_redundancies_by_key(ClusterId, Method, Key) ->
    {ok, Options} = get_options(ClusterId),
    AddrId = leo_konsul_chash:vnode_id(Key),
    get_redundancies_by_addr_id_1(
      ClusterId, ring_table(Method), AddrId, Options).

-spec(get_redundancies_by_key(ClusterId, Method, Key, ConsistencyLevel) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 Key::binary(),
                                 ConsistencyLevel::[{ConsistencyItem, ConsistencyValue}],
                                 ConsistencyItem::consistency_item(),
                                 ConsistencyValue::pos_integer()).
get_redundancies_by_key(ClusterId, Method, Key, ConsistencyLevel) ->
    {ok, Options} = get_options(ClusterId),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    AddrId = leo_konsul_chash:vnode_id(BitOfRing, Key),
    get_redundancies_by_addr_id(ClusterId, Method, AddrId, ConsistencyLevel).


%% @doc Retrieve redundancies from the ring-table.
-spec(get_redundancies_by_addr_id(ClusterId, AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 AddrId::integer()).
get_redundancies_by_addr_id(ClusterId, AddrId) ->
    get_redundancies_by_addr_id(ClusterId, default, AddrId).

-spec(get_redundancies_by_addr_id(ClusterId, Method, AddrId) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 AddrId::integer()).
get_redundancies_by_addr_id(ClusterId, Method, AddrId) ->
    {ok, Options} = get_options(ClusterId),
    get_redundancies_by_addr_id_1(ClusterId, ring_table(Method), AddrId, Options).

-spec(get_redundancies_by_addr_id(ClusterId, Method, AddrId, ConsistencyLevel) ->
             {ok, #redundancies{}} |
             {error, any()} when ClusterId::cluster_id(),
                                 Method::method(),
                                 AddrId::integer(),
                                 ConsistencyLevel::[{ConsistencyItem, ConsistencyValue}],
                                 ConsistencyItem::consistency_item(),
                                 ConsistencyValue::pos_integer()).
get_redundancies_by_addr_id(ClusterId, Method, AddrId, ConsistencyLevel) ->
    {ok, Options} = get_options(ClusterId),
    N_Value = leo_misc:get_value(?PROP_N, Options),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    MDCR_N_Value = leo_misc:get_value(?PROP_N, ConsistencyLevel),

    case (N_Value >= MDCR_N_Value) of
        true ->
            MDCR_R_Value = leo_misc:get_value(?PROP_R, ConsistencyLevel),
            MDCR_W_Value = leo_misc:get_value(?PROP_W, ConsistencyLevel),
            MDCR_D_Value = leo_misc:get_value(?PROP_D, ConsistencyLevel),
            Options_1 = [{?PROP_N, MDCR_N_Value},
                         {?PROP_R, MDCR_R_Value},
                         {?PROP_W, MDCR_W_Value},
                         {?PROP_D, MDCR_D_Value},
                         {?PROP_RING_BIT, BitOfRing}],
            get_redundancies_by_addr_id_1(ClusterId, ring_table(Method), AddrId, Options_1);
        false ->
            {error, ?ERROR_INVALID_MDCR_CONFIG}
    end.

%% @private
-spec(get_redundancies_by_addr_id_1(ClusterId, TableInfo, AddrId, Options) ->
             {ok, #redundancies{}} | {error, any()} when ClusterId::cluster_id(),
                                                         TableInfo::{atom(), atom()},
                                                         AddrId::non_neg_integer(),
                                                         Options::[{atom(), any()}]).
get_redundancies_by_addr_id_1(ClusterId, Tbl, AddrId, Options) ->
    N = leo_misc:get_value(?PROP_N, Options),
    R = leo_misc:get_value(?PROP_R, Options),
    W = leo_misc:get_value(?PROP_W, Options),
    D = leo_misc:get_value(?PROP_D, Options),

    case leo_konsul_ring:lookup(ClusterId, Tbl, AddrId) of
        {ok, Redundancies} ->
            RedundantNodes = Redundancies#redundancies.nodes,
            LenNodes = erlang:length(RedundantNodes),
            RedundantNodes_1 =
                case (LenNodes > N) of
                    true ->
                        lists:sublist(RedundantNodes, N);
                    false ->
                        RedundantNodes
                end,

            CurRingHash =
                case leo_misc:get_env(?APP, ?PROP_RING_HASH) of
                    {ok, RingHash} ->
                        RingHash;
                    undefined ->
                        {ok, {RingHash, _}} = checksum(ClusterId, ?CHECKSUM_RING),
                        ok = leo_misc:set_env(?APP, ?PROP_RING_HASH, RingHash),
                        RingHash
                end,
            {ok, Redundancies#redundancies{n = N,
                                           r = R,
                                           w = W,
                                           d = D,
                                           nodes = RedundantNodes_1,
                                           ring_hash = CurRingHash}};
        not_found = Cause ->
            {error, Cause}
    end.


%% @doc Collect redundant nodes for LeoFS' erasure-coding
-spec(collect_redundancies_by_key(ClusterId, Key, TotalNumOfRedundancies) ->
             {ok, {Options, RedundantNodeL}}|{error, any()}
                 when ClusterId::cluster_id(),
                      Key::binary(),
                      TotalNumOfRedundancies::pos_integer(),
                      Options::[{atom(), any()}],
                      RedundantNodeL::[#redundancies{}]).
collect_redundancies_by_key(ClusterId, Key, TotalNumOfRedundancies) ->
    {ok, Options} = get_options(ClusterId),
    BitOfRing = leo_misc:get_value(?PROP_RING_BIT, Options),
    AddrId = leo_konsul_chash:vnode_id(BitOfRing, Key),

    case leo_konsul_ring:lookup(
           ClusterId, ring_table(default), AddrId, TotalNumOfRedundancies) of
        {ok, #redundancies{nodes = Nodes}} ->
            {ok, {Options, Nodes}};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


-spec(part_of_collect_redundancies_by_key(
        ClusterId, Index, ChildKey, NumOfReplicas, TotalNumOfRedundancies) ->
             {ok, RedundantNode}|{error, any()}
                 when ClusterId::cluster_id(),
                      Index::pos_integer(),
                      ChildKey::binary(),
                      NumOfReplicas::pos_integer(),
                      TotalNumOfRedundancies::pos_integer(),
                      RedundantNode::#redundant_node{}).
part_of_collect_redundancies_by_key(ClusterId, Index, ChildKey,
                                    NumOfReplicas, TotalNumOfRedundancies) ->
    ParentKey = begin
                    case binary:matches(ChildKey, [<<"\n">>], []) of
                        [] ->
                            ChildKey;
                        PosL ->
                            {Pos,_} = lists:last(PosL),
                            binary:part(ChildKey, 0, Pos)
                    end
                end,
    case collect_redundancies_by_key(ClusterId, ParentKey, TotalNumOfRedundancies) of
        {ok, {_Options, RedundantNodeL}}
          when erlang:length(RedundantNodeL) >= NumOfReplicas ->
            {ok, lists:nth(Index, RedundantNodeL)};
        Other ->
            Other
    end.


%% @doc Retrieve range of vnodes.
-spec(range_of_vnodes(ClusterId, ToVNodeId) ->
             {ok, [tuple()]} when ClusterId::cluster_id(),
                                  ToVNodeId::integer()).
range_of_vnodes(ClusterId, ToVNodeId) ->
    TblInfo = table_info(?VER_CUR),
    leo_konsul_chash:range_of_vnodes(TblInfo, ClusterId, ToVNodeId).


%% @doc Re-balance objects in the cluster.
-spec(rebalance(ClusterId) ->
             {ok, [tuple()]} | {error, any()} when ClusterId::cluster_id()).
rebalance(ClusterId) ->
    %% Exec takeover-nodes
    Ret = case leo_cluster_tbl_member:find_by_status(
                 ?MEMBER_TBL_CUR, ClusterId, ?STATE_ATTACHED) of
              {ok, JoinedMembers} ->
                  {ok, JoinedMembers};
              not_found ->
                  {ok, []};
              Error_1 ->
                  Error_1
          end,
    rebalance_1(Ret, ClusterId).

%% @private
rebalance_1({ok, JoinedMembers}, ClusterId) ->
    Ret = case leo_cluster_tbl_member:find_by_status(
                 ?MEMBER_TBL_CUR, ClusterId, ?STATE_DETACHED) of
              {ok, LeavedMembers} ->
                  {ok, {JoinedMembers, LeavedMembers}};
              not_found ->
                  {ok, {JoinedMembers, []}};
              Error ->
                  Error
          end,
    rebalance_2(Ret, ClusterId);
rebalance_1(Error,_ClusterId) ->
    Error.

%% @private
rebalance_2({ok, {JoinedMembers, LeavedMembers}}, ClusterId) ->
    %% Update all members
    {ok, JoinedMembers_1} = make_pair_of_takeover(
                              JoinedMembers, LeavedMembers, []),
    case leo_cluster_tbl_member:bulk_insert(
           ?MEMBER_TBL_CUR, JoinedMembers_1) of
        ok ->
            case create(ClusterId) of
                {ok, Members_1, HashValues} ->
                    %% Retrieve different redundancies
                    case leo_konsul_ring:diff_redundancies(ClusterId) of
                        {ok, DiffRedundancies} ->
                            case after_retrieving_diff_redundancies(ClusterId) of
                                ok ->
                                    {ok, DiffRedundancies};
                                Error ->
                                    Error
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
rebalance_2(Error,_) ->
    Error.


%% @doc Update the record of an joined node
%% @private
make_pair_of_takeover([],_, Acc) ->
    {ok, lists:reverse(Acc)};
make_pair_of_takeover([JoinedMember|RestJoinedMembers], LeavedMemberL, Acc) ->
    {Alias_1, RestLeavedMembers_1} =
        case LeavedMemberL of
            [] ->
                {JoinedMember#?MEMBER.alias, []};
            [#?MEMBER{alias = Alias,
                      node = Node}|RestLeavedMembers] ->
                {Alias, RestLeavedMembers}
        end,
    make_pair_of_takeover(RestJoinedMembers, RestLeavedMembers_1,
                          [JoinedMember#?MEMBER{alias = Alias_1,
                                                state = ?STATE_RUNNING,
                                                clock = leo_date:clock()}|Acc]).


%% @doc After execute rebalance#2:
%%      1. After exec taking over data from leave-node to join-node
%%      2. Remove leaved-nodes fomr ring and members
%%      3. Synchronize previous-ring
%%      4. Export members and ring
%% @private
-spec(after_retrieving_diff_redundancies(ClusterId) ->
             ok | {error, any()} when ClusterId::cluster_id()).
after_retrieving_diff_redundancies(ClusterId) ->
    %% @TODO
    ok.


%% @doc Generate an alias of the node
-spec(get_alias(ClusterId, Node, RackId) ->
             {ok, tuple()} when ClusterId::cluster_id(),
                                Node::atom(),
                                RackId::string()).
get_alias(ClusterId, Node, RackId) ->
    case leo_cluster_tbl_member:find_by_status(
           ClusterId, ?STATE_DETACHED) of
        not_found ->
            get_alias_1([], ClusterId, Node, RackId);
        {ok, Members} ->
            get_alias_1(Members, ClusterId, Node, RackId);
        {error, Cause} ->
            {error, Cause}
    end.

%% @private
get_alias_1([],_ClusterId,Node,_RackId) ->
    PartOfAlias = string:substr(
                    leo_hex:binary_to_hex(
                      crypto:hash(md5, lists:append([atom_to_list(Node)]))),1,8),
    {ok, lists:append([?NODE_ALIAS_PREFIX, PartOfAlias])};

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      node = Node_1}|Rest],
            ClusterId, Node, RackId) when ClusterId == ClusterId_1,
                                          Node == Node_1 ->
    get_alias_1(Rest, ClusterId, Node, RackId);

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      alias = [],
                      node  = Node_1}|Rest],
            ClusterId, Node, RackId) when ClusterId == ClusterId_1,
                                          Node /= Node_1 ->
    get_alias_1(Rest, ClusterId, Node, RackId);

get_alias_1([#?MEMBER{cluster_id = ClusterId_1,
                      alias = Alias,
                      node  = Node_1,
                      grp_level_2 = RackId_1}|Rest],
            ClusterId, Node, RackId) when ClusterId == ClusterId_1,
                                          Node  /= Node_1,
                                          RackId == RackId_1 ->
    case leo_cluster_tbl_member:find_by_alias(ClusterId, Alias) of
        {ok, [Member|_]} ->
            %% {ok, {Member, Member#?MEMBER.alias}};
            {ok, Member#?MEMBER.alias};
        _ ->
            get_alias_1(Rest, ClusterId, Node, RackId)
    end;

get_alias_1([_|Rest], ClusterId, Node, RackId) ->
    get_alias_1(Rest, ClusterId, Node, RackId).


%%--------------------------------------------------------------------
%% API-3  FUNCTIONS (leo_member_management_server)
%%--------------------------------------------------------------------
%% @doc Has a member ?
-spec(has_member(ClusterId, Node) ->
             boolean() when ClusterId::cluster_id(),
                            Node::atom()).
has_member(ClusterId, Node) ->
    leo_konsul_member:has_member(ClusterId, Node).


%% @doc Has charge of node?
%%      'true' is returned even if it detects an error
-spec(has_charge_of_node(ClusterId, Key, NumOfReplicas) ->
             boolean() when ClusterId::cluster_id(),
                            Key::binary(),
                            NumOfReplicas::integer()).
has_charge_of_node(ClusterId, Key, 0) ->
    case leo_cluster_tbl_conf:get(ClusterId) of
        {ok, #?SYSTEM_CONF{n = NumOfReplica}} ->
            has_charge_of_node(ClusterId, Key, NumOfReplica);
        _ ->
            true
    end;
has_charge_of_node(ClusterId, Key, NumOfReplica) ->
    case get_redundancies_by_key(ClusterId, put, Key) of
        {ok, #redundancies{nodes = Nodes}} ->
            Nodes_1 = lists:sublist(Nodes, NumOfReplica),
            lists:foldl(
              fun(#redundant_node{node = N,
                                  can_read_repair = CanReadRepair}, false) ->
                      (N == erlang:node() andalso
                       CanReadRepair == true);
                 (_, true ) ->
                      true
              end, false, Nodes_1);
        _ ->
            true
    end.


%% @doc get members.
-spec(get_members(ClusterId) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id()).
get_members(ClusterId) ->
    get_members(ClusterId, ?VER_CUR).

-spec(get_members(ClusterId, Ver) ->
             {ok, Members} | {error, Cause} when ClusterId::cluster_id(),
                                                 Ver::?VER_CUR | ?VER_PREV,
                                                 Members::[#?MEMBER{}],
                                                 Cause::any()).
get_members(ClusterId, Ver) when Ver == ?VER_CUR;
                                 Ver == ?VER_PREV ->
    leo_konsul_member:find_all(ClusterId, Ver);
get_members(_,_) ->
    {error, invalid_version}.


%% @doc get all version members
-spec(get_all_ver_members(ClusterId) ->
             {ok, {CurMembers, PrevMembers}} |
             {error, Cause} when ClusterId::cluster_id(),
                                 CurMembers::[#?MEMBER{}],
                                 PrevMembers::[#?MEMBER{}],
                                 Cause::any()).
get_all_ver_members(ClusterId) ->
    case get_members(ClusterId, ?VER_CUR) of
        {ok, CurMembers} ->
            case get_members(ClusterId, ?VER_PREV) of
                {ok, PrevMembers} ->
                    {ok, {CurMembers, PrevMembers}};
                {error, not_found} ->
                    {ok, {CurMembers, CurMembers}};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc get a member by node-name.
-spec(get_member_by_node(ClusterId, Node) ->
             {ok, Member} |
             {error, any()} when ClusterId::cluster_id(),
                                 Node::atom(),
                                 Member::#?MEMBER{}).
get_member_by_node(ClusterId, Node) ->
    leo_konsul_member:find_by_node(ClusterId, Node).


%% @doc get # of members.
-spec(get_members_count(ClusterId) ->
             integer() | {error, any()} when ClusterId::cluster_id()).
get_members_count(ClusterId) ->
    leo_cluster_tbl_member:table_size(ClusterId).


%% @doc get members by status
-spec(get_members_by_status(ClusterId, Status) ->
             {ok, Members} |
             {error, any()} when ClusterId::cluster_id(),
                                 Status::atom(),
                                 Members::[#?MEMBER{}]
                                 ).
get_members_by_status(ClusterId, Status) ->
    get_members_by_status(ClusterId, ?VER_CUR, Status).

-spec(get_members_by_status(ClusterId, Ver, Status) ->
             {ok, Members} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR | ?VER_PREV,
                                 Status::atom(),
                                 Members::[#?MEMBER{}]).
get_members_by_status(ClusterId, Ver, Status) ->
    leo_konsul_member:find_by_status(ClusterId, Ver, Status).


%% @doc update members.
-spec(update_member(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
update_member(ClusterId, Member) ->
    case leo_konsul_member:update(ClusterId, Member) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update members.
-spec(update_members(ClusterId, Members) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Members::[#?MEMBER{}]).
update_members(ClusterId, Members) ->
    case leo_konsul_member:bulk_update(ClusterId, Members) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc update a member by node-name.
-spec(update_member_by_node(ClusterId, Node, State) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      State::atom()).
update_member_by_node(ClusterId, Node, State) ->
    leo_konsul_member:update_by_node(ClusterId, Node, State).

-spec(update_member_by_node(ClusterId, Node, Clock, State) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::non_neg_integer(),
                                      State::atom()).
update_member_by_node(ClusterId, Node, Clock, State) ->
    leo_konsul_member:update_by_node(ClusterId, Node, Clock, State).


%% @doc remove a member by node-name.
-spec(delete_member_by_node(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
delete_member_by_node(ClusterId, Node) ->
    leo_konsul_member:delete_by_node(ClusterId, Node).


%% %% @doc Is alive the process?
%% -spec(is_alive(ClusterId) ->
%%              ok when ClusterId::cluster_id()).
%% is_alive(ClusterId) ->
%%     leo_membership_cluster_local:heartbeat(?id_membership_local(ClusterId)).


%% @doc Retrieve table-info by version.
-spec(table_info(Ver) ->
             ring_table_info() when Ver::?VER_CUR | ?VER_PREV).
-ifdef(TEST).
table_info(?VER_CUR)  ->
    ?RING_TBL_CUR;
table_info(?VER_PREV) ->
    ?RING_TBL_PREV.
-else.
table_info(?VER_CUR) ->
    ?RING_TBL_CUR;
table_info(?VER_PREV) ->
    ?RING_TBL_PREV.
-endif.


%% @doc Force sync ring-workers
-spec(force_sync_workers(ClusterId) ->
             ok when ClusterId::cluster_id()).
force_sync_workers(ClusterId) ->
    ok = leo_konsul_ring:force_sync(ClusterId),
    ok.


%% @doc Retrieve local cluster's status
-spec(get_cluster_status(ClusterId) ->
             {ok, #?CLUSTER_STAT{}} | not_found when ClusterId::cluster_id()).
get_cluster_status(ClusterId) ->
    {ok, #?SYSTEM_CONF{cluster_id = ClusterId}} = leo_cluster_tbl_conf:get(ClusterId),
    case get_members(ClusterId) of
        {ok, Members} ->
            Status = judge_cluster_status(Members),
            {ok, {Checksum,_}} = checksum(ClusterId, ?CHECKSUM_MEMBER),
            {ok, #?CLUSTER_STAT{cluster_id = ClusterId,
                                state = Status,
                                checksum = Checksum}};
        _ ->
            not_found
    end.


%% @doc Judge status of local cluster
%% @private
-spec(judge_cluster_status(Members) ->
             node_state() when Members::[#?MEMBER{}]).
judge_cluster_status(Members) ->
    NumOfMembers = length(Members),
    SuspendNode  = length([N || #?MEMBER{state = ?STATE_SUSPEND,
                                         node  = N} <- Members]),
    RunningNode  = length([N || #?MEMBER{state = ?STATE_RUNNING,
                                         node  = N} <- Members]),
    case SuspendNode of
        NumOfMembers ->
            ?STATE_SUSPEND;
        _ ->
            case (RunningNode > 0) of
                true  -> ?STATE_RUNNING;
                false -> ?STATE_STOP
            end
    end.


%% @doc Retrieve checksums of cluster-related tables
-spec(get_cluster_tbl_checksums(ClusterId) ->
             {ok, [tuple()]} when ClusterId::cluster_id()).
get_cluster_tbl_checksums(ClusterId) ->
    Chksum_1 = leo_cluster_tbl_conf:checksum(ClusterId),
    Chksum_2 = leo_mdcr_tbl_cluster_info:checksum(),
    Chksum_3 = leo_mdcr_tbl_cluster_mgr:checksum(),
    Chksum_4 = leo_mdcr_tbl_cluster_member:checksum(ClusterId),
    Chksum_5 = leo_mdcr_tbl_cluster_stat:checksum(ClusterId),
    {ok, [{?CHKSUM_CLUSTER_CONF, Chksum_1},
          {?CHKSUM_CLUSTER_INFO, Chksum_2},
          {?CHKSUM_CLUSTER_MGR, Chksum_3},
          {?CHKSUM_CLUSTER_MEMBER, Chksum_4},
          {?CHKSUM_CLUSTER_STAT, Chksum_5}
         ]}.


%%--------------------------------------------------------------------
%% API-4  FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Retrieve conf of remote clusters
-spec(get_remote_clusters(ClusterId) ->
             {ok, [#?CLUSTER_INFO{}]} |
             {error, any()} when ClusterId::cluster_id()).
get_remote_clusters(ClusterId) ->
    case leo_cluster_tbl_conf:get(ClusterId) of
        {ok, #?SYSTEM_CONF{max_mdc_targets = MaxTargetClusters}} ->
            get_remote_clusters_by_limit(MaxTargetClusters);
        _ ->
            not_found
    end.

-spec(get_remote_clusters_by_limit(NumOfDestClusters) ->
             {ok, [#?CLUSTER_INFO{}]} |
             {error, any()} when NumOfDestClusters::integer()).
get_remote_clusters_by_limit(NumOfDestClusters) ->
    leo_mdcr_tbl_cluster_info:find_by_limit(NumOfDestClusters).


%% @doc Retrieve remote cluster members
-spec(get_remote_members(ClusterId) ->
             {ok, #?CLUSTER_MEMBER{}} |
             {error, any()} when ClusterId::atom()).
get_remote_members(ClusterId) ->
    get_remote_members(ClusterId, ?DEF_NUM_OF_REMOTE_MEMBERS).

-spec(get_remote_members(ClusterId, NumOfMembers) ->
             {ok, #?CLUSTER_MEMBER{}} |
             {error, any()} when ClusterId::atom(),
                                 NumOfMembers::integer()).
get_remote_members(ClusterId, NumOfMembers) ->
    leo_mdcr_tbl_cluster_member:find_by_limit(ClusterId, NumOfMembers).


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Specify ETS's table.
%% @private
-spec(ring_table(method()) ->
             ring_table_info()).
ring_table(default) -> table_info(?VER_CUR);
ring_table(put)     -> table_info(?VER_CUR);
ring_table(get)     -> table_info(?VER_PREV);
ring_table(delete)  -> table_info(?VER_CUR);
ring_table(head)    -> table_info(?VER_PREV).
