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
%%======================================================================
%%--------------------------------------------------------------------
%% CONSTANTS
%%--------------------------------------------------------------------
%% Application name
-define(APP, 'leo_konsul').

%% Process id
-define(id_member(_ClusterId),
        list_to_atom(
          lists:append(["leo_konsul_member_",
                        atom_to_list(_ClusterId)]))).

-define(id_ring(_ClusterId),
        list_to_atom(
          lists:append(["leo_konsul_ring_",
                        atom_to_list(_ClusterId)]))).

-define(id_gossip(_ClusterId),
        list_to_atom(
          lists:append(["leo_konsul_gossip_",
                        atom_to_list(_ClusterId)]))).

-define(id_system_conf(_ClusterId),
        list_to_atom(
          lists:append(["leo_konsul_conf_",
                        atom_to_list(_ClusterId)]))).


%% Error
-define(ERROR_COULD_NOT_GET_RING, "Could not get ring").
-define(ERROR_COULD_NOT_GET_CHECKSUM, "Could not get checksum").
-define(ERROR_COULD_NOT_GET_SOME_INFO, "Could not get some info").
-define(ERROR_COULD_NOT_GET_REDUNDANCIES, "Could not get redundancies").
-define(ERROR_COULD_NOT_GET_MEMBERS, "Could not get members").
-define(ERROR_INVALID_CONF, "Invalid configuration").
-define(ERROR_INVALID_MDCR_CONFIG, "Invalid configurations of mdc-replication").

-define(ERR_TYPE_INCONSISTENT_HASH, inconsistent_hash).
-define(ERR_TYPE_NODE_DOWN, nodedown).


%% Property name
-define(PROP_N, 'n').
-define(PROP_W, 'w').
-define(PROP_R, 'r').
-define(PROP_D, 'd').
-define(PROP_RACK_N, 'num_of_rack_replicas').
-define(PROP_DC_N, 'num_of_dc_replicas').
-define(PROP_MAX_DC_TARGETS, 'max_mdc_targets').
-define(PROP_MDCR_W, 'mdcr_w').
-define(PROP_MDCR_R, 'mdcr_r').
-define(PROP_MDCR_D, 'mdcr_d').
-define(PROP_RING_BIT, 'bit_of_ring').

-type(consistency_item() :: ?PROP_N |
                            ?PROP_W |
                            ?PROP_R |
                            ?PROP_D |
                            ?PROP_RACK_N |
                            ?PROP_DC_N |
                            ?PROP_MAX_DC_TARGETS |
                            ?PROP_MDCR_W |
                            ?PROP_MDCR_R |
                            ?PROP_MDCR_D |
                            ?PROP_RING_BIT).

%% Member tables
-define(MEMBER_TBL_CUR, 'leo_members_cur').
-define(MEMBER_TBL_PREV, 'leo_members_prev').
-type(member_table() :: ?MEMBER_TBL_CUR | ?MEMBER_TBL_PREV).
-type(mnesia_copies() :: disc_copies | ram_copies).

%% Ring related
-define(RING_TBL_CUR, 'leo_ring_cur').
-define(RING_TBL_PREV, 'leo_ring_prv').
-define(NODE_ALIAS_PREFIX, "node_").

-type(table_info() :: ?RING_TBL_CUR |
                      ?RING_TBL_PREV).

-define(ring_ver(_Tbl), case _Tbl of
                            ?RING_TBL_CUR ->
                                ?VER_CUR;
                            ?RING_TBL_PREV ->
                                ?VER_PREV
                        end).

-type(ring_table_info() :: ?RING_TBL_CUR |
                           ?RING_TBL_PREV).

%% Checksum
-define(CHECKSUM_RING, 'ring').
-define(CHECKSUM_MEMBER, 'member').
-define(CHECKSUM_SYS_CONF, 'system_conf').
-type(checksum_type() :: ?CHECKSUM_RING |
                         ?CHECKSUM_MEMBER |
                         ?CHECKSUM_SYS_CONF).

%% Default
-define(MD5, 128).
-define(DEF_OPT_N, 1).
-define(DEF_OPT_R, 1).
-define(DEF_OPT_W, 1).
-define(DEF_OPT_D, 1).
-define(DEF_OPT_BIT_OF_RING, ?MD5).
-define(DEF_NUM_OF_REMOTE_MEMBERS, 5).
-define(DEF_MAX_MDC_TARGETS, 2).
-define(DEF_CLUSTER_ID, 'leofs_c1').
-define(DEF_DC_ID, 'dc_1').

-ifdef(TEST).
-define(DEF_NUMBER_OF_VNODES, 32).
-else.
-define(DEF_NUMBER_OF_VNODES, 168).
-endif.


%% Node State
-define(STATE_ATTACHED, 'attached').
-define(STATE_DETACHED, 'detached').
-define(STATE_SUSPEND, 'suspend').
-define(STATE_RUNNING, 'running').
-define(STATE_STOP, 'stop').
-define(STATE_RESTARTED, 'restarted').
-define(STATE_RESERVED, 'reserved').

-type(node_state() :: ?STATE_ATTACHED |
                      ?STATE_DETACHED |
                      ?STATE_SUSPEND |
                      ?STATE_RUNNING |
                      ?STATE_STOP |
                      ?STATE_RESTARTED |
                      ?STATE_RESERVED).

-define(is_correct_state(_State),
        case _State of
            ?STATE_ATTACHED ->
                true;
            ?STATE_DETACHED ->
                true;
            ?STATE_SUSPEND ->
                true;
            ?STATE_RUNNING ->
                true;
            ?STATE_STOP ->
                true;
            ?STATE_RESTARTED ->
                true;
            ?STATE_RESERVED ->
                true;
            _ ->
                false
        end).


%% Property
-define(PROP_NOTIFY_MF, 'notify_mf').
-define(PROP_SYNC_MF, 'sync_mf').
-define(PROP_RING_HASH, 'ring_hash').

-define(ITEM_MEMBER, 'member').
-define(ITEM_RING, 'ring').
-define(ITEM_CONF, 'conf').
-define(ITEM_NODEDOWN, 'nodedown').
-type(check_item() :: ?ITEM_MEMBER |
                      ?ITEM_RING |
                      ?ITEM_CONF |
                      ?ITEM_NODEDOWN).

-define(PROP_SYNC_NEW_CLUSTER_MOD, 'sync_new_cluster_mod').

%% Version
-define(VER_CUR, 'cur' ).
-define(VER_PREV, 'prev').
-type(version() :: ?VER_CUR | ?VER_PREV).

-define(member_table(_VER),
        case _VER of
            ?VER_CUR ->
                ?MEMBER_TBL_CUR;
            ?VER_PREV ->
                ?MEMBER_TBL_PREV
        end).

-define(ring_table_to_member_table(_Tbl),
        case _Tbl of
            {_, ?RING_TBL_CUR} ->
                ?MEMBER_TBL_CUR;
            {_, ?RING_TBL_PREV} ->
                ?MEMBER_TBL_PREV
        end).

-define(sync_target_to_ver(_Target),
        case _Target of
            ?SYNC_TARGET_RING_CUR ->
                ?VER_CUR;
            ?SYNC_TARGET_RING_PREV ->
                ?VER_PREV
        end).

-define(sync_target_to_table(_Target),
        case _Target of
            ?SYNC_TARGET_RING_CUR ->
                ?RING_TBL_CUR;
            ?SYNC_TARGET_RING_PREV ->
                ?RING_TBL_PREV
        end).

-define(ring_table(_VER),
        case _VER of
            ?VER_CUR ->
                ?RING_TBL_CUR;
            ?VER_PREV ->
                ?RING_TBL_PREV
        end).

-define(table_to_sync_target(_Table),
        case _Table of
            ?RING_TBL_CUR ->
                ?SYNC_TARGET_RING_CUR;
            ?RING_TBL_PREV ->
                ?SYNC_TARGET_RING_PREV
        end).


%% Synchronization
-define(SYNC_TARGET_BOTH, 'both').
-define(SYNC_TARGET_RING_CUR, 'ring_cur').
-define(SYNC_TARGET_RING_PREV, 'ring_prev').
-define(SYNC_TARGET_MEMBER, 'member').
-type(sync_target() :: ?SYNC_TARGET_BOTH |
                       ?SYNC_TARGET_RING_CUR |
                       ?SYNC_TARGET_RING_PREV |
                       ?SYNC_TARGET_MEMBER).

%% Consensus Roles
-define(CNS_ROLE_LEADER, 'L').
-define(CNS_ROLE_FOLLOWER_1,'FL').
-define(CNS_ROLE_FOLLOWER_2, 'FL').
-define(CNS_ROLE_OBSERBER, 'O').
-type(consensus_role() :: ?CNS_ROLE_LEADER |
                          ?CNS_ROLE_FOLLOWER_1 |
                          ?CNS_ROLE_FOLLOWER_2 |
                          ?CNS_ROLE_OBSERBER).

%% ClusterId
-type(cluster_id() :: atom()).
%% VNodeId
-type(vnode_id() :: non_neg_integer()).
-type(vnode_list() :: [{vnode_id, atom(), integer()}]). %% [{VNodeId, Node, Clock}]


%% Server Type
-define(MONITOR_NODE, 'monitor_node').
-define(PERSISTENT_NODE, 'persistent_node').
-define(WORKER_NODE, 'worker_node').
-type(node_type() :: ?MONITOR_NODE |
                     ?PERSISTENT_NODE |
                     ?WORKER_NODE).

-define(ITEM_KEY_MONITORS, monitors).
-define(ITEM_KEY_MQ_DIR, mq_dir).
-define(ITEM_KEY_SYSTEM_CONF, system_conf).
-define(ITEM_KEY_MEMBERSHIP_CALLBACK, membership_callback).
-type(option_item_key() :: ?ITEM_KEY_MONITORS |
                           ?ITEM_KEY_MQ_DIR |
                           ?ITEM_KEY_SYSTEM_CONF |
                           ?ITEM_KEY_MEMBERSHIP_CALLBACK).

%% Mnesia Tables
-define(TBL_SYSTEM_CONF, 'leo_system_conf').
-define(TBL_CLUSTER_STAT, 'leo_cluster_stat').
-define(TBL_CLUSTER_INFO, 'leo_cluster_info').
-define(TBL_CLUSTER_MEMBER, 'leo_cluster_member').
-define(TBL_CLUSTER_MGR, 'leo_cluster_manager').
-undef(ERROR_MNESIA_NOT_START).
-define(ERROR_MNESIA_NOT_START, "Mnesia is not available").

-define(CHKSUM_CLUSTER_CONF, 'cluster_conf').
-define(CHKSUM_CLUSTER_INFO, 'cluster_info').
-define(CHKSUM_CLUSTER_MGR, 'cluster_mgr').
-define(CHKSUM_CLUSTER_MEMBER, 'cluster_member').
-define(CHKSUM_CLUSTER_STAT, 'cluster_stat').

%% Dump File
-define(DEF_LOG_DIR_MEMBERS, "./log/ring/").
-define(DEF_LOG_DIR_RING, "./log/ring/").
-define(DUMP_FILE_MEMBERS_CUR, "members_cur.").
-define(DUMP_FILE_MEMBERS_PREV, "members_prv.").
-define(DUMP_FILE_RING_CUR, "ring_cur.").
-define(DUMP_FILE_RING_PREV, "ring_prv.").

%% Gossip Protocol
-define(PROP_THRESHOLD_ERROR_CNT, 'threshold_error_count').
-define(DEF_THRESHOLD_ERROR_CNT, 3).

-define(PROP_THRESHOLD_ELAPSED_TIME, 'threshold_elapsed_time').
-define(DEF_THRESHOLD_ELAPSED_TIME, timer:seconds(5)).


%%--------------------------------------------------------------------
%% RECORDS-1
%%--------------------------------------------------------------------
%% Configure of Redundancies and Consistency Level
-record(system_conf, {
          version = 0 :: non_neg_integer(),
          n = 1 :: pos_integer(),
          r = 1 :: pos_integer(),
          w = 1 :: pos_integer(),
          d = 1 :: pos_integer(),
          bit_of_ring = 128 :: pos_integer(),
          level_1 = 1 :: pos_integer(),
          level_2 = 0 :: non_neg_integer()
         }).
-record(system_conf_1, {
          version = 0 :: non_neg_integer(),
          cluster_id :: atom()|string(),
          dc_id :: atom()|string(),
          n = 1 :: pos_integer(),
          r = 1 :: pos_integer(),
          w = 1 :: pos_integer(),
          d = 1 :: pos_integer(),
          bit_of_ring = 128 :: pos_integer(),
          num_of_dc_replicas = 1 :: pos_integer(),
          num_of_rack_replicas = 0 :: non_neg_integer()
         }).
-record(system_conf_2, {
          version = 1 :: non_neg_integer(), %% version
          cluster_id :: cluster_id(),       %% cluster-id
          dc_id :: atom(),                  %% dc-id
          n = 1 :: pos_integer(),       %% # of replicas
          r = 1 :: pos_integer(),       %% # of replicas needed for a successful READ operation
          w = 1 :: pos_integer(),       %% # of replicas needed for a successful WRITE operation
          d = 1 :: pos_integer(),       %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128 :: pos_integer(),        %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas  = 1 :: pos_integer(),  %% # of destination of nodes a cluster for MDC-replication
          num_of_rack_replicas = 0 :: non_neg_integer(), %% # of Rack-awareness replicas
          max_mdc_targets = ?DEF_MAX_MDC_TARGETS :: pos_integer() %% max multi-dc replication targets for MDC-replication
         }).
-record(system_conf_3, {
          version = 1 :: non_neg_integer(), %% version
          cluster_id :: atom(),             %% cluster-id
          dc_id :: atom(),                  %% dc-id
          n = 1 :: pos_integer(),           %% # of replicas
          r = 1 :: pos_integer(),           %% # of replicas needed for a successful READ operation
          w = 1 :: pos_integer(),           %% # of replicas needed for a successful WRITE operation
          d = 1 :: pos_integer(),           %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128 :: pos_integer(),        %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas  = 1 :: pos_integer(),  %% # of destination of nodes a cluster for MDC-replication
          mdcr_r = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful READ operation
          mdcr_w = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful WRITE operation
          mdcr_d = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful DELETE operation
          num_of_rack_replicas = 0 :: non_neg_integer(), %% # of Rack-awareness replicas
          max_mdc_targets = ?DEF_MAX_MDC_TARGETS :: pos_integer() %% max multi-dc replication targets for MDC-replication
         }).
-define(SYSTEM_CONF, 'system_conf_3').


%% Configuration of a remote cluster
-record(cluster_info, {
          cluster_id :: atom()|string(), %% cluster-id
          dc_id :: atom()|string(),      %% dc-id
          n = 1 :: pos_integer(),    %% # of replicas
          r = 1 :: pos_integer(),    %% # of replicas needed for a successful READ operation
          w = 1 :: pos_integer(),    %% # of replicas needed for a successful WRITE operation
          d = 1 :: pos_integer(),    %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128 :: pos_integer(),        %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas = 1 :: pos_integer(),   %% # of replicas a DC for MDC-replication
          num_of_rack_replicas = 0 :: non_neg_integer()  %% # of Rack-awareness replicas
         }).
-record(cluster_info_1, {
          cluster_id :: cluster_id(), %% cluster-id
          dc_id :: atom(),            %% dc-id
          n = 1 :: pos_integer(), %% # of replicas
          r = 1 :: pos_integer(), %% # of replicas needed for a successful READ operation
          w = 1 :: pos_integer(), %% # of replicas needed for a successful WRITE operation
          d = 1 :: pos_integer(), %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128 :: pos_integer(),        %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas = 1 :: pos_integer(),   %% # of replicas a DC for MDC-replication
          num_of_rack_replicas = 0 :: non_neg_integer(), %% # of Rack-awareness replicas
          max_mdc_targets = ?DEF_MAX_MDC_TARGETS :: pos_integer() %% max multi-dc replication targets for MDC-replication
         }).
-record(cluster_info_2, {
          cluster_id :: atom(),       %% cluster-id
          dc_id :: atom(),            %% dc-id
          n = 1 :: pos_integer(), %% # of replicas
          r = 1 :: pos_integer(), %% # of replicas needed for a successful READ operation
          w = 1 :: pos_integer(), %% # of replicas needed for a successful WRITE operation
          d = 1 :: pos_integer(), %% # of replicas needed for a successful DELETE operation
          bit_of_ring = 128 :: pos_integer(),        %% # of bits for the hash-ring (fixed 128bit)
          num_of_dc_replicas = 1 :: pos_integer(),   %% # of replicas a DC for MDC-replication
          mdcr_r = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful READ operation
          mdcr_w = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful WRITE operation
          mdcr_d = 1 :: pos_integer(), %% mdc-replication / # of replicas needed for a successful DELETE operation
          num_of_rack_replicas = 0 :: non_neg_integer(), %% # of Rack-awareness replicas
          max_mdc_targets = ?DEF_MAX_MDC_TARGETS :: pos_integer() %% max multi-dc replication targets for MDC-replication
         }).
-define(CLUSTER_INFO, 'cluster_info_2').


%% For Multi-DC Replication
-record(cluster_stat, {
          cluster_id :: atom()|string(),      %% cluster-id
          status = null :: node_state()|null, %% status:[running | stop]
          checksum = 0 :: non_neg_integer(),  %% checksum of members
          updated_at = 0 :: non_neg_integer() %% updated at
         }).

-record(cluster_stat_1, {
          cluster_id :: atom(),               %% cluster-id
          state = null :: node_state()|null,  %% status:[running | stop]
          checksum = 0 :: non_neg_integer(),  %% checksum of members
          updated_at = 0 :: non_neg_integer() %% updated at
         }).
-define(CLUSTER_STAT, 'cluster_stat_1').


%% Cluster Manager
-record(cluster_manager, {
          node :: atom(), %% actual node-name
          cluster_id :: cluster_id()  %% cluster-id
         }).


%% Cluster Members
-record(cluster_member, {
          node :: atom(),                 %% actual node-name
          cluster_id :: atom()|string(),  %% cluster-id
          alias = [] :: string(),         %% node-alias
          ip = "0.0.0.0" :: string(),     %% ip-address
          port = 13075 :: pos_integer(),  %% port-number
          inet = 'ipv4' :: 'ipv4'|'ipv6', %% type of ip
          clock = 0 :: non_neg_integer(), %% joined at
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: pos_integer(), %% # of vnodes
          status = null :: node_state()
         }).
-record(cluster_member_1, {
          node :: atom(),                 %% actual node-name
          cluster_id :: atom(),           %% cluster-id
          alias = [] :: string(),         %% node-alias
          ip = "0.0.0.0" :: string(),     %% ip-address
          port = 13075 :: pos_integer(),  %% port-number
          inet = 'ipv4' :: 'ipv4'|'ipv6', %% type of ip
          clock = 0 :: non_neg_integer(), %% joined at
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: pos_integer(), %% # of vnodes
          state = null :: node_state()
         }).
-define(CLUSTER_MEMBER, 'cluster_member_1').


%% a member of a local cluster
-record(member, {
          node :: atom(),                   %% actual node-name
          alias = [] :: string(),           %% node-alias
          ip = "0.0.0.0" :: string(),       %% ip-address
          port = 13075 :: pos_integer(),    %% port-number
          inet = 'ipv4' :: 'ipv4'|'ipv6',   %% type of ip
          clock = 0 :: pos_integer(),       %% joined at
          state = null:: node_state()|null, %% current-status
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
          grp_level_1 = [] :: string(),      %% Group of level_1 for multi-dc replication
          grp_level_2 = [] :: string()       %% Group of level_2 for rack-awareness replication
         }).
%% since leo_konsul:v2.0.0 / LeoFS:v1.4.0
-record(member_1, {
          id :: {atom(), atom()},           %% {cluster_id, node}
          node :: atom(),                   %% actual node-name
          cluster_id :: atom(),             %% [new] cluster-id
          alias = [] :: string(),           %% node-alias
          ip = "0.0.0.0" :: string(),       %% ip-address
          port = 13075 :: pos_integer(),    %% port-number
          inet = 'ipv4' :: 'ipv4'|'ipv6',   %% type of ip
          clock = 0 :: pos_integer(),       %% joined at
          state = null:: node_state()|null, %% current-status
          num_of_vnodes = ?DEF_NUMBER_OF_VNODES :: integer(), %% # of vnodes
          grp_level_1 = [] :: string(),      %% Group of level_1 for multi-dc replication
          grp_level_2 = [] :: string()       %% Group of level_2 for rack-awareness replication
         }).
-define(MEMBER, 'member_1').


%% Synchronization info
-record(sync_info, {
          target :: ?SYNC_TARGET_RING_CUR|?SYNC_TARGET_RING_PREV,
          org_checksum = 0 :: non_neg_integer(), %% original checksum
          cur_checksum = 0 :: non_neg_integer()  %% current chechsum
         }).


%% Gossip - Issue which is a sub-item of `Issue Bucket`
-record(issue, {
          item :: check_item(),
          version :: version(),
          count = 0 :: non_neg_integer(),
          created_at = 0 :: non_neg_integer(),
          updated_at = 0 :: non_neg_integer()
         }).

%% Gossip - Issue Bucket
-record(issue_bucket, {
          node :: node(),
          issues = [] :: [#issue{}]
         }).

%% Gossip - sharing items
-record(sharing_items, {
          member_hashes = [] :: {non_neg_integer(), non_neg_integer()},
          ring_hashes = [] :: {non_neg_integer(), non_neg_integer()},
          conf_hash = 0 :: non_neg_integer(),
          issue_buckets = [] :: [#issue_bucket{}]
         }).


%%--------------------------------------------------------------------
%% RECORDS-2 - for RING
%%--------------------------------------------------------------------
-record(redundant_node, {
          node :: atom(),                      %% node name
          vnode_id = 0 :: vnode_id(),          %% VNode Id
          available = true :: boolean(),       %% alive/dead
          can_read_repair = true :: boolean(), %% able to execute read-repair in case of 'Get Operation'
          role :: consensus_role()             %% consensus's role
         }).

-record(vnodeid_nodes, {
          id = 0 :: non_neg_integer(),     %% id
          vnode_id_from = 0 :: vnode_id(), %% vnode-id's from
          vnode_id_to = 0 :: vnode_id(),   %% vnode-id's to
          nodes :: [#redundant_node{}]     %% list of nodes
         }).

-record(ring_group, {
          index_from = 0 :: non_neg_integer(),          %% group-index's from
          index_to = 0 :: non_neg_integer(),            %% group-index's to
          vnodeid_nodes_list = [] :: [#vnodeid_nodes{}] %% list of vnodeid(s)
         }).

-record(ring_info, {
          checksum = -1 :: integer(),       %% Ring's checksum
          first_vnode_id = 0 :: vnode_id(), %% start vnode-id
          last_vnode_id = 0 :: vnode_id(),  %% end vnode-id
          members = [] :: [#?MEMBER{}],     %% cluster-members
          vnode_list = [] :: [{non_neg_integer(), atom(), non_neg_integer()}], %% vnode-id-list
          routing_table = [] :: leo_gb_trees:tree()  %% routing-table
         }).

-record(node_state, {
          node :: atom(),  %% actual node-name
          state :: atom(), %% current-status
          ring_hash_new = "-1" :: string(), %% current ring-hash
          ring_hash_old = "-1" :: string(), %% prev ring-hash
          when_is = 0 :: non_neg_integer(), %% joined at
          error = 0 :: non_neg_integer()    %% # of errors
         }).

-record(redundancies, {
          id = -1 :: integer(),              %% ring's address
          vnode_id_from = -1 :: vnode_id(),  %% start of vnode_id
          vnode_id_to = -1 :: vnode_id(),    %% end   of vnode_id (ex. vnode_id)
          temp_nodes = [] :: [atom()],       %% tempolary objects of redundant-nodes
          temp_level_2 = [] :: [string()],   %% tempolary list of level-2's node
          nodes = [] :: [#redundant_node{}], %% objects of redundant-nodes
          n = 0 :: non_neg_integer(),        %% # of replicas
          r = 0 :: non_neg_integer(),        %% # of successes of READ
          w = 0 :: non_neg_integer(),        %% # of successes of WRITE
          d = 0 :: non_neg_integer(),        %% # of successes of DELETE
          level_1 = 0 :: non_neg_integer(),  %% # of dc-awareness's replicas
          level_2 = 0 :: non_neg_integer(),  %% # of rack-awareness's replicas
          ring_hash = -1 :: integer()        %% ring-hash when writing an object
         }).

-record(ring, {
          vnode_id = -1 :: vnode_id(), %% vnode-id
          node :: atom() %% node
         }).
-record(ring_0_16_8, {
          vnode_id = -1 :: vnode_id(), %% vnode-id
          node :: atom(), %% node
          clock = 0 :: non_neg_integer() %% clock
         }).
-record(ring_2, {
          id :: {cluster_id(), non_neg_integer()}, %% binary
          cluster_id :: cluster_id(),    %% cluster-id
          vnode_id = -1 :: vnode_id(),   %% vnode-id
          node :: atom(),                %% node
          clock = 0 :: non_neg_integer() %% clock
         }).
-define(RING, 'ring_2').


-record(rebalance, {
          members_cur = [] :: [#?MEMBER{}],  %% current members
          members_prev = [] :: [#?MEMBER{}], %% previous members
          tbl_cur :: {atom(),atom()},  %% current table
          tbl_prev :: {atom(),atom()}  %% previous table
         }).

-record(diff_redundancies, {
          vnode_id :: vnode_id(),
          senders :: [atom()],
          receivers :: [atom()]
         }).


%%--------------------------------------------------------------------
%% RECORDS-3 - for Multi Cluster
%%--------------------------------------------------------------------
-record(mdc_replication_info, {
          cluster_id :: atom(),                         %% cluster-id
          num_of_replicas = 0  :: non_neg_integer(),    %% num of replicas
          cluster_members = [] :: [#?CLUSTER_MEMBER{}], %% cluster members
          metadata :: any()                             %% metadata
         }).

-ifdef(TEST).
-define(rnd_nodes_from_ring(_),
        begin
            {ok,_Host} = inet:gethostname(),
            [#redundant_node{node = list_to_atom("sync_test_me@" ++ _Host),
                             available = true},
             #redundant_node{node = list_to_atom("sync_test_node_0@" ++ _Host),
                             available = false},
             #redundant_node{node = list_to_atom("sync_test_node_1@" ++ _Host),
                             available = true}]
        end).
-else.
-define(rnd_nodes_from_ring(_ClusterId),
        begin
            {ok,_Options} = leo_konsul_api:get_options(_ClusterId),
            _BitOfRing = leo_misc:get_value('bit_of_ring',_Options),
            _AddrId = random:uniform(leo_math:power(2,_BitOfRing)),

            case leo_konsul_api:get_redundancies_by_addr_id(
                   _ClusterId, _AddrId) of
                {ok, #redundancies{nodes = _Redundancies}} ->
                    _Redundancies;
                _ ->
                    []
            end
        end).
-endif.


%% @doc Retrieve method and method of sync-fun
-define(env_sync_mod_and_method(),
        case application:get_env(?APP, ?PROP_SYNC_MF) of
            {ok, [_Mod,_Method]} = Ret ->
                Ret;
            undefined ->
                {ok, [leo_manager_api, synchronize]}
        end).

%% @doc Retrieve method and method of notify-fun
-define(env_notify_mod_and_method(),
        case application:get_env(?APP, ?PROP_NOTIFY_MF) of
            {ok, [_Mod,_Method]} = Ret ->
                Ret;
            undefined ->
                {ok, [leo_manager_api, notify]}
        end).

%% @doc Retrieve module-name of synchronization of objects with a remote-cluster
-define(env_sync_new_cluster_mod(),
        case application:get_env(?APP, ?PROP_SYNC_NEW_CLUSTER_MOD) of
            {ok,_Mod} ->
                _Mod;
            undefined = Ret ->
                Ret
        end).

%% @doc Retrieve the threshole error count of a notification to the leader-node
-define(env_threshold_error_count(),
        case application:get_env(?APP, ?PROP_THRESHOLD_ERROR_CNT) of
            {ok,_Cnt} ->
                _Cnt;
            undefined ->
                ?DEF_THRESHOLD_ERROR_CNT
        end).

%% @doc Retrieve the threshole error count of a notification to the leader-node
-define(env_threshold_elapsed_time(),
        case application:get_env(?APP, ?PROP_THRESHOLD_ELAPSED_TIME) of
            {ok,_ElapsedTime} ->
                _ElapsedTime;
            undefined ->
                ?DEF_THRESHOLD_ELAPSED_TIME
        end).

%% @doc Retrieve the log-directory for the ring
-define(log_dir(),
        begin
            case application:get_env(leo_konsul, log_dir_ring) of
                undefined -> ?DEF_LOG_DIR_RING;
                {ok, _Dir} ->
                    case (string:len(_Dir) == string:rstr(_Dir, "/")) of
                        true  -> _Dir;
                        false -> _Dir ++ "/"
                    end
            end
        end).


-define(alias(_Member),
        case is_record(_Member, ?MEMBER) of
            true when _Member#?MEMBER.alias == [] ->
                #?MEMBER{cluster_id = _ClusterId,
                         node = _Node,
                         grp_level_2 = _RackId} = _Member,

                _NodeStr = atom_to_list(_Node),
                _IP = case (string:chr(_NodeStr, $@) > 0) of
                          true ->
                              lists:nth(2,string:tokens(_NodeStr, "@"));
                          false ->
                              []
                      end,

                {ok,_Alias} =
                    leo_konsul_api:get_alias(_ClusterId,_Node,_RackId),
                {ok, _Member#?MEMBER{alias = _Alias,
                                     ip = _IP}};
            true ->
                {ok, _Member};
            false ->
                {error, invalid_record}
        end).


%% @doc Retrieve current time
-define(timestamp(), leo_math:floor(leo_date:clock() / 1000)).


%%--------------------------------------------------------------------
%% For TESTS
%%--------------------------------------------------------------------
-define(check_redundancies(_RedL),
        begin
            lists:foreach(
              fun(#redundant_node{node = _NA}) ->
                      true = lists:foldl(fun(#redundant_node{node = _NB},_SoFar) ->
                                                 case (_NA == _NB) of
                                                     true ->
                                                         _SoFar + 1;
                                                     false ->
                                                         _SoFar
                                                 end
                                         end, 0,_RedL) == 1
              end, _RedL)
        end).
