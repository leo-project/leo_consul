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
-module(leo_konsul_ring_tests).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_ID, 'leofs_c1').
-define(HOSTNAME, "127.0.0.1").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

suite_test_() ->
    {setup,
     fun () ->
             setup()
     end,
     fun (Pid) ->
             teardown(Pid)
     end,
     [{"test all functions",
       {timeout, 300, fun suite/0}}
     ]}.

setup() ->
    %% Laundh network
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),

    RN_0 = list_to_atom("node_0@" ++ Hostname),
    net_kernel:start([RN_0, shortnames]),
    {ok, RN_1} = slave:start_link(list_to_atom(Hostname), 'node_1'),
    {ok, RN_2} = slave:start_link(list_to_atom(Hostname), 'node_2'),
    {ok, RN_3} = slave:start_link(list_to_atom(Hostname), 'node_3'),
    {ok, RN_4} = slave:start_link(list_to_atom(Hostname), 'node_4'),
    {ok, RN_5} = slave:start_link(list_to_atom(Hostname), 'node_5'),
    {ok, RN_6} = slave:start_link(list_to_atom(Hostname), 'node_6'),
    {ok, RN_7} = slave:start_link(list_to_atom(Hostname), 'node_7'),
    RN_All = [RN_1, RN_2, RN_3,
              RN_4, RN_5, RN_6, RN_7],

    Now = fun() ->
                  {H,S,M} = os:timestamp(),
                  1000000000000 * H + (S * 1000000 + M)
          end,
    [begin
         true = rpc:call(RN, code, add_path, ["../deps/meck/ebin"]),
         ok = rpc:call(RN, meck, new,    [leo_konsul_gossip, [no_link, non_strict]]),
         ok = rpc:call(RN, meck, expect,
                       [leo_konsul_gossip, send,
                        fun(_C,_IL) ->
                                MemberHash = erlang:crc32(crypto:strong_rand_bytes(64)),
                                RingHash = erlang:crc32(crypto:strong_rand_bytes(64)),
                                ConfHash = erlang:crc32(crypto:strong_rand_bytes(64)),

                                {ok, #sharing_items{
                                        member_hashes = {MemberHash,MemberHash},
                                        ring_hashes = {RingHash, RingHash},
                                        conf_hash = ConfHash,
                                        issue_buckets =
                                            [#issue_bucket{
                                                node = RN_3,
                                                issues = [#issue{item = ?ITEM_MEMBER,
                                                                 version = ?VER_CUR,
                                                                 count = 1,
                                                                 created_at = Now(),
                                                                 updated_at = Now()}]
                                               },
                                             #issue_bucket{
                                                node = RN_7,
                                                issues = [#issue{item = ?ITEM_RING,
                                                                 version = ?VER_CUR,
                                                                 count = 1,
                                                                 created_at = Now(),
                                                                 updated_at = Now()}]
                                               }
                                            ]
                                       }}
                        end
                       ])
     end
     || RN  <- RN_All],

    %% Laundh apps
    application:start(crypto),
    application:start(mnesia),
    application:start(leo_konsul),

    %% Prepare the tests
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),

    CallbackFun = fun()-> ok end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3}, {w, 2}, {r ,1}, {d, 2}]}],
    ok = leo_konsul_api:start(?CLUSTER_ID, Options),
    timer:sleep(timer:seconds(3)),

    %% Setup Nodes
    Node_0 = list_to_atom("node_0@" ++ Hostname),
    Node_1 = list_to_atom("node_1@" ++ Hostname),
    Node_2 = list_to_atom("node_2@" ++ Hostname),
    Node_3 = list_to_atom("node_3@" ++ Hostname),
    Node_4 = list_to_atom("node_4@" ++ Hostname),
    Node_5 = list_to_atom("node_5@" ++ Hostname),
    Node_6 = list_to_atom("node_6@" ++ Hostname),
    Node_7 = list_to_atom("node_7@" ++ Hostname),

    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_0},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_0,
                                   alias= "node_0",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_1},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_1,
                                   alias= "node_1",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_2},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_2,
                                   alias= "node_2",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_3},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_3,
                                   alias= "node_3",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_4},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_4,
                                   alias= "node_4",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_5},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_5,
                                   alias= "node_5",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_6},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_6,
                                   alias= "node_6",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_7},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_7,
                                   alias= "node_7",
                                   grp_level_2 = "r1",
                                   state = ?STATE_ATTACHED}),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),

    {ok, MemberL, ChecksumL} = leo_konsul_api:create(?CLUSTER_ID, ?VER_PREV),
    ?assertEqual(8, length(MemberL)),
    ?assertEqual(2, length(ChecksumL)),

    {CurRingHash, PrevRingHash} = leo_misc:get_value('ring', ChecksumL),
    {CurMemberHash, PrevMemberHash} = leo_misc:get_value('member', ChecksumL),
    ?assertEqual(true, CurRingHash == PrevRingHash),
    ?assertEqual(true, CurMemberHash == PrevMemberHash),

    ?debugVal("::: Start to test Gossip Protocol :::"),
    timer:sleep(timer:seconds(10)),
    ok.

teardown(_) ->
    application:stop(leo_konsul),
    application:stop(mnesia),
    application:stop(crypto),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    ok.

suite() ->
    ?debugVal("=== START - Get Redundancies ==="),
    {ok, #redundancies{vnode_id_to = VNodeIdTo_F,
                       nodes = NF}} = leo_konsul_ring:first(
                                        ?CLUSTER_ID, ?RING_TBL_CUR),
    {ok, #redundancies{vnode_id_to = VNodeIdTo_F,
                       nodes = NF}} = leo_konsul_ring:first(
                                        ?CLUSTER_ID, ?RING_TBL_PREV),
    {ok, #redundancies{vnode_id_to = VNodeIdTo_L,
                       nodes = NL}} = leo_konsul_ring:last(
                                        ?CLUSTER_ID, ?RING_TBL_CUR),
    {ok, #redundancies{vnode_id_to = VNodeIdTo_L,
                       nodes = NL}} = leo_konsul_ring:last(
                                        ?CLUSTER_ID, ?RING_TBL_PREV),
    AddrIdList = lists:map(fun(Num) when Num == 0 ->
                                   0;
                              (Num) ->
                                   leo_math:power(2, Num)
                           end, lists:seq(0, 128)),
    [begin
         {ok, #redundancies{nodes = N}} = leo_konsul_ring:lookup(
                                            ?CLUSTER_ID, ?RING_TBL_CUR, A),
         {ok, #redundancies{nodes = N}} = leo_konsul_ring:lookup(
                                            ?CLUSTER_ID, ?RING_TBL_PREV, A),
         ?assertEqual(3, length(N)),
         lists:foreach(fun(#redundant_node{node = NA}) ->
                               true = lists:foldl(fun(#redundant_node{node = NB}, SoFar) ->
                                                          case (NA == NB) of
                                                              true ->
                                                                  SoFar + 1;
                                                              false ->
                                                                  SoFar
                                                          end
                                                  end, 0, N) == 1
                       end, N)
     end|| A <- AddrIdList],

    {ok, #redundancies{vnode_id_to = VNodeIdTo_M}} =
        leo_konsul_ring:lookup(
          ?CLUSTER_ID, ?RING_TBL_CUR, leo_math:power(2, 128)),

    case (VNodeIdTo_M > VNodeIdTo_L) of
        true ->
            ?assertEqual(true, VNodeIdTo_F == VNodeIdTo_M);
        false ->
            void
    end,

    St = leo_date:clock(),
    ok = check_redundancies(100000, 3, false),
    End = leo_date:clock(),

    ?debugVal((End - St) / 1000),

    St_1 = leo_date:clock(),
    ok = check_redundancies(100000, 3, true),
    End_1 = leo_date:clock(),
    ?debugVal((End_1 - St_1) / 1000),

    St_2 = leo_date:clock(),
    ok = check_redundancies(100000, 8, true),
    End_2 = leo_date:clock(),
    ?debugVal((End_2 - St_2) / 1000),

    ?debugVal("=== END - Get Redundancies ==="),

    ?debugVal("=== START - Difference Redundancies for Rebalance ==="),
    Node_11 = list_to_atom("node_11@" ++ ?HOSTNAME),
    leo_konsul_api:join(?CLUSTER_ID,
                          #?MEMBER{id = {?CLUSTER_ID, Node_11},
                                   cluster_id = ?CLUSTER_ID,
                                   node = Node_11,
                                   alias= "node_11",
                                   state = ?STATE_ATTACHED}),
    {ok, MemberL_1, ChecksumL_1} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),
    ?assertEqual(9, length(MemberL_1)),
    ?assertEqual(2, length(ChecksumL_1)),

    {CurRingHash, PrevRingHash} = leo_misc:get_value('ring', ChecksumL_1),
    {CurMemberHash, PrevMemberHash} = leo_misc:get_value('member', ChecksumL_1),
    ?assertEqual(true, CurRingHash /= PrevRingHash),
    ?assertEqual(true, CurMemberHash /= PrevMemberHash),

    [ begin
          {ok, #redundancies{nodes = NC}} = leo_konsul_ring:lookup(
                                              ?CLUSTER_ID, ?RING_TBL_CUR, A),
          {ok, #redundancies{nodes = NP}} = leo_konsul_ring:lookup(
                                              ?CLUSTER_ID, ?RING_TBL_PREV, A),
          ?assertEqual(3, length(NC)),
          ?assertEqual(3, length(NP))
      end|| A <- AddrIdList],

    St_3 = leo_date:clock(),
    {ok, DiffRedundancies} = leo_konsul_ring:diff_redundancies(?CLUSTER_ID),
    End_3 = leo_date:clock(),
    ?debugVal((End_3 - St_3) / 1000),

    ?assertEqual(true, length(DiffRedundancies) > 0),
    [begin
         ?assertEqual(true, length(SenderL) > 0),
         ?assertEqual(true, length(ReceiverL) > 0),
         lists:foreach(fun(SL) ->
                               false = lists:any(fun(RL) ->
                                                         SL == RL
                                                 end, ReceiverL)
                       end, SenderL),
         ok
     end|| #diff_redundancies{senders = SenderL,
                              receivers = ReceiverL} <- DiffRedundancies],

    St_4 = leo_date:clock(),
    ok = check_redundancies(100000, 9, true),
    End_4 = leo_date:clock(),
    ?debugVal((End_4 - St_4) / 1000),
    ?debugVal("=== END - Difference Redundancies for Rebalance ==="),
    ok.


%% @private
check_redundancies(0,_,_) ->
    ok;
check_redundancies(Index, NumOfReplicas, IsStrict) ->
    %% Test Total number of replicas
    AddrId = leo_konsul_chash:vnode_id(
               128, crypto:strong_rand_bytes(64)),
    {ok, #redundancies{nodes = NodeL}} =
        case (NumOfReplicas == 3) of
            true ->
                leo_konsul_ring:lookup(
                  ?CLUSTER_ID, ?RING_TBL_CUR, AddrId);
            false ->
                leo_konsul_ring:lookup(
                  ?CLUSTER_ID, ?RING_TBL_CUR, AddrId, NumOfReplicas)
        end,
    ?assertEqual(NumOfReplicas, length(NodeL)),

    %% Test duplicated nodes in redundancies
    case IsStrict of
        true ->
            ?check_redundancies(NodeL);
        false ->
            void
    end,
    check_redundancies(Index - 1, NumOfReplicas, IsStrict).

-endif.
