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
-module(leo_konsul_api_tests).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CLUSTER_ID, 'leofs_c1').
-define(NUM_OF_RECURSIVE_CALLS, 1000).


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

check_redundancies_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### CHECK-REDUNDANCIES ###"),
             {_} = setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             timer:sleep(timer:seconds(3)),
             ok
     end,
     [
      {"check redundancies",
       {timeout, 5000, fun check_redundancies/0}}
     ]}.

check_redundancies() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    ok = inspect_1(Hostname, 500),
    ok.


%% @doc Test SUITE
%%
suite_1_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-1 - Basic ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun basic/0}}
     ]}.
suite_2_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-2 - Join1 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun join_1/0}}
     ]}.
suite_3_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-3 - Join2 ###"),
             setup(),
             ok end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun join_2/0}}
     ]}.
suite_4_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-4 - Leave ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun leave/0}}
     ]}.
suite_5_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-5 - Takeover ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok
     end,
     [
      {"",{timeout, 5000, fun takeover/0}}
     ]}.
suite_6_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-6 - Rackawareness1 ###"),
             setup(),
             ok end,
     fun (_) ->
             teardown([]),
             ok end,
     [
      {"",{timeout, 5000, fun rack_aware_1/0}}
     ]}.
suite_7_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### TEST-7 - Rackawareness2 ###"),
             setup(),
             ok
     end,
     fun (_) ->
             teardown([]),
             ok end,
     [
      {"",{timeout, 5000, fun rack_aware_2/0}}
     ]}.


setup() ->
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    Me = list_to_atom("test_0@" ++ Hostname),
    net_kernel:start([Me, shortnames]),

    timer:sleep(timer:seconds(1)),
    application:start(crypto),
    application:start(mnesia),
    ok = application:start(leo_konsul),

    %% Create member tables
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    {Hostname}.


teardown(_) ->
    net_kernel:stop(),
    application:stop(leo_konsul),
    application:stop(mnesia),
    application:stop(crypto),

    os:cmd("rm -rf queue"),
    os:cmd("rm ring_*"),
    timer:sleep(timer:seconds(3)),
    ok.


%% @doc TEST the basic function
%% @private
basic() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    inspect_1(Hostname, 1000),
    ok.


%% @doc Test Join Node(s)-1
%% @private
join_1() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_PREV),
    {ok, {Chksum_0, Chksum_1}} = leo_konsul_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    ?assertEqual(true, (Chksum_0 > -1)),
    ?assertEqual(true, (Chksum_1 > -1)),

    %% rebalance.join
    JoinedNode = list_to_atom("node_8@" ++ Hostname),
    JoinedMember = #?MEMBER{id = {?CLUSTER_ID, JoinedNode},
                            cluster_id = ?CLUSTER_ID,
                            node = JoinedNode,
                            alias= "node_8x",
                            state = ?STATE_ATTACHED},
    ok = leo_konsul_api:join(?CLUSTER_ID, JoinedMember),
    leo_konsul_api:dump(?CLUSTER_ID, ?CHECKSUM_RING),
    leo_konsul_api:dump(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    %% execute
    {ok, Res1} = leo_konsul_api:rebalance(?CLUSTER_ID),
    ?assertEqual(true, Res1 =/= []),

    %% investigation
    lists:foreach(fun(#diff_redundancies{vnode_id = VNodeId,
                                         senders = Src,
                                         receivers = Dest}) ->
                          ?assertEqual(true, VNodeId > 0),
                          ?assertEqual(true, Src  /= Dest),
                          [ ?assertEqual(false, lists:member(_N1, [JoinedNode])) || _N1 <- Src ],
                          [ ?assertEqual(true,  lists:member(_N2, [JoinedNode])) || _N2 <- Dest ],
                          ok
                  end, Res1),

    {ok, MembersCur} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_PREV, ?CLUSTER_ID),
    ?assertEqual(9, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),
    ?assertNotEqual([], MembersPrev),

    {ok, {RingHashCur, RingHashPrev  }} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_MEMBER),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(RingHashCur, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    join_1_1(1000),
    ok.

%% @private
join_1_1(0) ->
    ok;
join_1_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_konsul_api:get_redundancies_by_key(
                 ?CLUSTER_ID, put, Key),
    {ok, R2} = leo_konsul_api:get_redundancies_by_key(
                 ?CLUSTER_ID, get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            ok;
        false ->
            ?assertEqual(true, (length(R2#redundancies.nodes) ==
                                    length(R1#redundancies.nodes))),
            ok = ?check_redundancies(R1#redundancies.nodes),
            ok = ?check_redundancies(R2#redundancies.nodes),
            ok
    end,
    join_1_1(Index - 1).


%% @doc Test Join Node(s)-2
%% @private
join_2() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_PREV),

    %% rebalance.join
    JoinedNodes = [list_to_atom("node_8@" ++ Hostname),
                   list_to_atom("node_9@" ++ Hostname),
                   list_to_atom("node_10@" ++ Hostname)],
    JoinedMembers =
        [ #?MEMBER{id = {?CLUSTER_ID, _N},
                   cluster_id = ?CLUSTER_ID,
                   node = _N,
                   alias= string:substr(
                            leo_hex:binary_to_hex(
                              crypto:hash(md5, atom_to_list(_N))),1,8),
                   state = ?STATE_ATTACHED}
          || _N <- JoinedNodes ],
    lists:foreach(fun(_M) ->
                          ok = leo_konsul_api:join(?CLUSTER_ID,_M)
                  end, JoinedMembers),
    leo_konsul_api:dump(?CLUSTER_ID, ?CHECKSUM_RING),
    leo_konsul_api:dump(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    %% execute
    {ok, Res1} = leo_konsul_api:rebalance(?CLUSTER_ID),
    ?assertEqual(true, Res1 =/= []),

    lists:foreach(fun(#diff_redundancies{vnode_id = VNodeId,
                                         senders = Src,
                                         receivers = Dest}) ->
                          ?assertEqual(true, VNodeId > 0),
                          ?assertEqual(true, Src  /= Dest),
                          [ ?assertEqual(false, lists:member(_N1, JoinedNodes)) || _N1 <- Src ],
                          [ ?assertEqual(true,  lists:member(_N2, JoinedNodes)) || _N2 <- Dest ],
                          ok
                  end, Res1),

    %% check
    {ok, MembersCur } = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_PREV, ?CLUSTER_ID),
    ?assertEqual(11, length(MembersCur)),
    ?assertEqual(8,  length(MembersPrev)),
    ?assertNotEqual([], MembersPrev),

    {ok, {RingHashCur, RingHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_MEMBER),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(RingHashCur, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    %% retrieve redundancies
    join_2_1(1000),
    ok.

%% @private
join_2_1(0) ->
    ok;
join_2_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_konsul_api:get_redundancies_by_key(?CLUSTER_ID, put, Key),
    {ok, R2} = leo_konsul_api:get_redundancies_by_key(?CLUSTER_ID, get, Key),
    ok = ?check_redundancies(R1#redundancies.nodes),
    ok = ?check_redundancies(R2#redundancies.nodes),

    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            void;
        false ->
            Difference = (length(R2#redundancies.nodes) -
                              length(R1#redundancies.nodes)),
            ?assertEqual(0, Difference),
            ok = ?check_redundancies(R1#redundancies.nodes),
            ok = ?check_redundancies(R2#redundancies.nodes),
            ok
    end,
    join_2_1(Index - 1).


%% @doc Test Leave Node(s)
%% @private
leave() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_PREV),

    %% 1. rebalance.leave
    LeavedNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_konsul_api:leave(?CLUSTER_ID, LeavedNode),

    %% 2. exec rebalance
    {ok, Res} = leo_konsul_api:rebalance(?CLUSTER_ID),

    %% investigation
    lists:foreach(fun(#diff_redundancies{vnode_id = VNodeId,
                                         senders = Src,
                                         receivers = Dest}) ->
                          ?assertEqual(true, VNodeId > 0),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= [LeavedNode]),
                          ?assertEqual(true, Dest /= [LeavedNode]),
                          ok
                  end, Res),

    {ok, MembersCur } = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} = leo_cluster_tbl_member:find_by_cluster_id(
                          ?MEMBER_TBL_PREV, ?CLUSTER_ID),
    ?assertNotEqual(MembersCur, MembersPrev),
    ?assertEqual(8, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),

    {ok, {RingHashCur, RingHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_MEMBER),

    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),

    leave_1_1(1000),
    ok.

leave_1_1(0) ->
    ok;
leave_1_1(Index) ->
    Key = list_to_binary("key_" ++ integer_to_list(Index)),
    {ok, R1} = leo_konsul_api:get_redundancies_by_key(
                 ?CLUSTER_ID, put, Key),
    {ok, R2} = leo_konsul_api:get_redundancies_by_key(
                 ?CLUSTER_ID,get, Key),
    case (R1#redundancies.nodes == R2#redundancies.nodes) of
        true ->
            ok;
        false ->
            ok = ?check_redundancies(R1#redundancies.nodes),
            ok = ?check_redundancies(R2#redundancies.nodes),
            ok
    end,
    leave_1_1(Index - 1).


%% @doc Test Takeover
%% @private
takeover() ->
    {ok, Hostname} = inet:gethostname(),
    ok = prepare(Hostname),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_CUR),
    {ok,_,_} = leo_konsul_api:create(?CLUSTER_ID, ?VER_PREV),
    {ok, {Chksum_0, Chksum_1}} = leo_konsul_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    ?assertEqual(true, (Chksum_0 > -1)),
    ?assertEqual(true, (Chksum_1 > -1)),

    %% join
    JoinedNode = list_to_atom("node_8@" ++ Hostname),
    JoinedMember = #?MEMBER{id = {?CLUSTER_ID, JoinedNode},
                            cluster_id = ?CLUSTER_ID,
                            node = JoinedNode,
                            alias= "node_8x",
                            state = ?STATE_ATTACHED},
    ok = leo_konsul_api:join(?CLUSTER_ID, JoinedMember),

    %% leave
    LeavedNode = list_to_atom("node_0@" ++ Hostname),
    ok = leo_konsul_api:leave(?CLUSTER_ID, LeavedNode),

    %% exec rebalance
    {ok, Res} = leo_konsul_api:rebalance(?CLUSTER_ID),

    %% investigation
    lists:foreach(fun(#diff_redundancies{vnode_id = VNodeId,
                                         senders = Src,
                                         receivers = Dest}) ->
                          ?assertEqual(true, VNodeId > 0),
                          ?assertEqual(true, Src  /= Dest),
                          ?assertEqual(true, Src  /= [LeavedNode]),
                          ?assertEqual(true, Src  /= [JoinedNode]),
                          ?assertEqual(true, Dest == [JoinedNode]),
                          ok
                  end, Res),

    {ok, MembersCur} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID),
    {ok, MembersPrev} =
        leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_PREV, ?CLUSTER_ID),
    ?assertNotEqual([], MembersPrev),
    ?assertEqual(9, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),

    {ok, {RingHashCur, RingHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {MemberHashCur, MemberHashPrev}} =
        leo_konsul_api:checksum(?CLUSTER_ID, ?CHECKSUM_MEMBER),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, RingHashPrev),
    ?assertNotEqual(RingHashCur, RingHashPrev),
    ?assertNotEqual(-1, MemberHashCur),
    ?assertNotEqual(-1, MemberHashPrev),
    ok.


%% @doc Test Rackaware in case of # of rackawareness is ONE
%% @private
rack_aware_1() ->
    %% preparation
    CallbackFun = fun()->
                          ok
                  end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3},
                              {w, 2},
                              {r ,1},
                              {d, 2},
                              {num_of_rack_replicas, 1}
                             ]}],
    ok = leo_konsul_api:start(?CLUSTER_ID, Options),
    Hostname = "0.0.0.0",
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
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_5},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_5,
                                 alias= "node_5",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_6},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_6,
                                 alias= "node_6",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_7},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_7,
                                 alias= "node_7",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),

    %% Create routing-table > confirmations
    {ok,_, Chksums} = leo_konsul_api:create(?CLUSTER_ID),
    ok = leo_konsul_api:dump(?CLUSTER_ID, member),
    ok = leo_konsul_api:dump(?CLUSTER_ID, ring),

    %% Inverstigation
    {RingHashCur,_} = leo_misc:get_value('ring', Chksums),
    {MemberHashCur,_} = leo_misc:get_value('member', Chksums),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, MemberHashCur),

    {ok, Members} = leo_cluster_tbl_member:get_available_members_by_cluster_id(?CLUSTER_ID),
    MemberDict = lists:foldl(fun(#?MEMBER{node = Node} = M, D) ->
                                     dict:append(Node, M, D)
                             end, dict:new(), Members),

    ok = check_redundancies(10000, MemberDict, 2),
    ok.


%% @doc Test Rackaware in case of # of rackawareness is PLURAL
%% @private
rack_aware_2() ->
    %% preparation
    CallbackFun = fun()->
                          ok
                  end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3},
                              {w, 2},
                              {r ,1},
                              {d, 2},
                              {num_of_rack_replicas, 2}
                             ]}],
    ok = leo_konsul_api:start(?CLUSTER_ID, Options),
    Hostname = "0.0.0.0",
    Node_0 = list_to_atom("node_0@" ++ Hostname),
    Node_1 = list_to_atom("node_1@" ++ Hostname),
    Node_2 = list_to_atom("node_2@" ++ Hostname),
    Node_3 = list_to_atom("node_3@" ++ Hostname),
    Node_4 = list_to_atom("node_4@" ++ Hostname),
    Node_5 = list_to_atom("node_5@" ++ Hostname),
    Node_6 = list_to_atom("node_6@" ++ Hostname),
    Node_7 = list_to_atom("node_7@" ++ Hostname),
    Node_8 = list_to_atom("node_8@" ++ Hostname),
    Node_9 = list_to_atom("node_9@" ++ Hostname),
    Node_10 = list_to_atom("node_10@" ++ Hostname),
    Node_11 = list_to_atom("node_11@" ++ Hostname),

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
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_5},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_5,
                                 alias= "node_5",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_6},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_6,
                                 alias= "node_6",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_7},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_7,
                                 alias= "node_7",
                                 grp_level_2 = "r2",
                                 state = ?STATE_ATTACHED}),

    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_8},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_8,
                                 alias= "node_8",
                                 grp_level_2 = "r3",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_9},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_9,
                                 alias= "node_9",
                                 grp_level_2 = "r3",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_10},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_10,
                                 alias= "node_10",
                                 grp_level_2 = "r3",
                                 state = ?STATE_ATTACHED}),
    leo_konsul_api:join(?CLUSTER_ID,
                        #?MEMBER{id = {?CLUSTER_ID, Node_11},
                                 cluster_id = ?CLUSTER_ID,
                                 node = Node_11,
                                 alias= "node_11",
                                 grp_level_2 = "r3",
                                 state = ?STATE_ATTACHED}),

    %% Create routing-table > confirmations
    {ok,_, Chksums} = leo_konsul_api:create(?CLUSTER_ID),

    {RingHashCur,_} = leo_misc:get_value('ring', Chksums),
    {MemberHashCur,_} = leo_misc:get_value('member', Chksums),
    ?assertNotEqual(-1, RingHashCur),
    ?assertNotEqual(-1, MemberHashCur),

    {ok, Members} = leo_cluster_tbl_member:get_available_members_by_cluster_id(?CLUSTER_ID),
    MemberDict = lists:foldl(fun(#?MEMBER{node = Node} = M, D) ->
                                     dict:append(Node, M, D)
                             end, dict:new(), Members),

    ok = check_redundancies(1000, MemberDict, 3),
    ok.


%% @private
check_redundancies(0,_,_) ->
    ok;
check_redundancies(Index, MemberDict, TotalRacks) ->
    AddrId = leo_konsul_chash:vnode_id(128, crypto:strong_rand_bytes(64)),
    {ok, #redundancies{nodes = NodeL}} =
        leo_konsul_ring:lookup(?CLUSTER_ID, ?RING_TBL_CUR, AddrId),

    RackDict =
        lists:foldl(fun(#redundant_node{node = N,
                                        role = Role}, Sofar) ->
                            {ok, [#?MEMBER{grp_level_2 = R}|_]} = dict:find(N, MemberDict),
                            dict:append(R, {N, Role}, Sofar)
                    end, dict:new(), NodeL),
    ?assertEqual(TotalRacks, dict:size(RackDict)),

    ?check_redundancies(NodeL),
    check_redundancies(Index - 1, MemberDict, TotalRacks).


%% -------------------------------------------------------------------
%% INNER FUNCTION
%% -------------------------------------------------------------------
%% @doc Prepare the tests
prepare(Hostname) ->
    prepare(Hostname, 8).

prepare(Hostname, NumOfNodes) ->
    CallbackFun = fun()->
                          ok
                  end,
    Options = [{mq_dir, "work/mq-dir/"},
               {monitors, []},
               {membership_callback, CallbackFun},
               {system_conf, [{n, 3},
                              {w, 2},
                              {r ,1},
                              {d, 2}]}],
    ok = leo_konsul_api:start(?CLUSTER_ID, Options),

    Node_0 = list_to_atom("node_0@" ++ Hostname),
    Node_1 = list_to_atom("node_1@" ++ Hostname),
    Node_2 = list_to_atom("node_2@" ++ Hostname),
    Node_3 = list_to_atom("node_3@" ++ Hostname),
    Node_4 = list_to_atom("node_4@" ++ Hostname),
    Node_5 = list_to_atom("node_5@" ++ Hostname),
    Node_6 = list_to_atom("node_6@" ++ Hostname),
    Node_7 = list_to_atom("node_7@" ++ Hostname),
    AllNodes = [Node_0, Node_1, Node_2, Node_3,
                Node_4, Node_5, Node_6, Node_7],
    TotalNodes = length(AllNodes),

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
    case NumOfNodes of
        TotalNodes ->
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
                                         state = ?STATE_ATTACHED});
        _ ->
            void
    end,
    ok.


%% @doc Test the basic functions
%% @private
inspect_1(Hostname, NumOfIteration) ->
    %% Check member-related functions
    {ok, Members_1} = leo_konsul_api:get_members(?CLUSTER_ID),
    {ok, {MembersCur, MembersPrev}} =
        leo_konsul_api:get_all_ver_members(?CLUSTER_ID),

    ?assertEqual(8, length(Members_1)),
    ?assertEqual(8, length(MembersCur)),
    ?assertEqual(8, length(MembersPrev)),
    ?assertEqual(true, leo_konsul_api:has_member(
                         ?CLUSTER_ID, list_to_atom("node_3@" ++ Hostname))),
    ?assertEqual(false, leo_konsul_api:has_member(
                          ?CLUSTER_ID, list_to_atom("node_8@" ++ Hostname))),
    {ok, Member_5} = leo_konsul_api:get_member_by_node(
                       ?CLUSTER_ID, list_to_atom("node_5@" ++ Hostname)),
    ?assertEqual(list_to_atom("node_5@" ++ Hostname), Member_5#?MEMBER.node),


    %% Create routing-table > confirmations
    {ok, MembersL_1, Chksums} = leo_konsul_api:create(?CLUSTER_ID),

    ok = leo_konsul_api:update_member_by_node(
           ?CLUSTER_ID,
           list_to_atom("node_7@" ++ Hostname),
           12345, ?STATE_SUSPEND),
    {ok, Member_7} = leo_konsul_api:get_member_by_node(
                       ?CLUSTER_ID,
                       list_to_atom("node_7@" ++ Hostname)),
    ?assertEqual(12345, Member_7#?MEMBER.clock),
    ?assertEqual(?STATE_SUSPEND, Member_7#?MEMBER.state),

    {ok, {Chksum_0, Chksum_1}} = leo_konsul_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    ?assertEqual(true, (Chksum_0 > -1)),
    ?assertEqual(true, (Chksum_1 > -1)),
    ?assertEqual(8, length(MembersL_1)),
    ?assertEqual(2, length(Chksums)),

    {ok, {Chksum_2, Chksum_3}} = leo_konsul_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_RING),
    {ok, {Chksum_4, Chksum_5}} = leo_konsul_api:checksum(
                                   ?CLUSTER_ID, ?CHECKSUM_MEMBER),
    ?assertEqual(true, (-1 =< Chksum_2)),
    ?assertEqual(true, (-1 =< Chksum_3)),
    ?assertEqual(true, (-1 =< Chksum_4)),
    ?assertEqual(true, (-1 =< Chksum_5)),

    %% Check redundant nodes
    {ok, #redundancies{nodes = N0}} =
        leo_konsul_api:get_redundancies_by_addr_id(?CLUSTER_ID, put, 0),
    {ok, #redundancies{nodes = N1}} =
        leo_konsul_api:get_redundancies_by_addr_id(?CLUSTER_ID, put, leo_math:power(2, 128)),
    ?assertEqual(3, length(N0)),
    ?assertEqual(3, length(N1)),

    ok = inspect_redundancies_1(NumOfIteration),
    ok = inspect_redundancies_2(NumOfIteration),

    Max = leo_math:power(2, ?MD5),
    {ok, #redundancies{id = Id,
                       vnode_id_to = VNodeId1,
                       nodes = Nodes1,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}} = leo_konsul_api:get_redundancies_by_addr_id(
                                   ?CLUSTER_ID, put, Max + 1),
    ?assertEqual(true, (Id > VNodeId1)),
    ?assertEqual(3, length(Nodes1)),

    lists:foreach(fun(_) ->
                          Id2 = random:uniform(Max),
                          {ok, Res2} = leo_konsul_api:range_of_vnodes(
                                         ?CLUSTER_ID, Id2),
                          inspect_2(Id2, Res2)
                  end, lists:seq(0, 300)),
    {ok, Res3} = leo_konsul_api:range_of_vnodes(
                   ?CLUSTER_ID, 0),
    inspect_2(0, Res3),

    Max1 = leo_math:power(2,128) - 1,
    {ok, Res4} = leo_konsul_api:range_of_vnodes(
                   ?CLUSTER_ID, Max1),
    ?assertEqual(2, length(Res4)),

    {ok, {_Options, Res6}} =
        leo_konsul_api:collect_redundancies_by_key(
          ?CLUSTER_ID, <<"air_on_the_g_string_2">>, 6),
    ?assertEqual(6, length(Res6)),

    {ok, {_Options, Res7}} =
        leo_konsul_api:part_of_collect_redundancies_by_key(
          ?CLUSTER_ID, 3, <<"air_on_the_g_string_2\n3">>, 6, 3),
    Res7_1 = lists:sublist(Res6, 3),
    ?assertEqual(Res7, Res7_1),

    ok = leo_konsul_api:dump(?CLUSTER_ID, work),
    ok.


%% @private
inspect_redundancies_1(0) ->
    ok;
inspect_redundancies_1(Counter) ->
    case leo_konsul_api:get_redundancies_by_key(
           ?CLUSTER_ID, integer_to_list(Counter)) of
        {ok, #redundancies{id = _Id0,
                           vnode_id_to = _VNodeId0,
                           nodes = Nodes0,
                           n = 3,
                           r = 1,
                           w = 2,
                           d = 2}} ->
            lists:foreach(fun(A) ->
                                  Nodes0a = lists:delete(A, Nodes0),
                                  lists:foreach(fun(B) ->
                                                        ?assertEqual(false, (A == B))
                                                end, Nodes0a)
                          end, Nodes0),
            ?assertEqual(3, length(Nodes0));
        _Other ->
            ?debugVal(_Other)
    end,
    inspect_redundancies_1(Counter - 1).

inspect_redundancies_2(0) ->
    ok;
inspect_redundancies_2(Counter) ->
    Max = leo_math:power(2, ?MD5),
    Id  = random:uniform(Max),
    {ok, #redundancies{id = _Id0,
                       vnode_id_to = _VNodeId0,
                       nodes = Nodes0,
                       n = 3,
                       r = 1,
                       w = 2,
                       d = 2}
    } = leo_konsul_api:get_redundancies_by_addr_id(
          ?CLUSTER_ID, put, Id),
    lists:foreach(fun(A) ->
                          Nodes0a = lists:delete(A, Nodes0),
                          lists:foreach(fun(B) ->
                                                ?assertEqual(false, (A == B))
                                        end, Nodes0a)
                  end, Nodes0),
    ?assertEqual(3, length(Nodes0)),
    inspect_redundancies_2(Counter - 1).

%% @private
inspect_2(Id, VNodes) ->
    Max = leo_math:power(2, 128),
    case length(VNodes) of
        1 ->
            [{From, To}] = VNodes,
            ?assertEqual(true, ((From =< Id) andalso (Id =< To)));
        2 ->
            [{From0, To0}, {From1, To1}] = VNodes,
            case (From1 =< Id andalso Id =< To1) of
                true ->
                    ?assertEqual(true, ((From0 =< Max) andalso (To0 =< Max))),
                    ?assertEqual(true, ((From1 =< Id ) andalso (Id =< To1)));
                false ->
                    ?assertEqual(true, ((From0 =< Max) andalso (To0 =< Max)))
            end
    end.
-endif.
