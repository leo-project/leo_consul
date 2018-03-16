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
-module(leo_cluster_tbl_member_tests).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-define(ETS, 'ets').
-define(MNESIA, 'mnesia').

-define(CLUSTER_ID_1, 'leofs_cluster_1').
-define(CLUSTER_ID_2, 'leofs_cluster_2').

-define(NODE_0, 'node_0@127.0.0.1').
-define(NODE_1, 'node_1@127.0.0.1').
-define(NODE_2, 'node_2@127.0.0.1').
-define(NODE_3, 'node_3@127.0.0.1').
-define(NODE_4, 'node_4@127.0.0.1').

-define(RACK_1, "rack_1").
-define(RACK_2, "rack_2").

-define(MEMBER_0, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_0},
                           cluster_id = ?CLUSTER_ID_1,
                           node = ?NODE_0,
                           alias="node_0_a",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_1},
                           node = ?NODE_1,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_1_b",
                           state = ?STATE_SUSPEND,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_2, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_2},
                           node = ?NODE_2,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_2_c",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_1}).
-define(MEMBER_3, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_3},
                           node = ?NODE_3,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_3_d",
                           state = ?STATE_RUNNING,
                           grp_level_2 = ?RACK_2}).
-define(MEMBER_4, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_4},
                           node = ?NODE_4,
                           cluster_id = ?CLUSTER_ID_1,
                           alias="node_4_e",
                           state = ?STATE_STOP,
                           grp_level_2 = ?RACK_2}).


-define(MEMBER_0_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_0},
                             cluster_id = ?CLUSTER_ID_1,
                             node = ?NODE_0,
                             alias="node_0_a",
                             state = ?STATE_RUNNING,
                             grp_level_2 = ?RACK_1}).
-define(MEMBER_1_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_1},
                             node = ?NODE_1,
                             cluster_id = ?CLUSTER_ID_1,
                             alias="node_1_b",
                             state = ?STATE_RUNNING,
                             grp_level_2 = ?RACK_1}).
-define(MEMBER_2_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_2},
                             node = ?NODE_2,
                             cluster_id = ?CLUSTER_ID_1,
                             alias="node_2_c",
                             state = ?STATE_RUNNING,
                             grp_level_2 = ?RACK_1}).
-define(MEMBER_3_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_3},
                             node = ?NODE_3,
                             cluster_id = ?CLUSTER_ID_1,
                             alias="node_3_d",
                             state = ?STATE_RUNNING,
                             grp_level_2 = ?RACK_2}).
-define(MEMBER_4_1, #?MEMBER{id = {?CLUSTER_ID_1, ?NODE_4},
                             node = ?NODE_4,
                             cluster_id = ?CLUSTER_ID_1,
                             alias="node_4_e",
                             state = ?STATE_DETACHED,
                             grp_level_2 = ?RACK_2}).

-define(MEMBER_1_L, [?MEMBER_0_1,
                     ?MEMBER_1_1,
                     ?MEMBER_2_1,
                     ?MEMBER_3_1,
                     ?MEMBER_4_1
                    ]).

member_tbl_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [
                           fun suite_mnesia_/1
                          ]]}.

setup() ->
    ok.
teardown(_) ->
    ok.


suite_mnesia_(_) ->
    application:start(mnesia),
    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_CUR),
    ok = leo_cluster_tbl_member:create_table('ram_copies', [node()], ?MEMBER_TBL_PREV),
    ok = inspect_1(),
    ok = inspect_2(),
    application:stop(mnesia),
    ok.


%% @private
inspect_1() ->
    not_found = leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    not_found = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    -1 = leo_cluster_tbl_member:checksum(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),

    %% Insert records
    ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, ?MEMBER_0),
    ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, ?MEMBER_1),
    ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, ?MEMBER_2),
    ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, ?MEMBER_3),
    ok = leo_cluster_tbl_member:insert(?MEMBER_TBL_CUR, ?MEMBER_4),

    %% Lookup records
    {ok, Ret_1} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    ?assertEqual(?MEMBER_0, Ret_1),
    {ok, Ret_2} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_1),
    ?assertEqual(?MEMBER_1, Ret_2),
    {ok, Ret_3} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_2),
    ?assertEqual(?MEMBER_2, Ret_3),
    {ok, Ret_4} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_3),
    ?assertEqual(?MEMBER_3, Ret_4),
    {ok, Ret_5} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_4),
    ?assertEqual(?MEMBER_4, Ret_5),

    {ok, RetL_1} = leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(5, erlang:length(RetL_1)),

    {ok, RetL_2} = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR,
                                                         ?CLUSTER_ID_1, ?STATE_RUNNING),
    ?assertEqual(3, erlang:length(RetL_2)),

    {ok, RetL_3} = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR,
                                                         ?CLUSTER_ID_1, ?STATE_SUSPEND),
    ?assertEqual(1, erlang:length(RetL_3)),

    {ok, RetL_4} = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR,
                                                         ?CLUSTER_ID_1, ?STATE_STOP),
    ?assertEqual(1, erlang:length(RetL_4)),


    {ok, Rack_1} = leo_cluster_tbl_member:find_by_level2(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_1),
    {ok, Rack_2} = leo_cluster_tbl_member:find_by_level2(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_2),
    ?assertEqual(3, length(Rack_1)),
    ?assertEqual(2, length(Rack_2)),

    Size_1 = leo_cluster_tbl_member:table_size(?CLUSTER_ID_1),
    Size_1 = leo_cluster_tbl_member:table_size(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(5, Size_1),

    ok = leo_cluster_tbl_member:delete(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    not_found = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),

    Size_2 = leo_cluster_tbl_member:table_size(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(4, Size_2),
    ok.


%% @private
inspect_2() ->
    %% exec bulk-insert
    ok = leo_cluster_tbl_member:bulk_insert(?MEMBER_TBL_CUR, ?MEMBER_1_L),

    %% Lookup records
    {ok, Ret_1} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    ?assertEqual(?MEMBER_0_1, Ret_1),
    {ok, Ret_2} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_1),
    ?assertEqual(?MEMBER_1_1, Ret_2),
    {ok, Ret_3} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_2),
    ?assertEqual(?MEMBER_2_1, Ret_3),
    {ok, Ret_4} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_3),
    ?assertEqual(?MEMBER_3_1, Ret_4),
    {ok, Ret_5} = leo_cluster_tbl_member:lookup(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_4),
    ?assertEqual(?MEMBER_4_1, Ret_5),

    %% find_by_cluster_id(Table, ClusterId)
    {ok, Ret_6} = leo_cluster_tbl_member:find_by_cluster_id(?CLUSTER_ID_1),
    {ok, Ret_6} = leo_cluster_tbl_member:find_by_cluster_id(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(5, length(Ret_6)),

    %% find_by_status(Table, ClusterId, Status)
    {ok, Ret_7} = leo_cluster_tbl_member:find_by_status(?CLUSTER_ID_1, ?STATE_RUNNING),
    {ok, Ret_7} = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?STATE_RUNNING),
    ?assertEqual(4, length(Ret_7)),
    not_found = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?STATE_ATTACHED),
    not_found = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?STATE_STOP),
    {ok, Ret_8} = leo_cluster_tbl_member:find_by_status(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?STATE_DETACHED),
    ?assertEqual(1, length(Ret_8)),


    %% find_by_level2(Table, ClusterId, L2)
    {ok, Ret_9} = leo_cluster_tbl_member:find_by_level2(?CLUSTER_ID_1, ?RACK_1),
    {ok, Ret_9} = leo_cluster_tbl_member:find_by_level2(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_1),
    ?assertEqual(3, length(Ret_9)),
    {ok, Ret_10} = leo_cluster_tbl_member:find_by_level2(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?RACK_2),
    ?assertEqual(2, length(Ret_10)),
    not_found = leo_cluster_tbl_member:find_by_level2(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, "invalid_rack"),

    %% find_by_alias(Table, ClusterId, Alias)
    {ok, Ret_11} = leo_cluster_tbl_member:find_by_alias(?CLUSTER_ID_1, "node_0_a"),
    {ok, Ret_11} = leo_cluster_tbl_member:find_by_alias(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, "node_0_a"),
    ?assertEqual(1, length(Ret_11)),
    not_found = leo_cluster_tbl_member:find_by_alias(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, "invalid_alias"),


    %% find_by_name(Table, ClusterId, Name)
    {ok, Ret_12} = leo_cluster_tbl_member:find_by_name(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, ?NODE_0),
    ?assertEqual(1, length(Ret_12)),
    not_found = leo_cluster_tbl_member:find_by_name(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, 'invalid_node'),

    5 = leo_cluster_tbl_member:table_size(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),

    %% first(Table, ClusterId)
    {ok, Ret_13} = leo_cluster_tbl_member:first(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(?NODE_0, Ret_13),

    {ok, Ret_14} = leo_cluster_tbl_member:next(?MEMBER_TBL_CUR, ?CLUSTER_ID_1, Ret_13),
    ?assertEqual(?NODE_1, Ret_14),

    Ret_15 = leo_cluster_tbl_member:checksum(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ?assertEqual(true, Ret_15 > -1),

    ok = leo_cluster_tbl_member:transform(),

    %% delete_all(Table, ClusterId)
    ok = leo_cluster_tbl_member:delete_all(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    0 = leo_cluster_tbl_member:table_size(?MEMBER_TBL_CUR, ?CLUSTER_ID_1),
    ok.

-endif.
