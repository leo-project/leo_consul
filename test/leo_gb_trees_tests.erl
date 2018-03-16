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
-module(leo_gb_trees_tests).

-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

suite_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [
                           fun suite_/1
                          ]]}.

setup() ->
    ok.

teardown(_) ->
    ok.

suite_(_) ->
    NodeL = ['node_0', 'node_1', 'node_2', 'node_3'],
    L = lists:seq(1,1024),
    Step = 10,

    %% Test some functions
    Tree_1 = leo_gb_trees:empty(),
    ?assertEqual(true, leo_gb_trees:is_empty(Tree_1)),

    Tree_2 = lists:foldl(
               fun(N, Acc) ->
                       Node = lists:nth((N rem 4) + 1, NodeL),
                       leo_gb_trees:insert(N * Step, Node, Acc)
               end, Tree_1, L),
    ?assertEqual(false, leo_gb_trees:is_empty(Tree_2)),
    ?assertEqual(1024, leo_gb_trees:size(Tree_2)),

    Tree_3 = leo_gb_trees:balance(Tree_2),
    ?assertEqual(false, leo_gb_trees:is_empty(Tree_3)),
    ?assertEqual(1024, leo_gb_trees:size(Tree_3)),
    ?assertEqual(1024, erlang:length(leo_gb_trees:to_list(Tree_3))),
    ?assertEqual(1024, erlang:length(leo_gb_trees:keys(Tree_3))),
    ?assertEqual(1024, erlang:length(leo_gb_trees:values(Tree_3))),

    {ok, {ListHash, TreeHash}} = leo_gb_trees:checksum(Tree_3),
    ?assertEqual(false, ListHash == TreeHash),
    ?assertEqual(true, ListHash > 0),
    ?assertEqual(true, TreeHash > 0),

    FirstItem = leo_gb_trees:lookup(0, Tree_3),
    ?assertEqual({Step, 'node_1'}, FirstItem),

    lists:foldl(fun(_, {K, I}) ->
                        {K1,_V1, I1} = leo_gb_trees:next(I),

                        ?assertEqual(true, K1 /= K),
                        case K > 0 of
                            true ->
                                ?assertEqual(Step, K1 - K);
                            false ->
                                void
                        end,
                        {K1, I1}
                end, {-1, leo_gb_trees:iterator(Tree_3)}, L),

    %% Test lookup/2
    _ = erlang:statistics(wall_clock),
    [begin
         %% Test retrieving an item by clockwise
         %% =============================================
         %% |--- [10] --- [20] --- [30] --- ... |
         %%    1..10 => 10
         %%   11..20 => 20
         %%   21..30 => 30
         %% =============================================
         {Key_1,_Node_1} = leo_gb_trees:lookup(N, Tree_3),
         N_1 = leo_math:ceiling(N / Step) * Step,
         ?assertEqual(N_1, Key_1),

         case leo_gb_trees:lookup(Key_1 + 1, Tree_3) of
             {Key_2,_Node_2} ->
                 ?assertEqual(true, Key_2 =< 10240),

                 case leo_gb_trees:lookup(Key_2 + 1, Tree_3) of
                     {Key_3,_Node_3} ->
                         ?assertEqual(true, Key_3 =< 10240);
                     nil ->
                         ?assertEqual(10240, Key_2),
                         ?assertEqual(FirstItem, leo_gb_trees:lookup(0, Tree_3))
                 end;
             nil ->
                 ?assertEqual(10240, Key_1),
                 ?assertEqual(FirstItem, leo_gb_trees:lookup(0, Tree_3))
         end,
         ok
     end || N <- lists:seq(1, 10240)],
    {_,Time} = erlang:statistics(wall_clock),
    ?debugVal({time, Time}),
    ok.

-endif.
