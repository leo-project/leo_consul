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
%% Leo Konsul - Consistent Hashing
%%
%% @doc The consistent-hashing implementation
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_konsul_chash.erl
%% @end
%%======================================================================
-module(leo_konsul_chash).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([add/1,
         add_from_list/1,
         range_of_vnodes/3,
         vnode_id/1, vnode_id/2]).
-export([export/2]).

-define(gen_child_name(_Alias,_N),
        lists:append([_Alias, "_", integer_to_list(_N)])).


%% @doc Add a node.
-spec(add(Members) ->
             {ok, VNodeList} |
             {error, any()} when Members::#?MEMBER{},
                             VNodeList::vnode_list()).
add(Member) ->
    add_1(0, Member, []).


%% @private
add_1(N, #?MEMBER{num_of_vnodes = N}, Acc) ->
    {ok, Acc};
add_1(N, #?MEMBER{alias = Alias,
                  node = Node,
                  grp_level_2 = RackId} = Member, Acc) ->
    VNodeId = vnode_id(?gen_child_name(Alias, N)),
    add_1(N + 1, Member, [{VNodeId, {Node, RackId}}|Acc]).


%% @doc Insert recods from the list
-spec(add_from_list(Members) ->
             {ok, VNodeIdList} | {error, any()} when Members::[#?MEMBER{}],
                                                     VNodeIdList::vnode_list()).
add_from_list(Members) ->
    add_from_list_1(Members, []).

%% @private
add_from_list_1([], Acc) ->
    {ok, Acc};
add_from_list_1([Member|Rest], Acc) ->
    {ok, Acc_1} = add_1(0, Member, Acc),
    add_from_list_1(Rest, Acc_1).


%% @doc Retrieve virtual-node-id
%%
-spec(vnode_id(Key) ->
             integer() when Key::any()).
vnode_id(Key) ->
    vnode_id(?MD5, Key).

vnode_id(?MD5, Key) ->
    leo_hex:raw_binary_to_integer(crypto:hash(md5, Key));
vnode_id(_, _) ->
    {error, badarg}.


%% @doc Dump table to a file.
%%
-spec(export(TableInfo, FileName) ->
             ok | {error, any()} when TableInfo::table_info(),
                                      FileName::string()).
export(_TableInfo,_FileName) ->
    %% @TODO (2018-01-18)
    %% {ok, RetL} = leo_cluster_tbl_ring:find_all(TableInfo),
    %% leo_file:file_unconsult(FileName, RetL).
    ok.


%% @doc Retrieve range of vnodes.
%%
-spec(range_of_vnodes(TableInfo, ClusterId, VNodeId) ->
             {ok, [tuple()]} when TableInfo::table_info(),
                                  ClusterId::cluster_id(),
                                  VNodeId::integer()).
range_of_vnodes(Tbl, ClusterId, VNodeId) ->
    case leo_konsul_ring:lookup(ClusterId, Tbl, VNodeId) of
        not_found ->
            {error, not_found};
        {ok, #redundancies{vnode_id_from = From,
                           vnode_id_to = To}} ->
            case From of
                0 ->
                    case leo_konsul_ring:last(ClusterId, Tbl) of
                        not_found ->
                            {ok, [{From, To}]};
                        {ok, #redundancies{vnode_id_to = LastId}} ->
                            {ok, [{From, To},
                                  {LastId + 1, leo_math:power(2, ?MD5)}]}
                    end;
                _ ->
                    {ok, [{From, To}]}
            end
    end.


%% %%====================================================================
%% %% Internal functions
%% %%====================================================================
%% %% @doc Retrieve active nodes.
%% %% @private
%% active_node(_Members, []) ->
%%     {error, no_entry};
%% active_node(Members, [#redundant_node{node = Node_1}|T]) ->
%%     case lists:foldl(
%%            fun(#?MEMBER{node  = Node_2,
%%                         state = ?STATE_RUNNING}, []) when Node_1 == Node_2 ->
%%                    Node_2;
%%               (_Member, SoFar) ->
%%                    SoFar
%%            end, [], Members) of
%%         [] ->
%%             active_node(Members, T);
%%         Res ->
%%             Res
%%     end.
