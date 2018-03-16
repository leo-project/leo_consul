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
%% @doc The cluster member table operation
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_cluster_tbl_member.erl
%% @end
%%======================================================================
-module(leo_cluster_tbl_member).

-include("leo_konsul.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([create_table/1, create_table/3,
         lookup/2, lookup/3,

         find_by_cluster_id/1, find_by_cluster_id/2,
         find_by_status/2, find_by_status/3,
         find_by_level1/3, find_by_level1/4,
         find_by_level2/2, find_by_level2/3,

         find_by_alias/2, find_by_alias/3,
         find_by_name/3,

         get_available_members_by_cluster_id/1,
         get_available_members_by_cluster_id/2,

         insert/1, insert/2,
         bulk_insert/2,
         delete/2, delete/3,
         delete_all/2,
         replace/2, replace/3,
         overwrite/3,

         table_size/1, table_size/2,
         first/2, next/3,
         checksum/2
        ]).
-export([transform/0]).


%% @doc Create the member table.
-spec(create_table(Tbl) ->
             ok when Tbl::member_table()).
create_table(Tbl) ->
    create_table('ram_copies', [node()], Tbl).

-spec(create_table(MnesiaCopy, Nodes, Tbl) ->
             ok when MnesiaCopy::mnesia_copies(),
                     Nodes::[atom()],
                     Tbl::member_table()).
create_table(MnesiaCopy, Nodes, Tbl) ->
    case mnesia:create_table(
           Tbl,
           [{MnesiaCopy, Nodes},
            {type, set},
            {record_name, ?MEMBER},
            {attributes, record_info(fields, ?MEMBER)}
           ]) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason};
        _Other ->
            _Other
    end.


%% @doc Retrieve a record by key from the table.
-spec(lookup(ClusterId, Node) ->
             {ok, #?MEMBER{}} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Node::atom()).
lookup(ClusterId, Node) ->
    lookup(?MEMBER_TBL_CUR, ClusterId, Node).

-spec(lookup(Tbl, ClusterId, Node) ->
             {ok, #?MEMBER{}} |
             not_found |
             {error, any()} when Tbl::atom(),
                                 ClusterId::cluster_id(),
                                 Node::atom()).
lookup(Tbl, ClusterId, Node) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Tbl),
                                X#?MEMBER.cluster_id == ClusterId,
                                X#?MEMBER.node == Node]),
                qlc:e(Q)
        end,
    case leo_mnesia:read(F) of
        {ok, [H|_]} ->
            {ok, H};
        Other ->
            Other
    end.


%% @doc Retrieve all members from the table.
-spec(find_by_cluster_id(ClusterId) ->
             {ok, [#?MEMBER{}]} | not_found | {error, any()} when ClusterId::cluster_id()).
find_by_cluster_id(ClusterId) ->
    find_by_cluster_id(?MEMBER_TBL_CUR, ClusterId).

-spec(find_by_cluster_id(Tbl, ClusterId) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when Tbl::member_table(),
                                 ClusterId::cluster_id()).
find_by_cluster_id(Tbl, ClusterId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId
                           ]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve members by status
-spec(find_by_status(ClusterId, Status) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Status::atom()).
find_by_status(ClusterId, Status) ->
    find_by_status(?MEMBER_TBL_CUR, ClusterId, Status).

-spec(find_by_status(Tbl, ClusterId, Status) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when Tbl::member_table(),
                                 ClusterId::cluster_id(),
                                 Status::atom()).
find_by_status(Tbl, ClusterId, Status) ->
    F = fun() ->
                Q = qlc:q([X || X <- mnesia:table(Tbl),
                                X#?MEMBER.cluster_id == ClusterId,
                                X#?MEMBER.state == Status]),
                qlc:e(Q)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve records by L1 and L2
-spec(find_by_level1(ClusterId, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(ClusterId, L1, L2) ->
    find_by_level1(?MEMBER_TBL_CUR, ClusterId, L1, L2).

-spec(find_by_level1(Tbl, ClusterId, L1, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Tbl::member_table(),
                                 ClusterId::cluster_id(),
                                 L1::atom(),
                                 L2::atom()).
find_by_level1(Tbl, ClusterId, L1, L2) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.grp_level_1 == L1,
                                 X#?MEMBER.grp_level_2 == L2]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve records by L2
-spec(find_by_level2(ClusterId, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 L2::atom()).
find_by_level2(ClusterId, L2) ->
    find_by_level2(?MEMBER_TBL_CUR, ClusterId, L2).

-spec(find_by_level2(Tbl, ClusterId, L2) ->
             {ok, list()} |
             not_found |
             {error, any()} when Tbl::member_table(),
                                 ClusterId::cluster_id(),
                                 L2::atom()).
find_by_level2(Tbl, ClusterId, L2) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.grp_level_2 == L2]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve records by alias
-spec(find_by_alias(ClusterId, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when ClusterId::cluster_id(),
                                 Alias::string()).
find_by_alias(ClusterId, Alias) ->
    find_by_alias(?MEMBER_TBL_CUR, ClusterId, Alias).

-spec(find_by_alias(Tbl, ClusterId, Alias) ->
             {ok, list()} |
             not_found |
             {error, any()} when Tbl::atom(),
                                 ClusterId::cluster_id(),
                                 Alias::string()).
find_by_alias(Tbl, ClusterId, Alias) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.alias == Alias]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve records by name
-spec(find_by_name(Tbl, ClusterId, Name) ->
             {ok, list()} |
             not_found |
             {error, any()} when Tbl::atom(),
                                 ClusterId::cluster_id(),
                                 Name::atom()).
find_by_name(Tbl, ClusterId, Name) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.node == Name]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Retrieve available members from the table.
-spec(get_available_members_by_cluster_id(ClusterId) ->
             {ok, [#?MEMBER{}]} | not_found | {error, any()} when ClusterId::cluster_id()).
get_available_members_by_cluster_id(ClusterId) ->
    get_available_members_by_cluster_id(?MEMBER_TBL_CUR, ClusterId).

-spec(get_available_members_by_cluster_id(Tbl, ClusterId) ->
             {ok, [#?MEMBER{}]} |
             not_found |
             {error, any()} when Tbl::member_table(),
                                 ClusterId::cluster_id()).
get_available_members_by_cluster_id(Tbl, ClusterId) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.state /= ?STATE_DETACHED,
                                 X#?MEMBER.state /= ?STATE_RESERVED
                           ]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    leo_mnesia:read(F).


%% @doc Insert a record into the table.
-spec(insert(Member) ->
             ok | {error, any} when Member::#?MEMBER{}).
insert(Member) ->
    insert(?MEMBER_TBL_CUR, Member).

-spec(insert(Tbl, Member) ->
             ok | {error, any} when Tbl::member_table(),
                                    Member::#?MEMBER{}).
insert(Tbl, #?MEMBER{node = Node,
                       cluster_id = ClusterId,
                       state = State,
                       clock = Clock} = Member) ->
    Ret = case lookup(Tbl, ClusterId, Node) of
              {ok, #?MEMBER{state = State,
                            clock = Clock_1}} when Clock >= Clock_1 ->
                  ok;
              {ok, #?MEMBER{state = State,
                            clock = Clock_1}} when Clock < Clock_1 ->
                  {error, ignore};
              {ok,_} ->
                  ok;
              not_found ->
                  ok;
              {error, Cause} ->
                  {error, Cause}
          end,
    insert_1(Ret, Tbl, Member).

%% @private
insert_1(ok, Tbl, Member) ->
    Fun = fun() ->
                  mnesia:write(Tbl, Member, write)
          end,
    leo_mnesia:write(Fun);
insert_1({error, ignore},_,_) ->
    ok;
insert_1({error, Cause},_,_) ->
    {error, Cause}.


%% @doc Insert members at one time
bulk_insert(Tbl, MemberL) ->
    F = fun() ->
                lists:foreach(fun(Member) ->
                                      ok = mnesia:write(Tbl, Member, write)
                              end, MemberL)
        end,
    try
        mnesia:activity(transaction, F)
    catch
        _: Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "bulk_insert/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {errro, "bulk-isert failure"}
    end.


%% @doc Remove a record from the table.
-spec(delete(ClusterId, Node) ->
             ok | {error, any} when ClusterId::cluster_id(),
                                    Node::atom()).
delete(ClusterId, Node) ->
    delete(?MEMBER_TBL_CUR, ClusterId, Node).

-spec(delete(Tbl, ClusterId, Node) ->
             ok | {error, any} when Tbl::member_table(),
                                    ClusterId::cluster_id(),
                                    Node::atom()).
delete(Tbl, ClusterId, Node) ->
    case lookup(Tbl, ClusterId, Node) of
        {ok, Member} ->
            Fun = fun() ->
                          mnesia:delete_object(Tbl, Member, write)
                  end,
            leo_mnesia:delete(Fun);
        not_found ->
            ok;
        Error ->
            Error
    end.


%% @doc Remove all records
-spec(delete_all(Tbl, ClusterId) ->
             ok | {error, any()} when Tbl::member_table(),
                                      ClusterId::cluster_id()).
delete_all(Tbl, ClusterId) ->
    case find_by_cluster_id(Tbl, ClusterId) of
        {ok, RetL} ->
            case mnesia:sync_transaction(
                   fun() ->
                           case delete_all_1(RetL, Tbl) of
                               ok ->
                                   ok;
                               _ ->
                                   mnesia:abort("Could Not remove due to an error")
                           end
                   end) of
                {atomic, ok} ->
                    ok;
                {aborted, Reason} ->
                    {error, Reason}
            end;
        not_found ->
            ok;
        Error ->
            Error
    end.

%% @private
delete_all_1([],_) ->
    ok;
delete_all_1([#?MEMBER{} = Member|Rest], Tbl) ->
    case mnesia:delete_object(Tbl, Member, write) of
        ok ->
            delete_all_1(Rest, Tbl);
        _ ->
            {error, transaction_abort}
    end.


%% @doc Replace members into the db.
-spec(replace(OldMembers, NewMembers) ->
             ok | {error, any()} when OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
replace(OldMembers, NewMembers) ->
    replace(?MEMBER_TBL_CUR, OldMembers, NewMembers).

-spec(replace(Tbl, OldMembers, NewMembers) ->
             ok | {error, any()} when Tbl::member_table(),
                                      OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
replace(Tbl, OldMembers, NewMembers) ->
    Fun = fun() ->
                  lists:foreach(fun(Member_1) ->
                                        mnesia:delete_object(Tbl, Member_1, write)
                                end, OldMembers),
                  lists:foreach(fun(Member_2) ->
                                        mnesia:write(Tbl, Member_2, write)
                                end, NewMembers)
          end,
    leo_mnesia:batch(Fun).


%% @doc Overwrite current records by source records
-spec(overwrite(SrcTbl, DestTbl, ClusterId) ->
             ok | {error, any()} when SrcTbl::member_table(),
                                      DestTbl::member_table(),
                                      ClusterId::cluster_id()).
overwrite(SrcTbl, DestTbl, ClusterId) ->
    case find_by_cluster_id(SrcTbl, ClusterId) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            delete_all(DestTbl, ClusterId);
        {ok, Members} ->
            overwrite_1(DestTbl, ClusterId, Members)
    end.

%% @private
overwrite_1(Tbl, ClusterId, Members) ->
    case mnesia:sync_transaction(
           fun() ->
                   overwrite_2(Tbl, ClusterId, Members)
           end) of
        {atomic, ok} ->
            ok;
        {aborted, Reason} ->
            {error, Reason}
    end.

%% @private
overwrite_2(_,_,[]) ->
    ok;
overwrite_2(Tbl, ClusterId, [#?MEMBER{node = Node} = Member|Rest]) ->
    case mnesia:delete(Tbl, Node, write) of
        ok ->
            case mnesia:write(
                   Tbl, Member#?MEMBER{cluster_id = ClusterId}, write) of
                ok ->
                    overwrite_2(Tbl, ClusterId, Rest);
                _ ->
                    mnesia:abort("Not inserted")
            end;
        _ ->
            mnesia:abort("Not removed")
    end.


%% @doc Retrieve total of records.
-spec(table_size(ClusterId) ->
             integer() | {error, any()} when ClusterId::cluster_id()).
table_size(ClusterId) ->
    table_size(?MEMBER_TBL_CUR, ClusterId).

-spec(table_size(Tbl, ClusterId) ->
             integer() | {error, any()} when Tbl::atom(),
                                             ClusterId::cluster_id()).
table_size(Tbl, ClusterId) ->
    case find_by_cluster_id(Tbl, ClusterId) of
        {ok, RetL} ->
            erlang:length(RetL);
        not_found ->
            0;
        Other ->
            Other
    end.


%% @doc Go to first record
-spec(first(Tbl, ClusterId) ->
             atom() | '$end_of_table' when Tbl::atom(),
                                           ClusterId::cluster_id()).
first(Tbl, ClusterId) ->
    case find_by_cluster_id(Tbl, ClusterId) of
        {ok, [#?MEMBER{node = N}|_]} ->
            {ok, N};
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @doc Go to next record
-spec(next(Tbl, ClusterId, Node) ->
             atom() | '$end_of_table' when Tbl::atom(),
                                           ClusterId::cluster_id(),
                                           Node::atom()).
next(Tbl, ClusterId, Node) ->
    F = fun() ->
                Q1 = qlc:q([X || X <- mnesia:table(Tbl),
                                 X#?MEMBER.cluster_id == ClusterId,
                                 X#?MEMBER.node /= Node]),
                Q2 = qlc:sort(Q1, [{order, ascending}]),
                qlc:e(Q2)
        end,
    case leo_mnesia:read(F) of
        {ok, [#?MEMBER{node = Node_1}|_]} ->
            {ok, Node_1};
        not_found ->
            '$end_of_table';
        Other ->
            Other
    end.


%% @doc Retrieves the checksum of a table by cluster-id
-spec(checksum(Tbl, ClusterId) ->
             Checksum when Tbl::atom(),
                           ClusterId::cluster_id(),
                           Checksum::integer()).
checksum(Tbl, ClusterId) ->
    case find_by_cluster_id(Tbl, ClusterId) of
        {ok, Members} ->
            erlang:crc32(term_to_binary(Members));
        _ ->
            -1
    end.


%% @TODO
%% @doc Transform records
-spec(transform() ->
             ok | {error, any()}).
transform() ->
    ok.
