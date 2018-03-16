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
%% Leo Konsul - Server
%%
%% @doc leo_redaundant_manager's server
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_konsul.erl
%% @end
%%======================================================================
-module(leo_konsul_member).

-behaviour(gen_server).

-include("leo_konsul.hrl").
-include_lib("leo_rpc/include/leo_rpc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/1]).

-export([checksum/1, checksum/2,
         has_member/2, find_all/1, find_all/2,
         get_available_members/1, get_available_members/2,
         find_by_node/2, find_by_node/3,
         find_by_status/3,
         bulk_insert/3,
         insert/2,
         update/2,
         bulk_update/2, bulk_update/4,
         update_by_node/3, update_by_node/4,
         delete_by_node/2,
         overwrite/3, dump/1]).

-export([join/2, reserve/2,
         leave/3, leave/4, suspend/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, timer:seconds(30)).
-define(DEF_TIMEOUT_LONG, timer:seconds(120)).

-record(state, {id :: atom(),
                cluster_id :: cluster_id()
               }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the process
start_link(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId], []).

%% @doc Stop the process
stop(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Retrieve checksum of ring/member
-spec(checksum(ClusterId) ->
             integer() when ClusterId::cluster_id()).
checksum(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {checksum, ?VER_CUR}, ?DEF_TIMEOUT).

-spec(checksum(ClusterId, Ver) ->
             {ok, integer() | tuple()} when ClusterId::cluster_id(),
                                            Ver::version()).
checksum(ClusterId, Ver) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {checksum, Ver}, ?DEF_TIMEOUT).


%% @doc Is exists member?
-spec(has_member(ClusterId, Node) ->
             boolean() when ClusterId::cluster_id(),
                            Node::atom()).
has_member(ClusterId, Node) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {has_member, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve all members
-spec(find_all(ClusterId) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id()).
find_all(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {find_all, ?VER_CUR}, ?DEF_TIMEOUT).

-spec(find_all(ClusterId, Ver) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id(),
                                                      Ver::version()).
find_all(ClusterId, Ver) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {find_all, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve available members
-spec(get_available_members(ClusterId) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id()).
get_available_members(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {get_available_members, ?VER_CUR}, ?DEF_TIMEOUT).

-spec(get_available_members(ClusterId, Ver) ->
             {ok, [#?MEMBER{}]} | {error, any()} when ClusterId::cluster_id(),
                                                      Ver::version()).
get_available_members(ClusterId, Ver) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {get_available_members, Ver}, ?DEF_TIMEOUT).


%% @doc Retrieve a member by node
-spec(find_by_node(ClusterId, Node) ->
             {ok, #?MEMBER{}} | {error, any()} when ClusterId::cluster_id(),
                                                    Node::atom()).
find_by_node(ClusterId, Node) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {find_by_node, ?VER_CUR, Node}, ?DEF_TIMEOUT).

-spec(find_by_node(ClusterId, Ver, Node) ->
             {ok, #?MEMBER{}} | {error, any()} when ClusterId::cluster_id(),
                                                    Ver::version(),
                                                    Node::atom()).
find_by_node(ClusterId, Ver, Node) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {find_by_node, Ver, Node}, ?DEF_TIMEOUT).


%% @doc Retrieve members by status
-spec(find_by_status(ClusterId, Ver, Status) ->
             {ok, [#?MEMBER{}]} |
             {error, any()} when ClusterId::cluster_id(),
                                 Ver::?VER_CUR|?VER_PREV,
                                 Status::atom()).
find_by_status(ClusterId, Ver, Status) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {find_by_status, Ver, Status}, ?DEF_TIMEOUT).


-spec(bulk_insert(ClusterId, Ver, List) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Ver::version(),
                                      List::[#?MEMBER{}]).
bulk_insert(ClusterId, Ver, List) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {bulk_insert, Ver, List}, ?DEF_TIMEOUT).


%% @doc Modify a member
-spec(insert(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
insert(ClusterId, Member) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {update, Member}, ?DEF_TIMEOUT).


%% @doc Modify a member
-spec(update(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
update(ClusterId, Member) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {update, Member}, ?DEF_TIMEOUT).


%% @doc Modify members
-spec(bulk_update(ClusterId, Members) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Members::[#?MEMBER{}]).
bulk_update(ClusterId, Members) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {bulk_update, Members}, ?DEF_TIMEOUT).

-spec(bulk_update(ClusterId, Tbl, OldMembers, NewMembers) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Tbl::atom(),
                                      OldMembers::[#?MEMBER{}],
                                      NewMembers::[#?MEMBER{}]).
bulk_update(ClusterId, Tbl, OldMembers, NewMembers) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {bulk_update, Tbl, OldMembers, NewMembers}, ?DEF_TIMEOUT).


%% @doc Modify a member by node
-spec(update_by_node(ClusterId, Node, NodeState) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      NodeState::atom()).
update_by_node(ClusterId, Node, NodeState) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {update_by_node, Node, NodeState}, ?DEF_TIMEOUT).

-spec(update_by_node(ClusterId, Node, Clock, NodeState) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer(),
                                      NodeState::atom()).
update_by_node(ClusterId, Node, Clock, NodeState) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {update_by_node, Node, Clock, NodeState}, ?DEF_TIMEOUT).


%% @doc Remove a member by node
-spec(delete_by_node(ClusterId, Node) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom()).
delete_by_node(ClusterId, Node) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {delete_by_node, Node}, ?DEF_TIMEOUT).


%% @doc Overwrite records
-spec(overwrite(ClusterId, Tbl_1, Tbl_2) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Tbl_1::?MEMBER_TBL_CUR | ?MEMBER_TBL_PREV,
                                      Tbl_2::?MEMBER_TBL_CUR | ?MEMBER_TBL_PREV).
overwrite(ClusterId, Tbl_1, Tbl_2) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {overwrite, Tbl_1, Tbl_2}, ?DEF_TIMEOUT).


%% @doc Dump files which are member and ring
-spec(dump(ClusterId) ->
             ok when ClusterId::cluster_id()).
dump(ClusterId) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, dump, ?DEF_TIMEOUT).


%% @doc Change node status to 'join'
-spec(join(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
join(ClusterId, #?MEMBER{} = Member) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {join, Member}, ?DEF_TIMEOUT).


%% @doc Change node status to 'reserve'
-spec(reserve(ClusterId, Member) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Member::#?MEMBER{}).
reserve(ClusterId, Member) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {reserve, Member}, ?DEF_TIMEOUT).


%% @doc Change node status to 'leave'
-spec(leave(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
leave(ClusterId, Node, Clock) ->
    TblInfo = leo_konsul_api:table_info(?VER_CUR),
    leave(ClusterId, TblInfo, Node, Clock).

-spec(leave(ClusterId, TblInfo, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      TblInfo::ring_table_info(),
                                      Node::atom(),
                                      Clock::integer()).
leave(ClusterId, TblInfo, Node, Clock) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {leave, TblInfo, Node, Clock}, ?DEF_TIMEOUT).


%% @doc Change node status to 'suspend'
-spec(suspend(ClusterId, Node, Clock) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      Node::atom(),
                                      Clock::integer()).
suspend(ClusterId, Node, Clock) ->
    Id = ?id_member(ClusterId),
    gen_server:call(Id, {suspend, Node, Clock}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Id, ClusterId]) ->
    {ok, #state{id = Id,
                cluster_id = ClusterId}}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};


handle_call({checksum, Ver}, _From, #state{cluster_id = ClusterId} = State) ->
    Hash = leo_cluster_tbl_member:checksum(?member_table(Ver), ClusterId),
    {reply, Hash, State};

handle_call({has_member, Node}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, _} ->
                    true;
                _Other ->
                    false
            end,
    {reply, Reply, State};

handle_call({find_all, Ver}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = find_all_1(Ver, ClusterId),
    {reply, Reply, State};

handle_call({get_available_members, Ver}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:get_available_members_by_cluster_id(
                   ?member_table(Ver), ClusterId) of
                {ok, Members} ->
                    {ok, Members};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({find_by_node, Ver, Node}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(
                   ?member_table(Ver), ClusterId, Node) of
                {ok, Member} ->
                    {ok, Member};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({find_by_status, Ver, Status}, _From, #state{cluster_id = ClusterId} = State) ->
    Tbl = ?member_table(Ver),
    Reply = case leo_cluster_tbl_member:find_by_status(
                   Tbl, ClusterId, Status) of
                {ok, Members} ->
                    {ok, Members};
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({update, #?MEMBER{state = MemberState} = Member},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(MemberState) of
                true ->
                    leo_cluster_tbl_member:insert(
                      Member#?MEMBER{cluster_id = ClusterId});
                false ->
                    ok
            end,
    {reply, Reply, State};

handle_call({bulk_insert, Ver, List}, _From, State) ->
    Reply = leo_cluster_tbl_member:bulk_insert(?member_table(Ver), List),
    {reply, Reply, State};

handle_call({bulk_update, Members}, _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:find_by_cluster_id(ClusterId) of
                {ok, CurMembers} ->
                    CurMembers_1 = lists:reverse(CurMembers),
                    CurMembersHash = erlang:crc32(term_to_binary(CurMembers_1)),
                    MembersHash = erlang:crc32(term_to_binary(Members)),

                    case (MembersHash =:= CurMembersHash) of
                        true ->
                            ok;
                        false ->
                            leo_cluster_tbl_member:replace(
                              ClusterId, CurMembers_1, Members)
                    end;
                not_found ->
                    leo_cluster_tbl_member:replace(ClusterId, [], Members);
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({bulk_update, Tbl, OldMembers, NewMembers},_From, State) ->
    Reply = leo_cluster_tbl_member:replace(Tbl, OldMembers, NewMembers),
    {reply, Reply, State};

handle_call({update_by_node, Node, NodeState}, _From,
            #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(NodeState) of
                true ->
                    update_by_node_1(ClusterId, Node, NodeState);
                false ->
                    {error, incorrect_node_state}
            end,
    {reply, Reply, State};

handle_call({update_by_node, Node, Clock, NodeState}, _From,
            #state{cluster_id = ClusterId} = State) ->
    Reply = case ?is_correct_state(NodeState) of
                true ->
                    update_by_node_1(ClusterId, Node, Clock, NodeState);
                false ->
                    {error, incorrect_node_state}
            end,
    {reply, Reply, State};

handle_call({delete_by_node, Node},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = leo_cluster_tbl_member:delete(ClusterId, Node),
    {reply, Reply, State};


handle_call({overwrite, Tbl_1, Tbl_2},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = leo_cluster_tbl_member:overwrite(Tbl_1, Tbl_2, ClusterId),
    {reply, Reply, State};



handle_call(dump,_From, #state{cluster_id = ClusterId} = State) ->
    LogDir = case application:get_env(leo_konsul, log_dir_member) of
                 undefined ->
                     ?DEF_LOG_DIR_MEMBERS;
                 {ok, Dir} ->
                     case (string:len(Dir) == string:rstr(Dir, "/")) of
                         true  -> Dir;
                         false -> Dir ++ "/"
                     end
             end,
    _ = filelib:ensure_dir(LogDir),

    Reply = case leo_cluster_tbl_member:find_by_cluster_id(
                   ?MEMBER_TBL_CUR, ClusterId) of
                {ok, MembersCur} ->
                    Path_1 = lists:append([LogDir,
                                           ?DUMP_FILE_MEMBERS_CUR,
                                           atom_to_list(ClusterId),
                                           ".",
                                           integer_to_list(leo_date:now())]),
                    leo_file:file_unconsult(Path_1, MembersCur),

                    case leo_cluster_tbl_member:find_by_cluster_id(
                           ?MEMBER_TBL_PREV, ClusterId) of
                        {ok, MembersPrev} ->
                            Path_2 = lists:append([LogDir,
                                                   ?DUMP_FILE_MEMBERS_PREV,
                                                   atom_to_list(ClusterId),
                                                   ".",
                                                   integer_to_list(leo_date:now())]),
                            leo_file:file_unconsult(Path_2, MembersPrev);
                        not_found = Cause ->
                            {error, Cause};
                        Error ->
                            Error
                    end;
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State};

handle_call({join, Member},_From, State) ->
    Reply = join_1(Member#?MEMBER{state = ?STATE_ATTACHED}),
    {reply, Reply, State};

handle_call({reserve, Node, CurState, AwarenessL2,
             Clock, NumOfVNodes, RPCPort},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    leo_cluster_tbl_member:insert(
                      Member#?MEMBER{cluster_id = ClusterId,
                                     state = CurState});
                not_found ->
                    NodeStr = atom_to_list(Node),
                    IP = case (string:chr(NodeStr, $@) > 0) of
                             true ->
                                 lists:nth(2,string:tokens(NodeStr,"@"));
                             false ->
                                 []
                         end,
                    leo_cluster_tbl_member:insert(
                      #?MEMBER{cluster_id = ClusterId,
                               node = Node,
                               ip = IP,
                               clock = Clock,
                               state = CurState,
                               num_of_vnodes = NumOfVNodes,
                               grp_level_2 = AwarenessL2,
                               port = RPCPort});
                {error, Cause} ->
                    {error, Cause}
            end,
    {reply, Reply, State};


handle_call({leave, TblInfo, Node, Clock},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    leave_1(TblInfo, Member#?MEMBER{clock = Clock});
                Error ->
                    Error
            end,
    {reply, Reply, State};


handle_call({suspend, Node, _Clock},
            _From, #state{cluster_id = ClusterId} = State) ->
    Reply = case leo_cluster_tbl_member:lookup(ClusterId, Node) of
                {ok, Member} ->
                    case leo_cluster_tbl_member:insert(
                           Member#?MEMBER{cluster_id = ClusterId,
                                          state = ?STATE_SUSPEND}) of
                        ok ->
                            ok;
                        Error ->
                            Error
                    end;
                not_found = Cause ->
                    {error, Cause};
                Error ->
                    Error
            end,
    {reply, Reply, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
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
%%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Add a node into storage-cluster
%% @private
join_1(Member) ->
    {ok, Member_1} = ?alias(Member),
    case leo_cluster_tbl_member:insert(Member_1) of
        ok ->
            ok;
        Error ->
            Error
    end.


%% @doc Leave a node from storage-cluster
%% @private
leave_1(?RING_TBL_CUR, Member) ->
    Node = Member#?MEMBER.node,
    case leo_cluster_tbl_member:insert(
           Member#?MEMBER{node = Node,
                          clock = Member#?MEMBER.clock,
                          state = ?STATE_DETACHED}) of
        ok ->
            ok;
        Error ->
            Error
    end;
leave_1(?RING_TBL_PREV,_Member) ->
    ok.


%% @doc Retrieve members
%% @private
find_all_1(Ver, ClusterId) ->
    case leo_cluster_tbl_member:find_by_cluster_id(
           ?member_table(Ver), ClusterId) of
        {ok, Members} ->
            {ok, Members};
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.


%% @doc Update the member by node
%% @private
update_by_node_1(ClusterId, Node, NodeState) ->
    update_by_node_1(ClusterId, Node, -1, NodeState).

update_by_node_1(ClusterId, Node, Clock, NodeState) ->
    case leo_cluster_tbl_member:lookup(ClusterId, Node) of
        {ok, Member} ->
            Member_1 = case Clock of
                           -1 ->
                               Member#?MEMBER{state = NodeState};
                           _  ->
                               Member#?MEMBER{clock = Clock,
                                              state = NodeState}
                       end,
            case leo_cluster_tbl_member:insert(Member_1) of
                ok ->
                    ok;
                Error ->
                    Error
            end;
        not_found = Cause ->
            {error, Cause};
        Error ->
            Error
    end.
