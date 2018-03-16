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
%% @doc The konsul's gossip protocol
%% @reference https://github.com/leo-project/leo_konsul/blob/master/src/leo_konsul_gossip.erl
%% @end
%%======================================================================
-module(leo_konsul_gossip).

-behaviour(gen_server).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, stop/1]).
-export([send/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ifdef(TEST).
-export([merge/2]).
-endif.

-undef(DEF_TIMEOUT).

-ifdef(TEST).
-define(DEF_GOSSIP_MIN_INTERVAL, 250).
-define(DEF_GOSSIP_MAX_INTERVAL, 1000).
-define(DEF_TIMEOUT, timer:seconds(3)).
-else.
-define(DEF_GOSSIP_MIN_INTERVAL, 300).
-define(DEF_GOSSIP_MAX_INTERVAL, 1500).
-define(DEF_TIMEOUT, timer:seconds(30)).
-endif.


-record(state, {id :: atom(),
                cluster_id :: cluster_id(),
                issue_buckets = [] :: [],
                timestamp = 0 :: non_neg_integer()
               }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Start the server
start_link(ClusterId) ->
    Id = ?id_gossip(ClusterId),
    gen_server:start_link({local, Id}, ?MODULE, [Id, ClusterId], []).

%% @doc Stop the server
stop(ClusterId) ->
    Id = ?id_gossip(ClusterId),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%% @doc Sucscrive changing records of the member-tables
-spec(send(ClusterId, IssueL) ->
             ok | {error, any()} when ClusterId::cluster_id(),
                                      IssueL::[#issue_bucket{}]).
send(ClusterId, IssueL) ->
    Id = ?id_gossip(ClusterId),
    gen_server:call(Id, {send, IssueL}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%%--------------------------------------------------------------------
%% @doc Initiates the server
init([Id, ClusterId]) ->
    NewState = next(#state{id = Id,
                           cluster_id = ClusterId,
                           issue_buckets = [],
                           timestamp = 0}),
    {ok, NewState}.

%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From,State) ->
    {stop, normal, ok, State};

handle_call({send, IssueL}, _From, #state{cluster_id = ClusterId,
                                          issue_buckets = CurIssueL} = State) ->
    try
        %% Retrieves the hashes for sending them to a requesting node
        {ok, MembersHash} = leo_konsul_api:checksum(ClusterId, ?CHECKSUM_MEMBER),
        {ok, RingHash} = leo_konsul_api:checksum(ClusterId, ?CHECKSUM_RING),
        {ok, SysConfHash} = leo_konsul_api:checksum(ClusterId, ?CHECKSUM_SYS_CONF),

        %% Merge issue-list into the current one
        NewIssueBuckets = merge(CurIssueL, IssueL),

        {reply, {ok, #sharing_items{
                        member_hashes = MembersHash,
                        ring_hashes = RingHash,
                        conf_hash = SysConfHash,
                        issue_buckets = CurIssueL}}, State#state{issue_buckets = NewIssueBuckets}}
    catch
        _:_ ->
            {reply, {error, ?ERROR_COULD_NOT_GET_SOME_INFO}, State}
    end;
handle_call(_Handle, _From, State) ->
    {reply, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(next, State) ->
    case catch next(State) of
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
%% @doc Synchronize
%% @private
-spec(next(State) ->
             State when State::#state{}).
next(#state{timestamp = Timestamp} = State) ->
    ThisTime = ?timestamp(),
    State_1 =
        case (Timestamp =< 0) of
            true ->
                State;
            false ->
                case ((ThisTime - Timestamp) < ?DEF_GOSSIP_MIN_INTERVAL) of
                    true ->
                        State;
                    false ->
                        gossip(State)
                end
        end,

    %% Requests a next processing
    Time = erlang:phash2(term_to_binary(leo_date:clock()),
                         (?DEF_GOSSIP_MAX_INTERVAL - ?DEF_GOSSIP_MIN_INTERVAL)
                        ) + ?DEF_GOSSIP_MIN_INTERVAL,
    catch timer:apply_after(Time, gen_server, cast, [self(), next]),
    State_1#state{timestamp = ThisTime}.


%% @private
-spec(merge(IssueBuckets_1, IssueBuckets_2) ->
             [#issue_bucket{}] when IssueBuckets_1::[#issue_bucket{}],
                                    IssueBuckets_2::[#issue_bucket{}]).
merge([], []) ->
    [];
merge(IssueBuckets_1, []) ->
    IssueBuckets_1;
merge([], IssueBuckets_2) ->
    IssueBuckets_2;
merge(IssueBuckets_1, IssueBuckets_2) ->
    DictIssueBucket = merge_1(IssueBuckets_2,
                              merge_1(IssueBuckets_1, dict:new())),
    [#issue_bucket{node = Node,
                   issues = IssueL}
     || {Node, IssueL} <- dict:to_list(DictIssueBucket)].

%% @private
merge_1([], D) ->
    D;
merge_1([#issue_bucket{node = N,
                       issues = IL}|Rest], D) ->
    merge_1(Rest, merge_2(IL, N, D)).

%% @private
merge_2([],_,D) ->
    D;
merge_2([I|Rest], N, D) ->
    case dict:find(N, D) of
        {ok, IL} ->
            merge_2(Rest, N,
                    dict:store(
                      N, merge_3(IL, I, false, []), D));
        _ ->
            merge_2(Rest, N, dict:append(N, I, D))
    end.

%% @private
merge_3([],_I, true, Acc) ->
    Acc;
merge_3([], I, false, Acc) ->
    [I|Acc];
merge_3([#issue{item = Item,
                version = Ver,
                count = Count,
                created_at = CreatedAt,
                updated_at = UpdatedAt} = I|Rest],
        #issue{item = Item,
               version = Ver,
               count = Count_1,
               created_at = CreatedAt_1,
               updated_at = UpdatedAt_1} = I1, _, Acc) ->
    merge_3(Rest, I1, true,
            [I#issue{count = Count + Count_1,
                     created_at = lists:min([CreatedAt, CreatedAt_1]),
                     updated_at = lists:max([UpdatedAt, UpdatedAt_1])}|Acc]);
merge_3([I|Rest], I1, SoFar, Acc) ->
    merge_3(Rest, I1, SoFar, [I|Acc]).


%% @doc Executes gossip:
%%      - Check the consistency of the RING and the Members
%%      - Check the remote-nodes' health
%% @priavte
gossip(#state{cluster_id = ClusterId,
              issue_buckets = IssueL} = State) ->
    %% Retrieves redundancies to send a message (ring-hash, members-hash)
    %% as well as checking remote-node's health
    try
        AddrId = leo_konsul_chash:vnode_id(
                   128, crypto:strong_rand_bytes(64)),
        case leo_konsul_ring:lookup(ClusterId, ?RING_TBL_CUR, AddrId) of
            {ok, #redundancies{nodes = RedundantNodes}} ->
                {ok, RetIssueL} = gossip_1(RedundantNodes, ClusterId, IssueL, []),
                IssueL_1 = merge(IssueL, RetIssueL),

                %% Notify an issue-list to the leader-node
                %% to fix inconsistent items
                ok = notify(IssueL_1, dict:new()),

                State#state{issue_buckets = IssueL_1};
            _Error ->
                State
        end
    catch
        _:_ ->
            State
    end.

%% @private
gossip_1([],_,_,Acc) ->
    {ok, lists:sort(Acc)};
gossip_1([#redundant_node{node = Node}|Rest], ClusterId, OldIssueL, Acc) when Node == erlang:node() ->
    gossip_1(Rest, ClusterId, OldIssueL, Acc);
gossip_1([#redundant_node{node = Node}|Rest], ClusterId, OldIssueL, Acc) ->
    %% Send the current issue to a remote node,
    %% then compare member-hash, ring-hash, and conf-hash
    %% with a requesting node
    RPCRet = case catch rpc:call(
                          Node, ?MODULE, send,
                          [ClusterId, OldIssueL], ?DEF_TIMEOUT) of
                 {'EXIT', Reason} ->
                     {error, Reason};
                 Ret ->
                     Ret
             end,
    Acc_1 = gossip_2(RPCRet, Node, ClusterId, Acc),
    gossip_1(Rest, ClusterId, OldIssueL, Acc_1).

%% @private
gossip_2({ok, #sharing_items{
                 member_hashes = {MembersHCur_R, MembersHPrev_R},
                 ring_hashes = {RingHCur_R, RingHPrev_R},
                 conf_hash = ConfH_R,
                 issue_buckets = IssueBucketL}}, Node, ClusterId, Acc) ->

    {ok, {MembersHCur_L, MembersHPrev_L}} =
        leo_konsul_api:checksum(ClusterId, ?CHECKSUM_MEMBER),
    {ok, {RingHCur_L, RingHPrev_L}} =
        leo_konsul_api:checksum(ClusterId, ?CHECKSUM_RING),
    {ok, ConfH_L} = leo_konsul_api:checksum(ClusterId, ?CHECKSUM_SYS_CONF),

    Now = leo_date:clock(),
    NewIssueL =
        lists:foldl(
          fun({ItemName, Ver, HashRemote, HashLocal}, Acc_1) ->
                  case (HashRemote == HashLocal) of
                      true ->
                          Acc_1;
                      false ->
                          [#issue{item = ItemName,
                                  count = 1,
                                  version = Ver,
                                  created_at = Now,
                                  updated_at = Now} | Acc_1]
                  end
          end, [],
          [{?ITEM_MEMBER, ?VER_CUR,  MembersHCur_R, MembersHCur_L},
           {?ITEM_MEMBER, ?VER_PREV, MembersHPrev_R, MembersHPrev_L},
           {?ITEM_RING,   ?VER_CUR,  RingHCur_R, RingHCur_L},
           {?ITEM_RING,   ?VER_PREV, RingHPrev_R, RingHPrev_L},
           {?ITEM_CONF,   ?VER_CUR,  ConfH_R, ConfH_L}
          ]),
    merge(
      merge(Acc, IssueBucketL),
      [#issue_bucket{node = Node,
                     issues = NewIssueL}]);
gossip_2({badrpc, nodedown}, Node,_ClusterId, Acc) ->
    Now = leo_date:clock(),
    merge(Acc,
          [#issue_bucket{node = Node,
                         issues = [#issue{
                                      item = ?ITEM_NODEDOWN,
                                      count = 1,
                                      version = ?VER_CUR,
                                      created_at = Now,
                                      updated_at = Now}]}]);
gossip_2(_,_,_,Acc) ->
    Acc.


%% @doc Notify a messeage to a remote node
%% @private
notify([], DictMsgByNode) ->
    case dict:to_list(DictMsgByNode) of
        [] ->
            ok;
        MsgL ->
            MsgL_1 = lists:sort(MsgL),
            %% @TODO (2018-03-15)
            %% ?debugVal(MsgL_1),
            ok
    end;
notify([#issue_bucket{node = Node,
                      issues = IL}|Rest], DictMsgByNode) ->
    DictMsgByNode_1 = notify_1(IL, Node, DictMsgByNode),
    notify(Rest, DictMsgByNode_1).

%% @private
notify_1([],_, DictMsgByNode) ->
    DictMsgByNode;
notify_1([#issue{count = C,
                 created_at = CreatedAt,
                 updated_at = UpdatedAt} = I|Rest], Node, DictMsgByNode) ->
    DictMsgByNode_1 =
        case (C >= ?env_threshold_error_count()) of
            true when (UpdatedAt - CreatedAt) > 0 ->
                case (erlang:round((UpdatedAt - CreatedAt) / 1000)
                      >= ?env_threshold_elapsed_time()) of
                    true ->
                        dict:append(Node, I, DictMsgByNode);
                    false ->
                        DictMsgByNode
                end;
            _ ->
                DictMsgByNode
        end,
    notify_1(Rest, Node, DictMsgByNode_1).



%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

merge_test() ->
    CurIssueL = [#issue_bucket{
                    node = 'node_1@127.0.0.1',
                    issues = [#issue{item = ?ITEM_MEMBER,
                                     version = ?VER_CUR,
                                     count = 1,
                                     created_at = 1520502850533854,
                                     updated_at = 1520502850533854}]
                   },
                 #issue_bucket{
                    node = 'node_2@127.0.0.1',
                    issues = [#issue{item = ?ITEM_RING,
                                     version = ?VER_CUR,
                                     count = 2,
                                     created_at = 1520502850533854,
                                     updated_at = 1520502850533854}]
                   }
                ],
    OldIssueL = [#issue_bucket{
                    node = 'node_2@127.0.0.1',
                    issues = [#issue{item = ?ITEM_MEMBER,
                                     version = ?VER_CUR,
                                     count = 3,
                                     created_at = 1520502850533000,
                                     updated_at = 1520502850533854},
                              #issue{item = ?ITEM_RING,
                                     version = ?VER_CUR,
                                     count = 4,
                                     created_at = 1520502850533072,
                                     updated_at = 1520502850533072}
                             ]
                   },
                 #issue_bucket{
                    node = 'node_3@127.0.0.1',
                    issues = [#issue{item = ?ITEM_RING,
                                     version = ?VER_PREV,
                                     count = 2,
                                     created_at = 1520502850530000,
                                     updated_at = 1520502850530000}]
                   }
                ],
    NewIssueBuckets = merge(CurIssueL, OldIssueL),
    [begin
         #issue_bucket{node = Node,
                       issues = IssueL} = IssueB,
         case Node  of
             'node_1@127.0.0.1' ->
                 ?assertEqual(1, length(IssueL)),
                 #issue{item = ?ITEM_MEMBER,
                        version = ?VER_CUR,
                        count = 1,
                        created_at = 1520502850533854,
                        updated_at = 1520502850533854} = hd(IssueL);
             'node_2@127.0.0.1' ->
                 ?assertEqual(2, length(IssueL)),
                 [begin
                      case I of
                          ?ITEM_MEMBER when V == ?VER_CUR ->
                              ?assertEqual(3, C),
                              ?assertEqual(1520502850533000, CreatedAt),
                              ?assertEqual(1520502850533854, UpdatedAt);
                          ?ITEM_RING when V == ?VER_CUR ->
                              ?assertEqual(6, C),
                              ?assertEqual(1520502850533072, CreatedAt),
                              ?assertEqual(1520502850533854, UpdatedAt);
                          _ ->
                              erlang:error("unexected error")
                      end
                  end || #issue{item = I,
                                version = V,
                                count = C,
                                created_at = CreatedAt,
                                updated_at = UpdatedAt} <- IssueL];
             'node_3@127.0.0.1' ->
                 ?assertEqual(1, length(IssueL)),
                 #issue{item = ?ITEM_RING,
                        version = ?VER_PREV,
                        count = 2,
                        created_at = 1520502850530000,
                        updated_at = 1520502850530000} = hd(IssueL)
         end
     end || IssueB  <- NewIssueBuckets],
    ?assertEqual(3, length(NewIssueBuckets)),
    ok.

-endif.
