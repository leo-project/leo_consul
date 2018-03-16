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
%% Leo konsul - Supervisor
%% @doc
%% @end
%%======================================================================
-module(leo_konsul_sup).

-behaviour(supervisor).

-include("leo_konsul.hrl").
-include_lib("eunit/include/eunit.hrl").

%% External API
-export([start_link/0,
         stop/0]).
%% Callbacks
-export([init/1]).
%% Others
-export([start_child/2]).


-ifdef(TEST).
%% -define(MNESIA_TYPE_COPIES, 'ram_copies').
-define(MODULE_SET_ENV_1(), application:set_env(?APP, ?PROP_NOTIFY_MF, [leo_manager_api, notify])).
-define(MODULE_SET_ENV_2(), application:set_env(?APP, ?PROP_SYNC_MF,   [leo_manager_api, synchronize])).
-else.
%% -define(MNESIA_TYPE_COPIES, 'disc_copies').
-define(MODULE_SET_ENV_1(), void).
-define(MODULE_SET_ENV_2(), void).
-endif.

-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART, 5).
-define(MAX_TIME, 60).


%%-----------------------------------------------------------------------
%% External API
%%-----------------------------------------------------------------------
%% @doc Launch and link the process
-spec(start_link() ->
             {ok, RefSup} | {error, Cause} when RefSup::pid(),
                                                Cause::any()).
start_link() ->
    case supervisor:start_link({local, ?MODULE}, ?MODULE, []) of
        {ok, RefSup} ->
            %% Initialize environment vars
            ok = leo_misc:init_env(),
            ?MODULE_SET_ENV_1(),
            ?MODULE_SET_ENV_2(),
            {ok, RefSup};
        {error, {already_started, RefSup}} ->
            {ok, RefSup};
        Other ->
            Other
    end.


%% @doc Launch the process
-spec(start_child(ClusgerId, Options) ->
             {ok, RefSup} | {error, Cause} when ClusgerId::cluster_id(),
                                                Options::[{option_item_key(), any()}],
                                                RefSup::pid(),
                                                Cause::any()).
start_child(ClusterId, Options) ->
    SystemConf = leo_misc:get_value(?ITEM_KEY_SYSTEM_CONF, Options),

    %% Launch membership for local-cluster,
    %% then lunch mdc-tables sync
    SystemConf_1 = #?SYSTEM_CONF{
                       cluster_id = ClusterId,
                       n = leo_misc:get_value(?PROP_N, SystemConf, 0),
                       r = leo_misc:get_value(?PROP_R, SystemConf, 0),
                       w = leo_misc:get_value(?PROP_W, SystemConf, 0),
                       d = leo_misc:get_value(?PROP_D, SystemConf, 0),
                       num_of_rack_replicas = leo_misc:get_value(?PROP_RACK_N, SystemConf, 0),
                       bit_of_ring = leo_misc:get_value(?PROP_RING_BIT, SystemConf, 0),
                       num_of_dc_replicas = leo_misc:get_value(?PROP_DC_N, SystemConf, 0),
                       mdcr_r = leo_misc:get_value(?PROP_MDCR_R, SystemConf, 0),
                       mdcr_w = leo_misc:get_value(?PROP_MDCR_W, SystemConf, 0),
                       mdcr_d = leo_misc:get_value(?PROP_MDCR_D, SystemConf, 0),
                       max_mdc_targets = leo_misc:get_value(?PROP_MAX_DC_TARGETS, SystemConf, 0)
                      },
    %% @TODO: leo_cluster_tbl_conf:update(SystemConf_1)
    case start_child_1(ClusterId, SystemConf_1) of
        ok ->
            %% @TODO Set environment vars (2018-02-28)
            ok = leo_konsul_api:set_options(ClusterId, SystemConf),
            ok;
        Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "start_link/2"},
                                    {line, ?LINE}, {body, Cause}]),
            case leo_konsul_sup:stop() of
                ok ->
                    exit(invalid_launch);
                not_started ->
                    exit(noproc)
            end
    end.


%% @private
start_child_1(ClusterId, SystemConf) ->
    ok = init_tables(),
    Specs = [
             {?id_member(ClusterId),
              {leo_konsul_member, start_link, [ClusterId]},
              permanent,
              ?SHUTDOWN_WAITING_TIME, worker,
              [leo_konsul_member]},

             {?id_ring(ClusterId),
              {leo_konsul_ring, start_link, [ClusterId, SystemConf]},
              permanent,
              ?SHUTDOWN_WAITING_TIME, worker,
              [leo_konsul_ring]},

             {?id_gossip(ClusterId),
              {leo_konsul_gossip, start_link, [ClusterId]},
              permanent,
              ?SHUTDOWN_WAITING_TIME, worker,
              [leo_konsul_gossip]}
            ],
    start_child_2(Specs).

%% @private
start_child_2([]) ->
    ok;
start_child_2([Spec|Rest]) ->
    case supervisor:start_child(leo_konsul_sup, Spec) of
        {ok,_} ->
            start_child_2(Rest);
        Error ->
            Error
    end.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    ok.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    {ok, {_SupFlags = {one_for_one, 5, 60}, []}}.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc Create members table.
%% @private
-ifdef(TEST).
init_tables()  ->
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    ok.
-else.

init_tables() ->
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_CUR),
    catch leo_cluster_tbl_member:create_table(?MEMBER_TBL_PREV),
    ok.
-endif.
