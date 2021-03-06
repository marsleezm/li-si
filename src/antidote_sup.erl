%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% -------------------------------------------------------------------
-module(antidote_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
    
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    PartitionMaster = { partition_vnode_master,
                      {riak_core_vnode_master, start_link, [partition_vnode]},
                      permanent, 5000, worker, [riak_core_vnode_master]},

    GeneralTxCoordSup =  { general_tx_coord_sup,
                           {general_tx_coord_sup, start_link, []},
                           permanent, 5000, supervisor, [general_tx_coord_sup]},
    
    ReplFsmSup = {repl_fsm_sup,
    		      {repl_fsm_sup, start_link, []},
    		      permanent, 5000, supervisor,
    		      [repl_fsm_sup]},

%    ClockService = {clock_service,
%                 {clock_service,  start_link,
%                  []},
%                 permanent, 5000, worker, [clock_service]},

    antidote_config:load("antidote.config"),

    ets:new(meta_info,
        [set,public,named_table,{read_concurrency,true},{write_concurrency,false}]),

    {ok,
     {{one_for_one, 5, 10},
      [PartitionMaster,
       GeneralTxCoordSup,
       ReplFsmSup]}}.
 %      ClockService]}}.
