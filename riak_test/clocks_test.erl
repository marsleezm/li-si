%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(clocks_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clocks_test1(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocks_test1(Nodes) ->
    lager:info("Test1 started"),

    K1=key1, K2=key2, K3=key3,

    {ok, {_, L0, _}}=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
        [[{read, K1}, {read, K2}, {read, K3}]]),
    lager:info("L0 ~p", [L0]),

    % Read and update
    ResultRead1=rpc:call(hd(Nodes), antidote, execute_tx,
                    [[%{read, K1},
                      {update, K2, increment, 1},
                      {update, K2, increment, 1}]]),
    {ok, {_, ReadSet1, _}}=ResultRead1,
    lager:info("ReadSet1 ~p", [ReadSet1]),

    {ok, {_, L1, _}}=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
      [[{read, K1}, {read, K2}, {read, K3}]]),
    lager:info("L1 ~p", [L1]),

    % Single update
    ResultRead2=rpc:call(hd(Nodes), antidote, execute_tx,
        [[{update, K2, increment, 1}]]),
    {ok, {_, ReadSet2, _}}=ResultRead2,
    lager:info("ReadSet2 ~p", [ReadSet2]),

    {ok, {_, L2, _}}=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
      [[{read, K1}, {read, K2}, {read, K3}]]),
    lager:info("L2~p", [L2]),

    % Update only
    ResultRead3=rpc:call(hd(Nodes), antidote, execute_tx,
                    [[{update, K1, increment, 1},
                        {update, K2, increment, 1},
                        {update, K3, increment, 1}]]),
    {ok, {_, ReadSet3, _}}=ResultRead3,
    lager:info("ReadSet3 ~p", [ReadSet3]),

    {ok, {_, L3, _}}=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
      [[{read, K1}, {read, K2}, {read, K3}]]),
    lager:info("L3~p", [L3]),

    rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
      [[{update, K3, increment, 2}, {read, K2}]]),

    % Update only
    ResultRead4=rpc:call(lists:nth(3, Nodes), antidote, execute_tx,
      [[{read, K3},
        {update, K1, increment, 1},
        {read, K1},
        {update, K3, increment, 1}]]),
    {ok, {_, ReadSet4, _}}=ResultRead4,
    lager:info("ReadSet4 ~p", [ReadSet4]),

    pass.
