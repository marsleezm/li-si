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

%%    rpc:call(hd(Nodes), antidote, execute_tx,
%%                    [
%%                     [{update, key1, increment, 1},
%%                      {update, key2, increment, 1},
%%                      {update, key3, increment, 1}]]),
%%
%%    rpc:call(hd(Nodes), antidote, execute_tx,
%%      [
%%        [ {update, key2, increment, 1},
%%          {update, key3, increment, 1}]]),
%%
%%
%%    rpc:call(hd(Nodes), antidote, execute_tx,
%%      [
%%        [ {update, key2, increment, 1},
%%          {update, key3, increment, 1}]]),
%%
%%    rpc:call(hd(Nodes), antidote, execute_tx,
%%      [
%%        [ {update, key2, increment, 1},
%%          {update, key3, increment, 1}]]),

    ResultRead=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
                    [
                     [{read, key1},
                      {read, key2},
                      {read, key3}]]),
   {ok, {_, ReadSet, _}}=ResultRead,
    lager:info("ReadSet ~p", [ReadSet]),

    rpc:call(hd(Nodes), antidote, execute_tx,
          [
            [ {update, key2, increment, 1},
              {update, key3, increment, 1}]]),

    ResultRead1=rpc:call(lists:nth(2, Nodes), antidote, execute_tx,
    [
      [{read, key1},
        {read, key2},
        {read, key3}]]),
    {ok, {_, ReadSet1, _}}=ResultRead1,
    lager:info("ReadSet ~p", [ReadSet1]),

    pass.
