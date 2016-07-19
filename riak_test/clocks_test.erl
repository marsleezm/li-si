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
    clocks_single_key_update_read_test(Nodes),
    clocks_multiple_key_update_read_test(Nodes),
    clocks_multiple_read_update_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% @doc The following function tests that ClockSI can run a non-interactive tx
%%      that updates multiple partitions.
clocks_test1(Nodes) ->
    lager:info("Test1 started"),
    FirstNode = hd(Nodes),

    Result0=rpc:call(FirstNode, antidote, execute_tx,
                    [[]]),
    ?assertMatch({ok, _}, Result0),
    lager:info("Empty transaction works"),

    Result11=rpc:call(FirstNode, antidote, execute_tx,
                    [
                     [{read, key1}]]),
    ?assertMatch({ok, _}, Result11),
    {ok, {_, ReadSet11, _}}=Result11,
    lager:info("Result11 is ~w", [Result11]),
    ?assertMatch([nil], ReadSet11),
    lager:info("A simple read returns empty works"),

    Result2=rpc:call(FirstNode, antidote, execute_tx,
                    [
                      [{read, key1},
                      {update, key1, increment, 1},
                      {update, key2, increment, 2},
                      {read, key1}]]),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, ReadSet2, _}}=Result2, 
    ?assertMatch([nil,1], ReadSet2),
    lager:info("Read what you wrote works"),

    Result3=rpc:call(FirstNode, antidote, execute_tx,
                    [
                     [{read, key1},
                      {read, key2}]]),
    ?assertMatch({ok, _}, Result3),
    {ok, {_, ReadSet3, _}}=Result3,
    ?assertEqual([1,2], ReadSet3),
    lager:info("Update is persisted && update to multiple keys are atomic works"),

    Result5=rpc:call(FirstNode, antidote, execute_tx,
                    [
                     [{update, key1, increment, 1},
                      {update, key1, increment, 1}]]),
    ?assertMatch({ok,_}, Result5),
    lager:info("Multiple updates to a key in a transaction works"),

    Result6=rpc:call(FirstNode, antidote, execute_tx,
                    [
                     [{read, key1}]]),
    {ok, {_, ReadSet6, _}}=Result6,
    ?assertEqual(3, hd(ReadSet6)),
    lager:info("Test1 passed"),
    pass.
    
%% @doc The following function tests that ClockSI can run both a single
%%      read and a bulk-update tx.
clocks_single_key_update_read_test(Nodes) ->
    lager:info("Test2 started"),
    FirstNode = hd(Nodes),
    Key = k3,
    Result= rpc:call(FirstNode, antidote, execute_tx,
                     [
                      [{update, Key, increment, 1},
                       {update, Key, increment, 1}]]),
    ?assertMatch({ok, _}, Result),
    {ok,{_,_,CommitTime}} = Result,
    lager:info("Commit Time is ~w", [CommitTime]),
    {ok, Result2}= rpc:call(FirstNode, antidote, read,
                      [Key]),
    ?assertMatch(2, Result2),
    lager:info("Test2 passed"),
    pass.

%% @doc Verify that multiple reads/writes are successful.
clocks_multiple_key_update_read_test(Nodes) ->
    lager:info("Test3 started"),
    Firstnode = hd(Nodes),
    Key1 = keym1,
    Key2 = keym2,
    Key3 = keym3,
    Ops = [{update,Key1, increment,1},
           {update,Key2, increment,10},
           {update,Key3, increment,1}],
    Writeresult = rpc:call(Firstnode, antidote, execute_tx,
                           [Ops]),
    ?assertMatch({ok,{_Txid, _Readset, _Committime}}, Writeresult),
    {ok,{_Txid, _Readset, _Committime}} = Writeresult,
    {ok, ReadResult1} = rpc:call(Firstnode, antidote, read,
                                        [Key1]),
    {ok, ReadResult2} = rpc:call(Firstnode, antidote, read,
                                        [Key2]),
    {ok, ReadResult3} = rpc:call(Firstnode, antidote, read,
                                        [Key3]),
    ?assertMatch(ReadResult1,1),
    ?assertMatch(ReadResult2,10),
    ?assertMatch(ReadResult3,1),
    lager:info("Test3 passed"),
    pass.

%% @doc Read an update a key multiple times.
clocks_multiple_read_update_test(Nodes) ->
    lager:info("Test4 started"),
    Node = hd(Nodes),
    Key = get_random_key(),
    NTimes = 100,
    {ok,_} = rpc:call(Node, antidote, read,
                       [Key]),
    lists:foreach(fun(_)->
                          read_update_test(Node, Key) end,
                  lists:seq(1,NTimes)),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key]),

    ?assertEqual(NTimes, Result2),
    lager:info("Test4 passed"),
    pass.

%% @doc Test updating prior to a read.
read_update_test(Node, Key) ->
    {ok,Result1} = rpc:call(Node, antidote, read,
                       [Key]),
    {ok,_} = rpc:call(Node, antidote, execute_tx,
                      [[{update, Key, increment, 1}]]),
    {ok,Result2} = rpc:call(Node, antidote, read,
                       [Key]),
    case Result1 of
      nil ->
        ?assertEqual(1,Result2);
      _ ->
        ?assertEqual(Result1+1,Result2)
    end,
    pass.

get_random_key() ->
    random:seed(now()),
    random:uniform(1000).
