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
-module(tx_utilities).

-include("antidote.hrl").

-ifdef(TEST).
-define(GET_AND_UPDATE_TS(Clock), partition_vnode:now_microsec()).
-else.
-define(GET_AND_UPDATE_TS(Clock), tx_utilities:get_and_update_ts(Clock)).
-endif.

-export([create_transaction_record/1, get_and_update_ts/1]).

-spec create_transaction_record(snapshot_time() | ignore) -> txid().
create_transaction_record(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    {A1,A2,A3} = now(),
    _A = ClientClock,
    _ = random:seed(A1, A2, A3),
    #tx_id{snapshot_time=?GET_AND_UPDATE_TS(ClientClock), server_pid=self()}.
    %lager:info("TxId is ~w",[TransactionId#tx_id.snapshot_time]),
    %TransactionId.
    
get_and_update_ts(ClientClock) ->
    LocalNode = case ets:lookup(meta_info, local_nodes) of
                    [] ->
                        L = hash_fun:get_local_nodes(),
                        true = ets:insert(meta_info, {local_nodes, L}),
                        lists:nth(random:uniform(length(L)),L);
                    [{local_nodes, L}] ->
                        lists:nth(random:uniform(length(L)),L)
                end,
    %lager:info("LocalNode is ~w", [LocalNode]),
    partition_vnode:get_and_update_ts(LocalNode, ClientClock).
