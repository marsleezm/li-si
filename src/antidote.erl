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
-module(antidote).

-include("antidote.hrl").

-export([update/3,
         read/1,
         print_stat/0,
         execute_tx/2,
         execute_tx/3]).

%% Public API

%% @doc The append/2 function adds an operation to the log of the CRDT
%%      object stored at some key.
-spec update(Key::key(), Type::type(), {term(),term()}) -> {ok, term()} | {error, reason()}.
update(Key, Op, Param) ->
    Operations = [{update, Key, Op, Param}],
    case execute_tx(ignore, Operations) of
        {ok, Result} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The read/2 function returns the current value for the CRDT
%%      object stored at some key.
-spec read(Key::key()) -> {ok, val()} | {error, reason()}.
read(Key) ->
    general_tx_coord_fsm:perform_singleitem_read(Key).

print_stat() ->
    Partitions = hash_fun:get_partitions(),
    partition_vnode:print_stat(Partitions).

%% Clock SI API

%% @doc Starts a new ClockSI transaction.
%%      Input:
%%      ClientClock: last clock the client has seen from a successful transaction.
%%      Operations: the list of the operations the transaction involves.
%%      Returns:
%%      an ok message along with the result of the read operations involved in the
%%      the transaction, in case the tx ends successfully.
%%      error message in case of a failure.
%%
-spec execute_tx(Clock :: snapshot_time(), StartPartId :: non_neg_integer(),
                         Operations::[any()]) -> term().
execute_tx(Clock, StartPartId, Operations) ->
    %lager:warning("Received op, start id is ~w", [StartPartId]),
    {ok, _} = general_tx_coord_sup:start_fsm([self(), Clock, StartPartId, Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.

-spec execute_tx(StartPartId::non_neg_integer(), Operations::[any()]) -> term().
execute_tx(StartPartId, Operations) ->
    %lager:warning("Received op, start id is ~w", [StartPartId]),
    {ok, _} = general_tx_coord_sup:start_fsm([self(), StartPartId, Operations]),
    receive
        EndOfTx ->
            EndOfTx
    end.
