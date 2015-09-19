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
%% @doc The coordinator for a given Clock SI general tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(general_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(PARTITION_VNODE, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(LOG_UTIL, log_utilities).
-define(PARTITION_VNODE, partition_vnode).
-endif.


%% API
-export([start_link/3, start_link/2]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([
         receive_reply/2,
         single_committing/2,
         execute_batch_ops/2,
         perform_singleitem_read/1,
         reply_to_client/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------
-record(state, {
	  from :: {pid(), term()},
	  tx_id :: txid(),
      operations :: [],
	  num_to_ack :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
      updated_partitions :: dict(),
      read_set :: [],
      causal_clock :: non_neg_integer(),
	  state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, 0, Operations], []).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

-spec perform_singleitem_read(key()) -> {ok,val()} | {error,reason()}.
perform_singleitem_read(Key) ->
    TxId = tx_utilities:create_transaction_record(0),
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case ?PARTITION_VNODE:read_data_item(IndexNode, Key, TxId) of
    error ->
        {error, unknown};
    {error, Reason} ->
        {error, Reason};
    {ok, Snapshot} ->
        {ok, Snapshot}
    end.

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    %lager:info("Initing"),
    random:seed(now()),
    SD = #state{
            causal_clock = ClientClock,
            operations = Operations,
            updated_partitions = dict:new(),
            read_set = [],
            from = From,
            prepare_time=0
           },
    {ok, execute_batch_ops, SD, 0}.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_batch_ops(timeout, SD=#state{causal_clock=CausalClock,
                    operations=Operations}) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    ProcessOp = fun(Operation, {UpdatedParts, RSet, Buffer}) ->
                    case Operation of
                        {read, Key} ->
                            {ok, Snapshot} = case dict:find(Key, Buffer) of
                                                    error ->
                                                        Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                                                        IndexNode = hd(Preflist),
                                                        ?PARTITION_VNODE:read_data_item(IndexNode, Key, TxId);
                                                    {ok, SnapshotState} ->
                                                        {ok, SnapshotState}
                                                    end,
                            Buffer1 = dict:store(Key, Snapshot, Buffer),
                            {UpdatedParts, [Snapshot|RSet], Buffer1};
                        {update, Key, Op, Param} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            UpdatedParts1 = case dict:is_key(IndexNode, UpdatedParts) of
                                                false ->
                                                    dict:store(IndexNode, [{Key, Op, Param}], UpdatedParts);
                                                true ->
                                                    dict:append(IndexNode, {Key, Op, Param}, UpdatedParts)
                                            end,
                            Buffer1 = case dict:find(Key, Buffer) of
                                        error ->
                                            NewSnapshot = update_object:update(Op, Param),
                                            dict:store(Key, NewSnapshot, Buffer);
                                        {ok, Snapshot} ->
                                            NewSnapshot = update_object:update(Snapshot, Op, Param),
                                            dict:store(Key, NewSnapshot, Buffer)
                                        end,
                            {UpdatedParts1, RSet, Buffer1}
                    end
                end,
    {WriteSet1, ReadSet1, _} = lists:foldl(ProcessOp, {dict:new(), [], dict:new()}, Operations),
    case dict:size(WriteSet1) of
        0->
            reply_to_client(SD#state{state=committed, tx_id=TxId, read_set=ReadSet1, 
                prepare_time=clock_service:now_microsec()});
        1->
            UpdatedPart = dict:to_list(WriteSet1),
            ?PARTITION_VNODE:single_commit(UpdatedPart, TxId),
            {next_state, single_committing,
            SD#state{state=committing, num_to_ack=1, read_set=ReadSet1, tx_id=TxId}};
        N->
            ?PARTITION_VNODE:prepare(WriteSet1, TxId),
            {next_state, receive_reply, SD#state{num_to_ack=N, state=prepared,
                 updated_partitions=WriteSet1, read_set=ReadSet1, tx_id=TxId}}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_reply({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck, tx_id=TxId, updated_partitions=UpdatedPartitions,
                            prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 
        1 ->
            ?PARTITION_VNODE:commit(UpdatedPartitions, TxId, MaxPrepareTime),
            reply_to_client(S0#state{state=committed, prepare_time=MaxPrepareTime});
        _ ->
            {next_state, receive_reply,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_reply(abort, S0) ->
    {next_state, receive_reply, S0};

receive_reply(timeout, S0=#state{tx_id=TxId, updated_partitions=UpdatedPartitions}) ->
    ?PARTITION_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(S0#state{state=aborted}).

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{prepare_time=CommitTime, state=committed});
    
single_committing(abort, S0=#state{from=_From, tx_id=TxId, updated_partitions=UpdatedPartitions}) ->
    ?PARTITION_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(S0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
reply_to_client(SD=#state{from=From, tx_id=TxId, state=TxState, read_set=ReadSet, prepare_time=CommitTime}) ->
    case TxState of
        committed ->
            From ! {ok, {TxId, ReadSet, CommitTime}},
            {stop, normal, SD};
        aborted ->
            From ! {error, commit_fail},
            {stop, normal, SD}
    end.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    {stop,normal,ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

