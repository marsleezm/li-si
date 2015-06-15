
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
%% @doc The coordinator for a given Clock SI static transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted), the fsm
%%      also finishes.

-module(clocksi_static_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
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
         execute_batch_ops/2,
         receive_prepared/2,
         single_committing/2,
         committing/2,
         receive_committed/2,
         abort/2,
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
	  from :: pid(),
	  transaction :: tx(),
      operations ::  [{read, key(), type()}| {update, key(), type(), term()}],
	  num_to_ack :: non_neg_integer(),
	  num_to_read :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
	  commit_time :: non_neg_integer(),
      updated_partitions :: dict(),
      read_set :: [term()],
	  state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    {Transaction,_TransactionId} = create_transaction_record(ClientClock),
    SD = #state{
            transaction = Transaction,
            read_set = [],
            prepare_time = 0,
            num_to_read=0,
            num_to_ack=0,
            operations = Operations,
            updated_partitions = dict:new(),
            from = From
           },
    {ok, execute_batch_ops, SD, 0}.

-spec create_transaction_record(snapshot_time() | ignore) -> {tx(),txid()}.
create_transaction_record(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    {A1,A2,A3} = now(),
    _ = random:seed(A1, A2, A3),
    {ok, SnapshotTime} = case ClientClock of
        ignore ->
            get_snapshot_time();
        _ ->
            get_snapshot_time(ClientClock)
    end,
    DcId = ?DC_UTIL:get_my_dc_id(),
    {ok, LocalClock} = ?VECTORCLOCK:get_clock_of_dc(DcId, SnapshotTime),
    TransactionId = #tx_id{snapshot_time=LocalClock, server_pid=self()},
    Transaction = #transaction{snapshot_time=LocalClock,
                               vec_snapshot_time=SnapshotTime,
                               txn_id=TransactionId},
    {Transaction,TransactionId}.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%      to execute the next operation.
execute_batch_ops(timeout, SD=#state{
                                     transaction = Transaction,
                                     operations = Operations}) ->
    ProcessOp = fun(Operation, {UpdatedPartitions, NumToRead}) ->
                    case Operation of
                        {read, Key, Type} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            lager:info("NumToRead count: ~w",[NumToRead+1]),
                            ok = ?CLOCKSI_VNODE:async_read_data_item(IndexNode, Key, Type, Transaction),
                            {UpdatedPartitions, NumToRead+1};
                        {update, Key, Type, Op} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            UpdatedPartitions1 = case dict:is_key(IndexNode, UpdatedPartitions) of
                                                    false ->
                                                        dict:store(IndexNode, [{Key, Type, Op}], 
                                                            UpdatedPartitions);
                                                    true ->
                                                        dict:append(IndexNode, {Key, Type, Op}, 
                                                            UpdatedPartitions)
                                                end,
                            {UpdatedPartitions1, NumToRead}
                    end
                end,
    {WriteSet, NumOfReads} = lists:foldl(ProcessOp, {dict:new(),0}, Operations),
    lager:info("Operations are ~w, WriteSet is ~w, NumOfReads ~w",[Operations, WriteSet, NumOfReads]),
    case dict:size(WriteSet) of
        0->
            case NumOfReads of
                0 ->
                    reply_to_client(SD#state{state=committed, 
                            commit_time=clocksi_vnode:now_microsec(erlang:now())});
                _ ->
                    lager:info("Waiting for ~w reads to reply", [NumOfReads]),
                    Snapshot_time=Transaction#transaction.snapshot_time,
                    {next_state, single_committing, SD#state{state=committing, num_to_ack=0, 
                        commit_time=Snapshot_time, num_to_read=NumOfReads}}
            end;
        1->
            UpdatedPart = dict:to_list(WriteSet),
            %lists:foldl(fun(X, Acc) -> {Part, K:eys}= X,
            %                            Acc++[{Part, [Key || {Key, _Type, _Op} <- Keys]} ]
            %            end, [], UpdatedPart),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, Transaction),
            {next_state, single_committing,
            SD#state{state=committing, num_to_ack=1, num_to_read=NumOfReads}};
        N->
            ?CLOCKSI_VNODE:prepare(WriteSet, Transaction),
            {next_state, receive_prepared, SD#state{num_to_ack=N, state=prepared, 
                num_to_read=NumOfReads, updated_partitions=WriteSet}}
    end.
    %{next_state, prepare, SD#state{read_set=ReadSet, updated_partitions=UpdatedPartitions}, 0}

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                           num_to_read=NumToRead,
                           prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 
        1 ->
            case NumToRead of
                0 ->
                    {next_state, committing, S0#state{prepare_time=MaxPrepareTime, 
                        commit_time=MaxPrepareTime, state=committing}, 0};
                _ ->
                    {next_state, receive_prepared, S0#state{num_to_ack= NumToAck-1, 
                        commit_time=MaxPrepareTime, prepare_time=MaxPrepareTime}}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared({ok, {Type,Snapshot}},
                 S0=#state{num_to_read=NumToRead,
                            read_set=ReadSet,
                            num_to_ack=NumToAck}) ->
    ReadSet1 = ReadSet ++ [Type:value(Snapshot)],
    case NumToRead of 
        1 ->
            case NumToAck of
                0 ->
                    {next_state, committing, S0#state{state=committing, read_set=ReadSet1}, 0};
                _ ->
                    {next_state, receive_prepared, S0#state{num_to_read= NumToRead-1,
                            read_set=ReadSet1}}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{read_set=ReadSet1, num_to_read = NumToRead-1}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing({ok, {Type, Snapshot}},
                 S0=#state{num_to_read=NumToRead,
                            read_set=ReadSet,
                            num_to_ack=NumToAck}) ->
    lager:info("Got some replies ~w", [Type:value(Snapshot)]),
    ReadSet1 = ReadSet ++ [Type:value(Snapshot)],
    case NumToRead of 
        1 ->
            case NumToAck of
                0 -> 
                    reply_to_client(S0#state{read_set=ReadSet1, state=committed});
                1 ->
                    {next_state, single_committing, S0#state{num_to_read=NumToRead-1, 
                            read_set=ReadSet1}}
            end;
        _ ->
            {next_state, single_committing, S0#state{num_to_read=NumToRead-1, read_set=ReadSet1}}
    end;

single_committing({committed, CommitTime}, S0=#state{from=_From, num_to_read=NumToRead}) ->
    case NumToRead of 
        0 ->
            reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
        _ ->
            {next_state, single_committing, S0#state{num_to_ack=0, prepare_time=CommitTime,
                        commit_time=CommitTime}}
    end;
    
single_committing(aborted, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, SD0=#state{transaction = Transaction,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    case dict:size(UpdatedPartitions) of
        0 ->
            lager:info("Replying directly"),
            reply_to_client(SD0#state{state=committed});
        N ->
            lager:info("Committing"),
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, Commit_time),
            {next_state, receive_committed,
             SD0#state{num_to_ack=N, state=committing}}
    end.


%% @doc the fsm waits for acks indicating that each partition has successfully
%%	committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(S0#state{state=committed});
        _ ->
           {next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.

%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{transaction = Transaction,
                          updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#state{state=aborted});

abort(abort, SD0=#state{transaction = Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#state{state=aborted});

abort({prepared, _}, SD0=#state{transaction=Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#state{state=aborted});

abort({ok, _}, SD0=#state{transaction=Transaction,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
    reply_to_client(SD0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(SD=#state{from=From, transaction=Transaction, read_set=ReadSet,
                                   state=TxState, commit_time=CommitTime}) ->
    
     _ = if undefined =/= From ->
            TxId = Transaction#transaction.txn_id,
            Reply = case TxState of
                        committed ->
                            DcId = ?DC_UTIL:get_my_dc_id(),
                            CausalClock = ?VECTORCLOCK:set_clock_of_dc(
                                DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                            {ok, {TxId, ReadSet, CausalClock}};
                        aborted->
                            {error, commit_fail};
                        Reason->
                            {TxId, Reason}
                    end,
            From ! Reply;
        true ->
            ok
    end,
    {stop, normal, SD}.

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

%%%===================================================================
%%% Internal Functions
%%%===================================================================


%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time())
                       -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).

-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
    Now = clocksi_vnode:now_microsec(erlang:now()) - ?OLD_SS_MICROSEC,
    case ?VECTORCLOCK:get_stable_snapshot() of
        {ok, VecSnapshotTime} ->
            DcId = ?DC_UTIL:get_my_dc_id(),
            SnapshotTime = dict:update(DcId,
                                       fun (_Old) -> Now end,
                                       Now, VecSnapshotTime),

            {ok, SnapshotTime}
    end.

-spec wait_for_clock(snapshot_time()) ->
                           {ok, snapshot_time()}.
wait_for_clock(Clock) ->
   case get_snapshot_time() of
       {ok, VecSnapshotTime} ->
	   %% dict:fold(fun(Dc,Time,_Acc) ->
	   %% 		     lager:info("Dc ~w, time ~w~n", [Dc,Time])
	   %% 	     end, 0, VecSnapshotTime),
           case vectorclock:ge(VecSnapshotTime, Clock) of
               true ->
                   %% No need to wait
                   {ok, VecSnapshotTime};
               false ->
                   %% wait for snapshot time to catch up with Client Clock
                   timer:sleep(10),
                   wait_for_clock(Clock)
           end
  end.


-ifdef(TEST).

main_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun empty_prepare_test/1,
      fun timeout_test/1,

      fun update_single_abort_test/1,
      fun update_single_success_test/1,
      fun update_multi_abort_test1/1,
      fun update_multi_abort_test2/1,
      fun update_multi_success_test/1,

      fun read_single_fail_test/1,
      fun read_success_test/1,

      fun get_snapshot_time_test/0,
      fun wait_for_clock_test/0
     ]}.

% Setup and Cleanup
setup()      -> {ok,Pid} = clocksi_interactive_tx_coord_fsm:start_link(self(), ignore), Pid. 
cleanup(Pid) -> case process_info(Pid) of undefined -> io:format("Already cleaned");
                                           _ -> clocksi_interactive_tx_coord_fsm:stop(Pid) end.

empty_prepare_test(Pid) ->
    fun() ->
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

timeout_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {timeout, riak_dt_pncounter, 
                    {increment, nothing}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_success_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test1(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_success_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

read_single_fail_test(Pid) ->
    fun() ->
            ?assertEqual({error, mock_read_fail}, 
                    gen_fsm:sync_send_event(Pid, {read, {read_fail, riak_dt_pncounter}}, infinity))
    end.

read_success_test(Pid) ->
    fun() ->
            ?assertEqual({ok, 2}, 
                    gen_fsm:sync_send_event(Pid, {read, {counter, riak_dt_gcounter}}, infinity)),
            ?assertEqual({ok, [a]}, 
                    gen_fsm:sync_send_event(Pid, {read, {set, riak_dt_gset}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

get_snapshot_time_test() ->
    {ok, SnapshotTime} = get_snapshot_time(),
    ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime)).

wait_for_clock_test() ->
    {ok, SnapshotTime} = wait_for_clock(vectorclock:from_list([{mock_dc,10}])),
    ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime)),
    VecClock = clocksi_vnode:now_microsec(now()),
    {ok, SnapshotTime2} = wait_for_clock(vectorclock:from_list([{mock_dc, VecClock}])),
    ?assertMatch([{mock_dc,_}],dict:to_list(SnapshotTime2)).


-endif.
