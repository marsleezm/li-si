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
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_vnode/1,
	 read_data_item/4,
	 async_read_data_item/4,
	 get_cache_name/2,
         check_prepared/3,
         replicate_log/3,
         log_replicated/2,
         prepare/2,
         commit/3,
         reset_prepared/5,
         async_send_msg/2,
         single_commit/2,
         abort/2,
         now_microsec/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
	 check_tables_ready/0,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-define(REP_FACTOR, 3).
-define(QUORUM, 2).
%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_tx: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                prepared_tx :: cache_id(),
                my_log :: cache_id(),
                replication_log :: dict(),
                committed_tx :: dict(),
                inmemory_store :: cache_id()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, Type, TxId) ->
    case clocksi_readitem_fsm:read_data_item(Node,Key,Type,TxId) of
        {ok, Snapshot} ->
	        {ok, Snapshot};
	    Other ->
	        Other
    end.


%% @doc Sends a read request to the Node that is responsible for the Key
async_read_data_item(Node, Key, Type, TxId) ->
    clocksi_readitem_fsm:async_read_data_item(Node,Key,Type,TxId).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    Self = {fsm, undefined, self()},
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId,WriteSet, Self},
                                Self,
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

replicate_log(ListOfNodes, Log, Master) ->
    riak_core_vnode_master:command(ListOfNodes,
                                    {replicate_log, Log, Master},
                                    {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

log_replicated(Master, Reply) ->
    riak_core_vnode_master:command(Master,
                                    {log_replicated, Reply},
                                    {fsm, undefined, self()},
                                   ?CLOCKSI_MASTER).

%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node,WriteSet}], TxId) ->
    Self = {fsm, undefined, self()},
    riak_core_vnode_master:command(Node,
                                   {single_commit, TxId,WriteSet, Self},
                                   Self,
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).


get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTx = open_table(Partition, prepared),
    CommittedTx = dict:new(),
    InMemoryStore = open_table(Partition, inmemory_store),
    MyLog = open_table(Partition, my_log),
    Predecessors = log_utilities:get_my_previous(Partition, ?REP_FACTOR-1),
    ReplicationLog = dict:new(),
    NewReplicationLog = open_local_tables(Predecessors, ReplicationLog),
    clocksi_readitem_fsm:start_read_servers(Partition),
    {ok, #state{partition=Partition,
                prepared_tx=PreparedTx,
                committed_tx=CommittedTx,
                my_log=MyLog,
                replication_log=NewReplicationLog,
                inmemory_store=InMemoryStore}}.


check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).

int_to_atom(Int) ->
    list_to_atom(integer_to_list(Int)).

check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_tables_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.

open_local_tables([], Dict) ->
    Dict;
open_local_tables([{Part, Node}|Rest], Dict) ->
    try
    Tab = ets:new(int_to_atom(Part),
            [set,?TABLE_CONCURRENCY]),
    NewDict = dict:store(Part, Tab, Dict),
    open_local_tables(Rest, NewDict)
    catch
    _:_Reason ->
        lager:info("Error opening table..."),
        %% Someone hasn't finished cleaning up yet
        open_local_tables([{Part, Node}|Rest], Dict)
    end.

open_table(Partition, Name) ->
    try
	ets:new(get_cache_name(Partition,Name),
		[set,protected,named_table,?TABLE_CONCURRENCY])
    catch
	_:_Reason ->
	    %% Someone hasn't finished cleaning up yet
	    open_table(Partition, Name)
    end.

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(get_cache_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};
    
handle_command({check_servers_ready},_Sender,SD0=#state{partition=Partition}) ->
    Node = node(),
    Result = clocksi_readitem_fsm:check_partition_ready(Node,Partition,?READ_CONCURRENCY),
    {reply, Result, SD0};

handle_command({replicate_log, Log, Master}, _Sender, #state{replication_log=ReplicationLog}=State) ->
    {Type, {TxId, Record}} = Log,
    {Index, _Node} = Master,
    RepTab = dict:fetch(Index, ReplicationLog),
    true = ets:insert(RepTab, {TxId, Record}),
    log_replicated(Master, {Type, TxId}), 
    {noreply, State};
    %{reply, {log_replicated, {Type, TxId}}, State};


handle_command({log_replicated, {Type, TxId}}, _Sender, #state{my_log=MyLog}=State) ->
    %lager:info("Got reply ~p", [Type]),
    case ets:lookup(MyLog, TxId) of 
        [{TxId, {RecordType, AckNeeded, Sender, MsgToReply, Record}}] ->
            case Type of
                RecordType ->
                    case AckNeeded of
                        1 -> %%Has got all neede ack, can log message already
                            %lager:info("Returned to client"),
                            ets:insert(MyLog, {TxId, Record}),
                            %lager:info("#####DONE#####Sending ~p to ~p", [Sender, MsgToReply]),
                            riak_core_vnode:reply(Sender, MsgToReply);
                        _ -> %%Wait for more replies
                            ets:insert(MyLog, {TxId, {RecordType, AckNeeded-1, 
                                    Sender, MsgToReply, Record}})
                    end;
                _ ->
                    ok
            end;
        [_SomeRecord] -> %%The record is appended already, do nothing
            %lager:info("No need to do anything ~p", [_SomeRecord]),
            ok
    end,
    {noreply, State};
    

handle_command({prepare, Transaction, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              committed_tx=CommittedTx,
                              prepared_tx=PreparedTx,
                              my_log=MyLog
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    Result = prepare(Transaction, WriteSet, CommittedTx, PreparedTx, 
                        PrepareTime),
    case Result of
        {ok, NewPrepare} ->
            TxId = Transaction#transaction.txn_id,
            PendingRecord = {prepare, ?QUORUM-1, OriginalSender, {prepared, NewPrepare},
                    {Transaction, WriteSet}},
            ets:insert(MyLog, {Transaction#transaction.txn_id, PendingRecord}), 
            ReplNodes = log_utilities:get_my_next(Partition, ?REP_FACTOR-1),
            replicate_log(ReplNodes, {prepare, {TxId, {Transaction,WriteSet}}}, {Partition, node()}),
            {noreply, State};
        {error, wait_more} ->
            spawn(clocksi_vnode, async_send_msg, [{prepare, Transaction, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, no_updates} ->
            riak_core_vnode:reply(OriginalSender, {error, no_tx_record}),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;


    

handle_command({single_commit, Transaction, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              committed_tx=CommittedTx,
                              prepared_tx=PreparedTx,
                              my_log=MyLog
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    %lager:info("Before preparing"),
    Result = prepare(Transaction, WriteSet, CommittedTx, PreparedTx, PrepareTime), 
    case Result of
        {ok, NewPrepare} ->
            ResultCommit = commit(Transaction, NewPrepare, WriteSet, CommittedTx, State),
            case ResultCommit of
                {ok, {committed, NewCommittedTxs}} ->
                    %lager:info("Trying to replicate"),
                    TxId = Transaction#transaction.txn_id,
                    PendingRecord = {commit, ?QUORUM-1, OriginalSender, 
                                {committed, NewPrepare}, {Transaction, WriteSet}},
                    ets:insert(MyLog, {TxId, PendingRecord}), 
                    ReplNodes = log_utilities:get_my_next(Partition, ?REP_FACTOR-1),
                    %lager:info("Got my next ~p", [ReplNodes]),
                    replicate_log(ReplNodes, {commit, {TxId, {NewPrepare,Transaction,WriteSet}}},
                                {Partition, node()}),
                    %lager:info("Sent message"),
                    {noreply, State#state{committed_tx=NewCommittedTxs}};
                %{error, timeout} ->
                %    {reply, {error, timeout}, State};
                {error, no_updates} ->
                    %gen_fsm:send_event(OriginalSender, no_tx_record),
                    riak_core_vnode:reply(OriginalSender, no_tx_record),
                    {noreply, State}
            end;
        {error, wait_more}->
            spawn(clocksi_vnode, async_send_msg, [{prepare, Transaction, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, no_updates} ->
            %gen_fsm:send_event(OriginalSender, {error, no_tx_record}),
            riak_core_vnode:reply(OriginalSender, {error, no_tx_record}),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, Transaction, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      my_log=MyLog,
                      committed_tx=CommittedTx
                      } = State) ->
    %%lager:info("Commting in vnode"),
    Result = commit(Transaction, TxCommitTime, Updates, CommittedTx, State),
    case Result of
        {ok, {committed,NewCommittedTx}} ->
            TxId = Transaction#transaction.txn_id,
            PendingRecord = {commit, ?QUORUM-1, Sender, 
                    committed, {Transaction, TxCommitTime, Updates}},
            ets:insert(MyLog, {TxId, PendingRecord}), 
            ReplNodes = log_utilities:get_my_next(Partition, ?REP_FACTOR-1),
            replicate_log(ReplNodes, {commit, {TxId, {Transaction,Updates}}},
                    {Partition, node()}),
            {noreply, State#state{committed_tx=NewCommittedTx}};
        %{error, materializer_failure} ->
        %    {reply, {error, materializer_failure}, State};
        %{error, timeout} ->
        %    {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, Transaction, Updates}, _Sender,
               #state{partition=_Partition} = State) ->
    TxId = Transaction#transaction.txn_id,
    case Updates of
        [_Something] -> 
            %LogId = log_utilities:get_logid_from_key(Key),
            %[Node] = log_utilities:get_preflist_from_key(Key),
            %Result = logging_vnode:append(Node,LogId,{TxId, aborted}),
            %case Result of
            %    {ok, _} ->
                    %lager:info("Trying to abort ~w with updates ~w", [TxId, Updates]),
                    clean_and_notify(TxId, Updates, State),
            %    {error, timeout} ->
            %        clean_and_notify(TxId, Key, State)
            %end,
            {reply, ack_abort, State};
        [] ->
            {reply, {error, no_tx_record}, State}
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_tx=Prepared} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

handle_command({start_read_servers}, _Sender,
               #state{partition=Partition} = State) ->
    clocksi_readitem_fsm:stop_read_servers(Partition),
    clocksi_readitem_fsm:start_read_servers(Partition),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName,Val}).

is_empty(State) ->
    {true,State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{partition=Partition} = _State) ->
    ets:delete(get_cache_name(Partition,prepared)),
    clocksi_readitem_fsm:stop_read_servers(Partition),    
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Msg, To) ->
    SleepTime = random:uniform(150)+50,
    timer:sleep(SleepTime),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

prepare(Transaction, TxWriteSet, CommittedTx, PreparedTx, PrepareTime)->
    TxId = Transaction#transaction.txn_id,
    case certification_check(TxId, TxWriteSet, CommittedTx, PreparedTx) of
        true ->
            %case TxWriteSet of 
            %    [{Key, Type, Op} | Rest] -> 
		    PrepList = set_prepared(PreparedTx, TxWriteSet, TxId,PrepareTime,[]),
		    NewPrepare = now_microsec(erlang:now()),
		    reset_prepared(PreparedTx, TxWriteSet, TxId,NewPrepare,PrepList),
		    %LogRecord = #log_record{tx_id=TxId,
			%		    op_type=prepare,
			%		    op_payload=NewPrepare},
            %        LogId = log_utilities:get_logid_from_key(Key),
            %        [Node] = log_utilities:get_preflist_from_key(Key),
		    %        NewUpdates = write_set_to_logrecord(TxId,TxWriteSet),
            %        Result = logging_vnode:append_group(Node,LogId,NewUpdates ++ [LogRecord]),
		    {ok, NewPrepare};
	    false ->
	        {error, write_conflict};
        wait ->
            {error,  wait_more};
		_ ->
		    {error, no_updates}
    end.


set_prepared(_PreparedTx,[],_TxId,_Time,Acc) ->
    lists:reverse(Acc);
set_prepared(PreparedTx,[{Key, _Type, _Op} | Rest],TxId,Time,Acc) ->
    ActiveTxs = case ets:lookup(PreparedTx, Key) of
		    [] ->
			[];
		    [{Key, List}] ->
			List
		end,
    true = ets:insert(PreparedTx, {Key, [{TxId, Time}|ActiveTxs]}),
    set_prepared(PreparedTx,Rest,TxId,Time,[ActiveTxs|Acc]).

reset_prepared(_PreparedTx,[],_TxId,_Time,[]) ->
    ok;
reset_prepared(PreparedTx,[{Key, _Type, _Op} | Rest],TxId,Time,[ActiveTxs|Rest2]) ->
    true = ets:insert(PreparedTx, {Key, [{TxId, Time}|ActiveTxs]}),
    reset_prepared(PreparedTx,Rest,TxId,Time,Rest2).


commit(Transaction, TxCommitTime, Updates, CommittedTx, 
                                State=#state{inmemory_store=InMemoryStore})->
    TxId = Transaction#transaction.txn_id,
    %DcId = dc_utilities:get_my_dc_id(),
    %LogRecord=#log_record{tx_id=TxId,
    %                      op_type=commit,
    %                      op_payload={{DcId, TxCommitTime},
    %                                  Transaction#transaction.vec_snapshot_time}},
    case Updates of
        [{Key, _Type, _Value} | _Rest] -> 
            %LogId = log_utilities:get_logid_from_key(Key),
            %[Node] = log_utilities:get_preflist_from_key(Key),
            %case logging_vnode:append(Node,LogId,LogRecord) of
            %    {ok, _} ->
                    %lager:info("Committing ops ~w",[Updates]),
                    update_store(Updates, Transaction, TxCommitTime, InMemoryStore),
                    NewDict = dict:store(Key, TxCommitTime, CommittedTx),
                    clean_and_notify(TxId,Updates,State),
                    {ok, {committed, NewDict}};
            %    {error, timeout} ->
            %        {error, timeout}
            %end;
        _ -> 
            {error, no_updates}
    end.




%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to:
%%      1. notify all read_fsms that are waiting for this transaction to finish
%%      2. clean the state of the transaction. Namely:
%%      a. ActiteTxsPerKey,
%%      b. PreparedTx
%%
clean_and_notify(TxId, Updates, #state{
			      prepared_tx=PreparedTx}) ->
    clean_prepared(PreparedTx,Updates,TxId).

clean_prepared(_PreparedTx,[],_TxId) ->
    ok;
clean_prepared(PreparedTx,[{Key, _Type, _Op} | Rest],TxId) ->
    case ets:lookup(PreparedTx, Key) of
        [{Key, ActiveTxs}] ->
            case ActiveTxs of
                [_Prepared] ->
                    true = ets:delete(PreparedTx, Key);
                _ ->
                    NewActive = lists:keydelete(TxId,1,ActiveTxs),
                    true = ets:insert(PreparedTx, {Key, NewActive})
            end;
        [] ->
            ok
    end,   
    %lager:info("Prepared transaction now is ~w", [NewActive]),
    clean_prepared(PreparedTx,Rest,TxId).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
%certification_check(_TxId, _H, _CommittedTx, _PreparedTx) ->
%    true.
certification_check(_, [], _, _) ->
    true;
certification_check(TxId, [H|T], CommittedTx, PreparedTx) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {Key, _Type, _} = H,
    case dict:find(Key, CommittedTx) of
        {ok, CommitTime} ->
            case CommitTime > SnapshotTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, PreparedTx, Key) of
                        true ->
                            certification_check(TxId, T, CommittedTx, PreparedTx);
                        false ->
                            false;
                        wait ->
                            wait
                    end
            end;
        error ->
            case check_prepared(TxId, PreparedTx, Key) of
                true ->
                    certification_check(TxId, T, CommittedTx, PreparedTx); 
                false ->
                    false;
                wait ->
                    wait
            end
    end.

check_prepared(TxId, PreparedTx, Key) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTx, Key) of
        [] ->
            true;
        [{Key, List}] ->
            [{_TxId1, PrepareTime}|_] = List,
            case PrepareTime > SnapshotTime of
                true ->
                    %lager:info("Has to abort for Key ~w", [Key]),
                    false;
                false ->
                    case random:uniform(2) of
                        1 ->
                            false;
                        2 ->
                            wait  
                    end
            end
    end.

%% check_keylog(_, [], _) ->
%%     false;
%% check_keylog(TxId, [H|T], CommittedTx)->
%%     {_Key, _Type, ThisTxId}=H,
%%     case ThisTxId > TxId of
%%         true ->
%%             CommitInfo = ets:lookup(CommittedTx, ThisTxId),
%%             case CommitInfo of
%%                 [{_, _CommitTime}] ->
%%                     true;
%%                 [] ->
%%                     check_keylog(TxId, T, CommittedTx)
%%             end;
%%         false ->
%%             check_keylog(TxId, T, CommittedTx)
%%     end.

-spec update_store(KeyValues :: [{key(), atom(), term()}],
                          Transaction::tx(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id()) -> ok.
update_store([], _Transaction, _TxCommitTime, _InMemoryStore) ->
    ok;
update_store([{Key, Type, {Param, Actor}}|Rest], Transaction, TxCommitTime, InMemoryStore) ->
    %lager:info("Store ~w",[InMemoryStore]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
     %       lager:info("Wrote ~w to key ~w",[Value, Key]),
            Init = Type:new(),
            {ok, NewSnapshot} = Type:update(Param, Actor, Init),
            %lager:info("Updateing store for key ~w, value ~w firstly", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]}), 
            update_store(Rest, Transaction, TxCommitTime, InMemoryStore);
        [{Key, ValueList}] ->
      %      lager:info("Wrote ~w to key ~w with ~w",[Value, Key, ValueList]),
            {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
            [{_CommitTime, First}|_] = RemainList,
            {ok, NewSnapshot} = Type:update(Param, Actor, First),
            %lager:info("Updateing store for key ~w, value ~w", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]}),
            update_store(Rest, Transaction, TxCommitTime, InMemoryStore)
    end,
    ok.

%write_set_to_logrecord(TxId, WriteSet) ->
%    lists:foldl(fun({Key,Type,Op}, Acc) ->
%			Acc ++ [#log_record{tx_id=TxId, op_type=update,
%					    op_payload={Key, Type, Op}}]
%		end,[],WriteSet).

%% Internal functions
filter_updates_per_key(Updates, Key) ->
    FilterMapFun = fun ({KeyPrime, _Type, Op}) ->
        case KeyPrime == Key of
            true  -> {true, Op};
            false -> false
        end
    end,
    lists:filtermap(FilterMapFun, Updates).


-ifdef(TEST).

%% @doc Testing filter_updates_per_key.
filter_updates_per_key_test()->
    Op1 = {update, {{increment,1}, actor1}},
    Op2 = {update, {{increment,2}, actor1}},
    Op3 = {update, {{increment,3}, actor1}},
    Op4 = {update, {{increment,4}, actor1}},

    ClockSIOp1 = {a, crdt_pncounter, Op1},
    ClockSIOp2 = {b, crdt_pncounter, Op2},
    ClockSIOp3 = {c, crdt_pncounter, Op3},
    ClockSIOp4 = {a, crdt_pncounter, Op4},

    ?assertEqual([Op1, Op4], 
        filter_updates_per_key([ClockSIOp1, ClockSIOp2, ClockSIOp3, ClockSIOp4], a)).

-endif.
