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
         update_store/4,
         check_prepared/3,
         prepare/2,
         commit/3,
         set_prepared/4,
         async_send_msg/2,
         single_commit/2,
         abort/2,
         now_microsec/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         open_table/2,
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

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          tx_metadata: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                tx_metadata :: cache_id(),
                committed_tx :: dict(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                quorum :: non_neg_integer(),
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
    TxMetadata = open_table(Partition, prepared),
    CommittedTx = dict:new(),
    %%true = ets:insert(TxMetadata, {committed_tx, dict:new()}),
    InMemoryStore = open_table(Partition, inmemory_store),
    
    clocksi_readitem_fsm:start_read_servers(Partition),

    IfCertify = antidote_config:get(do_cert),
    IfReplicate = antidote_config:get(do_repl),
    Quorum = antidote_config:get(quorum),

    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,

    {ok, #state{partition=Partition,
                committed_tx=CommittedTx,
                tx_metadata=TxMetadata,
                quorum = Quorum,
                if_certify = IfCertify,
                if_replicate = IfReplicate,
                inmemory_store=InMemoryStore}}.


check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


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

handle_command({prepare, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_tx=CommittedTx,
                              if_certify=IfCertify,
                              quorum=Quorum,
                              tx_metadata=TxMetadata
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    %[{committed_tx, CommittedTx}] = ets:lookup(TxMetadata, committed_tx),
    Result = prepare(TxId, WriteSet, CommittedTx, TxMetadata, 
                        PrepareTime, IfCertify),
    case Result of
        {ok, NewPrepare} ->
            case IfReplicate of
                true ->
                    PendingRecord = {prepare, Quorum-1, OriginalSender, 
                            {prepared, NewPrepare}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State};
                false ->
                    riak_core_vnode:reply(OriginalSender, {prepared, NewPrepare}),
                    {noreply, State}
            end;
        {error, wait_more} ->
            spawn(clocksi_vnode, async_send_msg, [{prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;


handle_command({single_commit, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              if_certify=IfCertify,
                              quorum=Quorum,
                              committed_tx=CommittedTx,
                              tx_metadata=TxMetadata
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    %[{committed_tx, CommittedTx}] = ets:lookup(TxMetadata, committed_tx),
    Result = prepare(TxId, WriteSet, CommittedTx, TxMetadata, PrepareTime, IfCertify), 
    case Result of
        {ok, NewPrepare} ->
            ResultCommit = commit(TxId, NewPrepare, WriteSet, CommittedTx, State),
            case ResultCommit of
                {ok, {committed, NewCommittedTx}} ->
                    case IfReplicate of
                        true ->
                            PendingRecord = {commit, Quorum-1, OriginalSender, 
                                {committed, NewPrepare}, {TxId, WriteSet}},
                            %ets:insert(TxMetadata, {committed_tx, NewCommittedTx}),
                            repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                            %{noreply, State};
                            {noreply, State#state{committed_tx=NewCommittedTx}};
                        false ->
                            %ets:insert(TxMetadata, {committed_tx, NewCommittedTx}),
                            riak_core_vnode:reply(OriginalSender, {committed, NewPrepare}),
                            %{noreply, State}
                            {noreply, State#state{committed_tx=NewCommittedTx}}
                        end;
                {error, no_updates} ->
                    %gen_fsm:send_event(OriginalSender, no_tx_record),
                    riak_core_vnode:reply(OriginalSender, no_tx_record),
                    {noreply, State}
            end;
        {error, wait_more}->
            spawn(clocksi_vnode, async_send_msg, [{prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxId, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      quorum=Quorum,
                      committed_tx=CommittedTx,
                      if_replicate=IfReplicate
                      } = State) ->
    %[{committed_tx, CommittedTx}] = ets:lookup(TxMetadata, committed_tx),
    Result = commit(TxId, TxCommitTime, Updates, CommittedTx, State),
    case Result of
        {ok, {committed,NewCommittedTx}} ->
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Quorum-1, Sender, 
                        committed, {TxId, TxCommitTime, Updates}},
                    %ets:insert(TxMetadata, {committed_tx, NewCommittedTx}),
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    %{noreply, State};
                    {noreply, State#state{committed_tx=NewCommittedTx}};
                false ->
                    %%ets:insert(TxMetadata, {committed_tx, NewCommittedTx}),
                    %{reply, committed, State}
                    {reply, committed, State#state{committed_tx=NewCommittedTx}}
            end;
        %{error, materializer_failure} ->
        %    {reply, {error, materializer_failure}, State};
        %{error, timeout} ->
        %    {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates}, _Sender,
               #state{partition=_Partition} = State) ->
    case Updates of
        [_Something] -> 
            clean_and_notify(TxId, Updates, State),
            {noreply, State};
            %%{reply, ack_abort, State};
        [] ->
            {reply, {error, no_tx_record}, State}
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{tx_metadata=Prepared} = State) ->
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
    ets:delete(get_cache_name(Partition,inmemory_store)),
    clocksi_readitem_fsm:stop_read_servers(Partition),    
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Msg, To) ->
    SleepTime = random:uniform(150)+50,
    timer:sleep(SleepTime),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

prepare(TxId, TxWriteSet, CommittedTx, TxMetadata, PrepareTime, IfCertify)->
    case certification_check(TxId, TxWriteSet, CommittedTx, TxMetadata, IfCertify) of
        true ->
            %case TxWriteSet of 
            %    [{Key, Type, Op} | Rest] -> 

		    set_prepared(TxMetadata, TxWriteSet, TxId,PrepareTime),
		    NewPrepare = now_microsec(erlang:now()),
		    set_prepared(TxMetadata, TxWriteSet, TxId,NewPrepare),

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
            {error,  wait_more}
    end.


set_prepared(_TxMetadata,[],_TxId,_Time) ->
    ok;
set_prepared(TxMetadata,[{Key, _Type, _Op} | Rest],TxId,Time) ->
    true = ets:insert(TxMetadata, {Key, {TxId, Time}}),
    set_prepared(TxMetadata,Rest,TxId,Time).

commit(TxId, TxCommitTime, Updates, CommittedTx, 
                                State=#state{inmemory_store=InMemoryStore})->
    case Updates of
        [{Key, _Type, _Value} | _Rest] -> 
            update_store(Updates, TxId, TxCommitTime, InMemoryStore),
            NewDict = dict:store(Key, TxCommitTime, CommittedTx),
            clean_and_notify(TxId,Updates,State),
            {ok, {committed, NewDict}};
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
%%      b. TxMetadata
%%
clean_and_notify(TxId, Updates, #state{
			      tx_metadata=TxMetadata}) ->
    clean_prepared(TxMetadata,Updates,TxId).

clean_prepared(_TxMetadata,[],_TxId) ->
    ok;
clean_prepared(TxMetadata,[{Key, _Type, _Op} | Rest],TxId) ->
    case ets:lookup(TxMetadata, Key) of
        [{Key, {TxId, _Time}}] ->
            true = ets:delete(TxMetadata, Key);
        _ ->
            ok
    end,   
    clean_prepared(TxMetadata,Rest,TxId).

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
%certification_check(_TxId, _H, _CommittedTx, _TxMetadata) ->
%    true.
certification_check(_, _, _, _, false) ->
    true;
certification_check(_, [], _, _, true) ->
    true;
certification_check(TxId, [H|T], CommittedTx, TxMetadata, true) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {Key, _Type, _} = H,
    case dict:find(Key, CommittedTx) of
        {ok, CommitTime} ->
            case CommitTime > SnapshotTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, TxMetadata, Key) of
                        true ->
                            certification_check(TxId, T, CommittedTx, TxMetadata, true);
                        false ->
                            false;
                        wait ->
                            wait
                    end
            end;
        error ->
            case check_prepared(TxId, TxMetadata, Key) of
                true ->
                    certification_check(TxId, T, CommittedTx, TxMetadata, true); 
                false ->
                    false;
                wait ->
                    wait
            end
    end.

check_prepared(TxId, TxMetadata, Key) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(TxMetadata, Key) of
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
                          TxId::txid(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id()) -> ok.
update_store([], _TxId, _TxCommitTime, _InMemoryStore) ->
    ok;
update_store([{Key, Type, {Param, Actor}}|Rest], TxId, TxCommitTime, InMemoryStore) ->
    %lager:info("Store ~w",[InMemoryStore]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
     %       lager:info("Wrote ~w to key ~w",[Value, Key]),
            Init = Type:new(),
            {ok, NewSnapshot} = Type:update(Param, Actor, Init),
            %lager:info("Updateing store for key ~w, value ~w firstly", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]}), 
            update_store(Rest, TxId, TxCommitTime, InMemoryStore);
        [{Key, ValueList}] ->
      %      lager:info("Wrote ~w to key ~w with ~w",[Value, Key, ValueList]),
            {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
            [{_CommitTime, First}|_] = RemainList,
            {ok, NewSnapshot} = Type:update(Param, Actor, First),
            %lager:info("Updateing store for key ~w, value ~w", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]}),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore)
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
