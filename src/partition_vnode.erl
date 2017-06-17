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
-module(partition_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_VERSION, 20).

-export([start_vnode/1,
	    read_data_item/4,
	    get_cache_name/2,
        print_stat/1,
        set_prepared/5,
        update_store/7,
        get_snapshot_time/2,
        prepare/3,
        commit/3,
        abort/3,
        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1,
        open_table/2,
	    check_tables_ready/0,
	    check_prepared_empty/0]).

-export([
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
%%          prepared_txs: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                commit_diff :: {non_neg_integer(), non_neg_integer()},
                prepared_txs :: cache_id(),
                committed_txs :: cache_id(),
                pending_prepare :: cache_id(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                if_precise :: boolean(),
                inmemory_store :: cache_id(),
                clock}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, TxId, AggrClock) ->
    riak_core_vnode_master:sync_command(Node,
                                   {read, Key, TxId, AggrClock},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Ask all nodes to print its status 
print_stat(ListofNodes) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {print_stat},
    						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ListofNodes).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId, AggrClock) ->
    dict:fold(fun(Node, WriteSet, _Acc) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, [Key||{Key, _, _} <- WriteSet], AggrClock},
    						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId, MaxClock) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet, MaxClock},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

get_snapshot_time(Node, CausalClock) ->
    riak_core_vnode_master:sync_command(Node,
                            {get_snapshot_time, CausalClock},
                            ?CLOCKSI_MASTER, infinity).

get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTxs = open_table(Partition, prepared),
    CommittedTxs = open_table(Partition, committed),
    %%true = ets:insert(PreparedTxs, {committed_tx, dict:new()}),
    InMemoryStore = open_table(Partition, inmemory_store),
    PendingPrepare = open_table(Partition, pending_prepare),

    IfCertify = antidote_config:get(do_cert),

    ClockType = antidote_config:get(clock_type),
    IfPrecise = antidote_config:get(if_precise),
    case IfPrecise of
        true ->
            true = ClockType /= physical,
            true = ClockType /= hybrid;
        false ->
            ok
    end,
    {ok, Clock} = clock_utilities:init_clock(ClockType),

    {ok, #state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs,
                if_certify = IfCertify,
                if_precise = IfPrecise,
                commit_diff = {0,0},
                inmemory_store=InMemoryStore,
		        pending_prepare=PendingPrepare,
                clock=Clock}}.

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

check_prepared_empty() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_prepared_empty(PartitionList).

check_prepared_empty([]) ->
    ok;
check_prepared_empty([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_prepared_empty},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	    true ->
            ok;
	    false ->
          lager:warning("Prepared not empty!")
    end,
	check_prepared_empty(Rest).

open_table(Partition, Name) ->
    try
	    ets:new(get_cache_name(Partition,Name),
		    [set,protected,named_table,?TABLE_CONCURRENCY])
    catch
	    _:_Reason ->
	      %% Someone hasn't finished cleaning up yet
	      open_table(Partition, Name)
    end.

handle_command({print_stat},_Sender,SD0=#state{commit_diff={Count, Diff}, partition=Partition}) ->
    lager:info("Partition ~w: count is ~w, avg diff of commit and prepare time is ~w", [Partition, Count, Diff/Count]), 
    {noreply, SD0};

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(get_cache_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};

handle_command({check_prepared_empty},_Sender,SD0=#state{prepared_txs=PreparedTxs}) ->
    PreparedList = ets:tab2list(PreparedTxs),
    case length(PreparedList) of
		 0 ->
            {reply, true, SD0};
		 _ ->
            lager:warning("Not empty!! ~w", [PreparedList]),
            {reply, false, SD0}
    end;

handle_command({pending_read, TxId, Key, Sender, Wait}, _From, SD0=#state{clock=Clock, prepared_txs=PreparedTxs,
                    inmemory_store=InMemoryStore, if_precise=IfPrecise}) ->
    {ok, CatchedUpV, Clock0} = clock_utilities:force_catch_up(Clock, TxId#tx_id.snapshot_time),
    case ready_or_block(TxId, Key, PreparedTxs, Sender, Wait, Clock0, IfPrecise) of
        not_ready ->
            {noreply, SD0#state{clock=Clock0}};
        ready ->
            {ok, {Value, Missed}} = read_value(Key, TxId, InMemoryStore),
            riak_core_vnode:reply(Sender, {ok, {Value, Missed, Wait}, CatchedUpV}),
            {noreply, SD0#state{clock=Clock0}}
    end;

handle_command({read, Key, TxId, AggrClock}, Sender, SD0=#state{clock=Clock, prepared_txs=PreparedTxs,
                    inmemory_store=InMemoryStore, if_precise=IfPrecise}) ->
    {ok, Wait, Clock0} = clock_utilities:catch_up(Clock, TxId#tx_id.snapshot_time, AggrClock),
    case round(Wait/1000) > 0 of
        true ->
            riak_core_vnode:send_command_after(round(Wait/1000), {pending_read, TxId, Key, Sender, Wait}),
            {noreply, SD0#state{clock=Clock0}};
        false ->
	        {ok, CatchedUpV, Clock1} = clock_utilities:force_catch_up(Clock0, TxId#tx_id.snapshot_time),
            case ready_or_block(TxId, Key, PreparedTxs, Sender, 0, Clock0, IfPrecise) of
                not_ready ->
                    {noreply, SD0#state{clock=Clock1}};
                ready ->
                    {ok, {Value, Missed}} = read_value(Key, TxId, InMemoryStore),
                    riak_core_vnode:reply(Sender, {ok, {Value, Missed, 0}, CatchedUpV}),
                    {noreply, SD0#state{clock=Clock1}}
            end
    end;

handle_command({pending_prepare, TxId, WriteSet, Sender, Wait}, _From,
               State = #state{partition=_Partition,
                              committed_txs=CommittedTxs,
                              clock=Clock,
                              if_certify=IfCertify,
                              if_precise=IfPrecise,
                              prepared_txs=PreparedTxs,
			                  pending_prepare=PendingPrepare
                              }) ->
    case ets:lookup(PendingPrepare, TxId) of
    	[{TxId, pending}] ->
            ets:delete(PendingPrepare, TxId),
    	    Clock1 = prepare_logic(TxId, WriteSet, CommittedTxs, PreparedTxs, IfCertify, Sender, Clock, Wait, IfPrecise),
    	    {noreply, State#state{clock=Clock1}};
        [] ->
    	    {noreply, State}
    end;

handle_command({prepare, TxId, WriteSet, AggrClock}, Sender,
               State = #state{partition=_Partition,
                              committed_txs=CommittedTxs,
                              clock=Clock,
                              if_certify=IfCertify,
                              if_precise=IfPrecise,
                              prepared_txs=PreparedTxs,
			                  pending_prepare=PendingPrepare
                              }) ->
    {ok, Wait, Clock0} = clock_utilities:catch_up(Clock, TxId#tx_id.snapshot_time, AggrClock),
    lager:warning("~w prepare, waiting is ~w, Clock0 is ~w", [TxId, Wait, Clock0]),
    case round(Wait/1000) > 0 of
        true ->
	        true = ets:insert(PendingPrepare, {TxId, pending}),
            riak_core_vnode:send_command_after(round(Wait/1000), {pending_prepare, TxId, WriteSet, Sender, Wait}),
            {noreply, State#state{clock=Clock0}};
        false ->
   	        Clock1 = prepare_logic(TxId, WriteSet, CommittedTxs, PreparedTxs, IfCertify, Sender, Clock0, 0, IfPrecise),
    	    {noreply, State#state{clock=Clock1}}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxId, TxCommitTime, Updates}, _Sender,
               #state{partition=_Partition,
                      committed_txs=CommittedTxs,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      commit_diff = {Count, TimeDiff} 
                      } = State) ->
    lager:warning("~w committed in ~w", [TxId, _Partition]),
    Result = commit(TxId, TxCommitTime, Updates, CommittedTxs, PreparedTxs, InMemoryStore),
    case Result of
        {ok, DiffInTime, committed} ->
            {noreply, State#state{commit_diff={Count+1, TimeDiff+DiffInTime}}};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates, MaxClock}, _Sender,
               #state{partition=_Partition, prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, pending_prepare=PendingPrepare, clock=MyClock, if_precise=IfPrecise} = State) ->
    lager:warning("~w geting aborted in ~w", [TxId, _Partition]),
    {ok, MyClock1} = clock_utilities:catch_up_if_aggr(MyClock, MaxClock),
    case Updates of
        [] ->
            lager:warning("~w no tx record", [TxId]),
            {reply, {error, no_tx_record}, State#state{clock=MyClock1}};
        _ ->
	    case ets:lookup(PendingPrepare, TxId) of
        	[{TxId, pending}] ->
		    ets:delete(PendingPrepare, TxId);
                [] ->
		    noop
            end,
            clean_abort_prepared(PreparedTxs,Updates,TxId, InMemoryStore, IfPrecise),
            {noreply, State#state{clock=MyClock1}}
    end;


%%%%%%%%%%%%%%  Handle clock-related commands  %%%%%%%%%%%%%%%%%%%%%%%%
handle_command({get_snapshot_time, _CausalClock}, _Sender, #state{clock=Clock}=State) ->
    {ok, SnapshotTime, Clock1} = clock_utilities:get_snapshot_time(Clock),
    {reply, SnapshotTime, State#state{clock=Clock1}};

%%%%%%%%%%% Other handling %%%%%%%%%%%%%
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
    ets:delete(get_cache_name(Partition,committed)),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
prepare(TxId, TxWriteSet, CommittedTx, PreparedTxs, PrepareTime, IfCertify, IfPrecise)->
    case certification_check(TxId, TxWriteSet, CommittedTx, PreparedTxs, IfCertify, IfPrecise, 0) of
        true ->
		    set_prepared(PreparedTxs, TxWriteSet, TxId, PrepareTime, IfPrecise),
		    ok;
        {true, PrecisePrepTime} ->
            IfPrecise = true,
            ToPrepTime = max(PrecisePrepTime, TxId#tx_id.snapshot_time)+1,
             lager:warning("Precise time is ~w, ToPrepTime is ~w", [PrecisePrepTime, ToPrepTime]),
		    set_prepared(PreparedTxs, TxWriteSet, TxId, ToPrepTime, IfPrecise),
		    {ok, ToPrepTime};
	    false ->
	        {error, write_conflict};
        wait ->
            {error, wait_more}
    end.

set_prepared(_PreparedTxs,[],_TxId,_Time, _) ->
    ok;
set_prepared(PreparedTxs,[Key | Rest],TxId,Time, false) ->
    true = ets:insert(PreparedTxs, {Key, {TxId, Time, []}}),
    set_prepared(PreparedTxs,Rest,TxId,Time, false);
set_prepared(PreparedTxs,[Key | Rest],TxId,Time, true) ->
    true = ets:insert(PreparedTxs, {Key, Time, {TxId, Time, []}}),
    set_prepared(PreparedTxs,Rest,TxId,Time, true).

commit(TxId, TxCommitTime, Updates, CommittedTxs, PreparedTxs, InMemoryStore)->
    DiffInTime = update_store(Updates, TxId, TxCommitTime, CommittedTxs, InMemoryStore, PreparedTxs, 0),
    {ok, DiffInTime, committed}.

clean_abort_prepared(_PreparedTxs,[],_TxId,_, _) ->
    ok;
clean_abort_prepared(PreparedTxs,[{Key, _Op, _Param} | Rest],TxId,InMemoryStore, IfPrecise) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time, PendingReaders}}] ->
            case ets:lookup(InMemoryStore, Key) of
                [{Key, ValueList}] ->
                    {_, Value} = hd(ValueList),
                    lists:foreach(fun({_SnapshotTime, Sender, Wait, Clock}) -> riak_core_vnode:reply(Sender, {ok, {Value, 0, Wait}, Clock}) end, PendingReaders);
                [] ->
                    lists:foreach(fun({_SnapshotTime, Sender, Wait, Clock}) -> riak_core_vnode:reply(Sender, {ok, {nil, 0, Wait}, Clock}) end, PendingReaders)
            end,
            true = ets:delete(PreparedTxs, Key);
        [{Key, LastReader, {TxId, _Time, PendingReaders}}] ->
            IfPrecise = true,
            case ets:lookup(InMemoryStore, Key) of
                [{Key, ValueList}] ->
                    {_, Value} = hd(ValueList),
                    lists:foreach(fun({_SnapshotTime, Sender, Wait, Clock}) -> riak_core_vnode:reply(Sender, {ok, {Value, 0, Wait}, Clock}) end, PendingReaders);
                [] ->
                    lists:foreach(fun({_SnapshotTime, Sender, Wait, Clock}) -> riak_core_vnode:reply(Sender, {ok, {nil, 0, Wait}, Clock}) end, PendingReaders)
            end,
            true = ets:insert(PreparedTxs, {Key, LastReader});
        _ ->
            ok
    end,
    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, IfPrecise).

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_, _, _, _, false, _, 0) ->
    true;
certification_check(_, [], _, _, true, false, 0) ->
    true;
certification_check(_, [], _, _, true, true, PrepareTime) ->
    {true, PrepareTime};
certification_check(TxId, [Key|T], CommittedTxs, PreparedTxs, true, IfPrecise, PrepTime) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(CommittedTxs, Key) of
        [{Key, CommitTime}] ->
            case CommitTime > SnapshotTime of
                true ->
                     lager:warning("Abort key ~w: ct ~w, st ~w", [Key, CommitTime, SnapshotTime]),
                    false;
                false ->
                    case check_prepared(TxId, PreparedTxs, Key, IfPrecise) of
                        true ->
                            certification_check(TxId, T, CommittedTxs, PreparedTxs, true, IfPrecise, PrepTime);
                        {true, LastReader} ->
                            certification_check(TxId, T, CommittedTxs, PreparedTxs, true, 
                                IfPrecise, max(LastReader, PrepTime));
                        false ->
                            false
                    end
            end;
        [] ->
            case check_prepared(TxId, PreparedTxs, Key, IfPrecise) of
                true ->
                    certification_check(TxId, T, CommittedTxs, PreparedTxs, true, IfPrecise, PrepTime);
                {true, LastReader} ->
                    certification_check(TxId, T, CommittedTxs, PreparedTxs, true, 
                        IfPrecise, max(LastReader, PrepTime));
                false ->
                    false
            end
    end.

check_prepared(_TxId, PreparedTxs, Key, true) ->
    case ets:lookup(PreparedTxs, Key) of
        [] ->
             lager:warning("~w ~w: LastReader is empty", [_TxId, Key]),
            true;
        [{Key, LastReader}] ->
             lager:warning("~w ~w: LastReader is ~w", [_TxId, Key, LastReader]),
            {true, LastReader};
        _R ->
             lager:warning("Abort key ~w: ~w is here, TxId is ~w!", [Key, _R, _TxId]),
            false
    end;
check_prepared(_TxId, PreparedTxs, Key, false) ->
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            true;
        _R ->
            lager:warning("Certification for key ~w, ~w is here!", [Key, _R]),
            false
    end.

ready_or_block(TxId, Key, PreparedTxs, Sender, Wait, Clock, true) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
             lager:warning("~w ~w: reading empty!", [TxId, Key]),
            ready;
        [{Key, LastReader}] ->
             lager:warning("~w ~w: read empty reader! ~w", [TxId, Key, LastReader]),
            case LastReader < SnapshotTime of
                true -> ets:insert(PreparedTxs, {Key, SnapshotTime});
                false -> ok
            end,
            ready;
        [{Key, LastReader, {PreparedTxId, PrepareTime, PendingReader}=Record}] ->
            case PrepareTime =< SnapshotTime of
                true ->
                     lager:warning("~w ~w: blocked! PrepTime ~w, LastReader ~w", [TxId, Key, PrepareTime, LastReader]),
                    ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReader), 
                        {PreparedTxId, PrepareTime, [{TxId#tx_id.snapshot_time, Sender, Wait, Clock}|PendingReader]}}),
                    not_ready;
                false ->
                     lager:warning("~w ~w: not blocked! PrepTime ~w, LastReader ~w", [TxId, Key, PrepareTime, LastReader]),
                    case LastReader < SnapshotTime of
                        true ->
                            ets:insert(PreparedTxs, {Key, SnapshotTime, Record});
                        false ->
                            ok
                    end,
                    ready
            end
    end;
ready_or_block(TxId, Key, PreparedTxs, Sender, Wait, Clock, false) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ready;
        [{Key, {PreparedTxId, PrepareTime, PendingReader}}] ->
            case PrepareTime =< SnapshotTime of
                true ->
                    ets:insert(PreparedTxs, {Key, {PreparedTxId, PrepareTime,
                        [{TxId#tx_id.snapshot_time, Sender, Wait, Clock}|PendingReader]}}),
                    not_ready;
                false ->
                    ready
            end
    end.

-spec update_store(KeyValues :: [{key(), atom(), term()}],
                          TxId::txid(),TxCommitTime:: {term(), term()}, CommittedTxs :: cache_id(),
                        InMemoryStore :: cache_id(), PreparedTxs :: cache_id(), DiffInTime :: non_neg_integer()) -> ok.
update_store([], _TxId, _TxCommitTime, _CommittedTxs, _InMemoryStore, _PreparedTxs, DiffInTime) ->
    DiffInTime;
update_store([{Key, Op, Param}|Rest], TxId, TxCommitTime, CommittedTxs, InMemoryStore, PreparedTxs, DiffInTime) ->
    Values= case ets:lookup(InMemoryStore, Key) of
                [] ->
                    NewSnapshot = update_object:update(Op, Param),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]}),
                    [nil, NewSnapshot];
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                    [{_CommitTime, First}|_] = RemainList,
                    NewSnapshot = update_object:update(First, Op, Param),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]}),
                    {_, FirstValue} = hd(ValueList),
                    [FirstValue, NewSnapshot]
            end,
    NewDiff = case ets:lookup(PreparedTxs, Key) of
            [{Key, {TxId, _Time, PendingReaders}}] ->
                lists:foreach(fun({SnapshotTime, Sender, Wait, Clock}) ->
                        case SnapshotTime >= TxCommitTime of
                            true ->
                                riak_core_vnode:reply(Sender, {ok, {lists:nth(2, Values), 1, Wait}, Clock});
                            false ->
                                riak_core_vnode:reply(Sender, {ok, {hd(Values), 0, Wait}, Clock})
                        end end,
                    PendingReaders),
                 lager:warning("~w diff is ~w", [TxId, TxCommitTime-_Time]),
                true = ets:delete(PreparedTxs, Key),
                max(TxCommitTime-_Time, DiffInTime);
            [{Key, LastReader, {TxId, _Time, PendingReaders}}] ->
                lists:foreach(fun({SnapshotTime, Sender, Wait, Clock}) ->
                        case SnapshotTime >= TxCommitTime of
                            true ->
                                riak_core_vnode:reply(Sender, {ok, {lists:nth(2, Values), 1, Wait}, Clock});
                            false ->
                                riak_core_vnode:reply(Sender, {ok, {hd(Values), 0, Wait}, Clock})
                        end end,
                    PendingReaders),
                 lager:warning("~w diff is ~w", [TxId, TxCommitTime-_Time]),
                true = ets:insert(PreparedTxs, {Key, LastReader}),
                max(TxCommitTime-_Time, DiffInTime);
            [] ->
                DiffInTime;
            Record ->
                lager:error("Something is wrong!!! ~w ~w", [TxId, TxCommitTime, Record]),
                DiffInTime
        end,
    ets:insert(CommittedTxs, {Key, TxCommitTime}),
    update_store(Rest, TxId, TxCommitTime, CommittedTxs, InMemoryStore, PreparedTxs, NewDiff).

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, {nil, 0}};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            {_, ReadValue, Missed} = find_version(ValueList, MyClock, 0),
            {ok, {ReadValue, Missed}}
    end.

%%%%%%%%%%%%%%% Internal %%%%%%%%%%%%%%%
find_version([], _SnapshotTime, Missed) ->
    {ok, nil, Missed};
find_version([{TS, Value}|Rest], SnapshotTime, Missed) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value, Missed};
        false ->
            find_version(Rest, SnapshotTime, Missed+1)
    end.

prepare_logic(TxId, WriteSet, CommittedTxs, PreparedTxs, IfCertify, Sender, Clock, Wait, IfPrecise) ->
    {ok, _, Clock1} = clock_utilities:force_catch_up(Clock, TxId#tx_id.snapshot_time),
    lager:warning("For tx ~w, Clock1 is ~w", [TxId, Clock1]),
    case IfPrecise of
        false ->
            {ok, PrepareTime, Clock2} = clock_utilities:get_prepare_time(Clock1),
            Result = prepare(TxId, WriteSet, CommittedTxs, PreparedTxs, PrepareTime, 
                    IfCertify, IfPrecise),
            case Result of
                ok ->
                    riak_core_vnode:reply(Sender, {prepared, PrepareTime, Wait});
                {error, write_conflict} ->
                    riak_core_vnode:reply(Sender, abort)
            end,
            Clock2;
        true ->
            Result = prepare(TxId, WriteSet, CommittedTxs, PreparedTxs, 0, 
                    IfCertify, IfPrecise),
            case Result of
                {ok, ToPrepTime} ->
                    lager:warning("~w passed prepare", [TxId]),
                    IfPrecise = true,
                    riak_core_vnode:reply(Sender, {prepared, ToPrepTime, Wait}),
                    %% No aggr clock available, giving 0 does not affect. 
                    {ok, 0, Clock2} = clock_utilities:catch_up(Clock, ToPrepTime, 0),
                    Clock2;
                {error, write_conflict} ->
                    lager:warning("~w failed prepare", [TxId]),
                    riak_core_vnode:reply(Sender, abort),
                    Clock1
            end
    end.
