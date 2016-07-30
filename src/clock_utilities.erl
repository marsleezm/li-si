%% -------------------------------------------------------------------
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
-module(clock_utilities).

-include("antidote.hrl").

-export([get_tx_id/2,
         init_clock/1,
         get_and_update_ts/1,
         get_ts_receiving_read/2,
         get_ts_receiving_prepare/2,
         update_ts_prepared_or_committed/2,
         compute_used_time/1,
         now_microsec/0]).

-record(physical_clock, {}).
-record(logical_clock, {next_ts=0, commit_ts=0}).
-record(hybrid_clock, {logical_ts=0}).
-record(hybrid2_clock, {max_ts=0}).

get_tx_id(Operations, CausalClock) ->
    case length(Operations) of
        0 ->
          TxId = tx_utilities:create_transaction_record(CausalClock);
        _ ->
          Key = element(2, hd(Operations)),
          FirstNode = hd(hash_fun:get_preflist_from_key(Key)),
          Ts = partition_vnode:get_and_update_ts(FirstNode, CausalClock),
          TxId = #tx_id{snapshot_time=Ts, server_pid=self()}
    end,
    TxId.

init_clock(ClockType) ->
    case ClockType of
        physical ->
            Clock0 = #physical_clock{};
        logical ->
            Clock0 = #logical_clock{};
        hybrid ->
            Clock0 = #hybrid_clock{};
        hybrid2 ->
            Clock0 = #hybrid2_clock{}
        end,
    Clock0.

get_and_update_ts(Clock) ->
    case Clock of
        #physical_clock{} ->
            TS = now_microsec(),
            Clock0 = Clock;
        #logical_clock{next_ts=NextTS} ->
            TS = NextTS+1,
            Clock0 = Clock#logical_clock{next_ts=TS};
        #hybrid_clock{logical_ts=LogicalTS} ->
            TS = max(now_microsec(), LogicalTS),
            Clock0 = Clock#hybrid_clock{logical_ts=TS};
        #hybrid2_clock{max_ts=MaxTS} ->
            TS = max(now_microsec(), MaxTS),
            Clock0 = Clock#hybrid2_clock{max_ts=TS}
    end,
    {TS, Clock0}.

get_ts_receiving_read(Clock, TxId) ->
    case Clock of
        #physical_clock{} ->
            Clock;
        #logical_clock{next_ts=NextTS} ->
            Clock#logical_clock{next_ts=max(TxId#tx_id.snapshot_time, NextTS)};
        #hybrid_clock{logical_ts=LogicalTS} ->
            Clock#hybrid_clock{logical_ts=max(now_microsec(), max(LogicalTS, TxId#tx_id.snapshot_time))};
        #hybrid2_clock{max_ts=MaxTS} ->
            Clock#hybrid2_clock{max_ts=max(TxId#tx_id.snapshot_time, MaxTS)}
    end.

get_ts_receiving_prepare(Clock, TxId) ->
    case Clock of
        #physical_clock{} ->
            Clock0=Clock,
            PrepareTime0 = now_microsec();
        #logical_clock{next_ts=NextTS} ->
            Clock0 = Clock#logical_clock{next_ts=max(TxId#tx_id.snapshot_time, NextTS)},
            PrepareTime0 = Clock0#logical_clock.next_ts+1;
        #hybrid_clock{logical_ts=LogicalTS} ->
            Clock0 = Clock#hybrid_clock{logical_ts=max(now_microsec(), max(LogicalTS, TxId#tx_id.snapshot_time))},
            PrepareTime0 = Clock#hybrid_clock.logical_ts+1;
        #hybrid2_clock{max_ts=MaxTS} ->
            Clock0 = Clock#hybrid2_clock{max_ts=max(TxId#tx_id.snapshot_time, MaxTS)},
            PrepareTime0 = Clock0#hybrid2_clock.max_ts+1
    end,
    {Clock0, PrepareTime0}.

update_ts_prepared_or_committed(Clock, TS) ->
    case Clock of
      #physical_clock{} ->
          Clock;
      #logical_clock{} ->
          Clock#logical_clock{next_ts=TS};
      #hybrid_clock{} ->
          Clock#hybrid_clock{logical_ts=TS};
      #hybrid2_clock{} ->
          Clock#hybrid2_clock{max_ts=TS}
    end.

compute_used_time(Time) ->
    case Time of
      {T, _} ->
        now_microsec() - T;
      T ->
        now_microsec() - T
      end.

now_microsec() ->
  {MegaSecs, Secs, MicroSecs} = now(),
  (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.