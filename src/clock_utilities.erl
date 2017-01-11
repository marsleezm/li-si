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
         get_snapshot_time/1,
         catch_up/2,
         force_catch_up/2,
         get_prepare_time/1, 
         now_microsec/0,
         now_microsec_new/1]).

-record(physical, {last}).
-record(logical, {last}).
-record(hybrid, {physical, logical}).
-record(bravo, {physical, logical}).

get_tx_id(Operations, CausalClock) ->
    case length(Operations) of
        0 ->
          TxId = tx_utilities:create_transaction_record(CausalClock);
        _ ->
          Key = element(2, hd(Operations)),
          FirstNode = hd(hash_fun:get_preflist_from_key(Key)),
          Ts = partition_vnode:get_snapshot_time(FirstNode, CausalClock),
          TxId = #tx_id{snapshot_time=Ts, server_pid=self()}
    end,
    TxId.

init_clock(ClockType) ->
    case ClockType of
        physical ->
            {ok, #physical{last=0}};
        logical ->
            {ok, #logical{last=0}};
        hybrid ->
            {ok, #hybrid{physical=0, logical=0}};
        bravo ->
            {ok, #bravo{physical=0, logical=0}}
    end.

get_snapshot_time(Clock) ->
    case Clock of
        #physical{last=Last} ->
            Now = now_microsec_new(Last),
            {ok, Now, Clock#physical{last=Now}};
        #logical{last=Last} ->
            {ok, Last, Clock};
        #hybrid{physical=Physical0, logical=Logical} ->
            Physical1 = now_microsec_new(Physical0),
            {ok, max(Physical1, Logical), Clock#hybrid{physical=Physical1}};
        #bravo{physical=Physical0, logical=Logical} ->
            Physical1 = now_microsec_new(Physical0),
            {ok, max(Physical1, Logical), Clock#bravo{physical=Physical1}}
    end.

catch_up(Clock, SnapshotTime) ->
    case Clock of
        #physical{last=Last} ->
            Now = now_microsec_new(Last), 
            {ok, SnapshotTime - Now, Clock#physical{last=Now}};
        #logical{last=Last} ->
            {ok, 0, Clock#logical{last=max(Last, SnapshotTime)}};
        #hybrid{physical=_Physical0, logical=Logical} ->
            {ok, 0, Clock#hybrid{logical=max(Logical, SnapshotTime)}};
        #bravo{physical=_Physical0, logical=Logical} ->
            {ok, 0, Clock#bravo{logical=max(Logical, SnapshotTime)}}
    end.

force_catch_up(Clock, SnapshotTime) ->
    case Clock of
        #physical{last=Last} ->
            {ok, Clock#physical{last=max(Last, SnapshotTime)}};
        _ ->
	    {ok, Clock}
    end.

get_prepare_time(Clock) ->
    case Clock of
        #physical{last=Last} ->
            Now = now_microsec_new(Last),
            {ok, Now, Clock#physical{last=Now}};
        #logical{last=Last} ->
            {ok, Last+1, Clock#logical{last=Last+1}};
        #hybrid{physical=Physical0, logical=Logical} ->
            Physical1 = now_microsec_new(Physical0),
            PrepareTime = max(Physical1, Logical+1),
            {ok, PrepareTime, Clock#hybrid{physical=Physical1}};
        #bravo{physical=_Physical0, logical=Logical} ->
            {ok, Logical + 1, Clock}
    end.

now_microsec() ->
  {MegaSecs, Secs, MicroSecs} = now(),
  (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

now_microsec_new(Last) ->
  {MegaSecs, Secs, MicroSecs} = os:timestamp(),
  Now = (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs,
  max(Last+1, Now).
