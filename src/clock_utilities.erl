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

-export([get_tx_id/3,
         init_clock/1,
         get_snapshot_time/1,
         catch_up/2,
         catch_up_if_aggr/2,
         force_catch_up/2,
         get_prepare_time/1, 
         now_microsec/0,
         now_microsec_new/1]).

-record(physical, {last}).
-record(logical, {last}).
-record(aggr_logical, {last}).
-record(hybrid, {physical, logical}).
-record(bravo, {physical, logical}).

get_tx_id(_Operations, ignore, CausalClock) ->
    Partitions = hash_fun:get_partitions(),
    Len = length(Partitions),
    FirstNode = lists:nth(random:uniform(Len), Partitions),
    Ts = partition_vnode:get_snapshot_time(FirstNode, CausalClock),
    lager:warning("Get tx id ignore, Ts is ~w", [Ts]),
    %lager:warning("StartPartId ignore, geting ~w from ~w", [Ts, FirstNode]),
    #tx_id{snapshot_time=Ts, server_pid=self()};
    %case length(Operations) of
    %    0 ->
    %      tx_utilities:create_transaction_record(CausalClock);
    %    _ ->
    %      Key = element(2, hd(Operations)),
    %      FirstNode = hd(hash_fun:get_preflist_from_key(Key)),
    %      Ts = partition_vnode:get_snapshot_time(FirstNode, CausalClock),
    %      #tx_id{snapshot_time=Ts, server_pid=self()}
    %end;
get_tx_id(_Operations, StartPartId, CausalClock) ->
    FirstNode = hd(hash_fun:get_preflist_from_key(StartPartId)),
    Ts = partition_vnode:get_snapshot_time(FirstNode, CausalClock),
    lager:warning("StartPartId is ~w, FirstNode is ~w, CausalClock is ~w, Ts is ~w", [StartPartId, FirstNode, CausalClock, Ts]),
    %lager:warning("StartPartId ~w, geting ~w from ~w", [StartPartId, Ts, FirstNode]),
    #tx_id{snapshot_time=Ts, server_pid=self()}.

init_clock(ClockType) ->
    case ClockType of
        physical ->
            {ok, #physical{last=0}};
        logical ->
            {ok, #logical{last=0}};
        aggr_logical ->
            {ok, #aggr_logical{last=0}};
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
        #aggr_logical{last=Last} ->
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
        #aggr_logical{last=Last} ->
            {ok, 0, Clock#aggr_logical{last=max(Last, SnapshotTime)}};
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

catch_up_if_aggr(MyClock, IncomingClock) ->
    case MyClock of
        #aggr_logical{last=Last} ->
            {ok, MyClock#aggr_logical{last=max(Last, IncomingClock)}};
        _ ->
	    {ok, MyClock}
    end.

get_prepare_time(Clock) ->
    case Clock of
        #physical{last=Last} ->
            Now = now_microsec_new(Last),
            {ok, Now, Clock#physical{last=Now}};
        #logical{last=Last} ->
            {ok, Last+1, Clock#logical{last=Last+1}};
        #aggr_logical{last=Last} ->
            {ok, Last+1, Clock#aggr_logical{last=Last+1}};
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
