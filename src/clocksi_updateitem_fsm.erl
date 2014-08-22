-module(clocksi_updateitem_fsm).

-behavior(gen_fsm).

-include("floppy.hrl").

%% API
-export([start_link/3]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([check_clock/2, update_item/2, waiting/2]).

-record(state,
        {partition, vclock, coordinator}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Coordinator, Tx, Vclock) ->
    gen_fsm:start_link(?MODULE, [Coordinator, Tx, Vclock], []).

%%%===================================================================
%%% States
%%%===================================================================

init([Coordinator, VecSnapshotTime, Partition]) ->
    SD = #state{partition=Partition, coordinator=Coordinator,
                vclock=VecSnapshotTime},
    {ok, check_clock, SD, 0}.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behinf, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
check_clock(timeout, SD0=#state{vclock=Vclock}) ->
    DcId = dc_utilities:get_my_dc_id(),
    {ok, T_TS} = vectorclock:get_clock_of_dc(DcId, Vclock),
    Time = now_milisec(erlang:now()),
    Newclock = dict:erase(DcId, Vclock),
    case T_TS > Time of
        true ->
            timer:sleep(T_TS - Time),
            {next_state, waiting, SD0#state{vclock=Newclock}, 0};
        false ->
            {next_state, waiting, SD0#state{vclock=Newclock}, 0}
    end.
waiting(timeout, SD0 = #state{vclock=Vclock, partition=Partition}) ->
    {ok, LocalVclock} = vectorclock:get_clock(Partition),
    case vectorclock:is_greater_than(LocalVclock, Vclock) of
        true ->
            {next_state, update_item, SD0, 0};
        false  ->
            %% wait
            {next_state, waiting, SD0, 10}
    end.

%% @doc simply finishes the fsm.
update_item(timeout, SD0=#state{coordinator=Coordinator}) ->
    riak_core_vnode:reply(Coordinator, ok),
    {stop, normal, SD0}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

now_milisec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.