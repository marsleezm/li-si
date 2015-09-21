-module(test_utilities).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([test_dict/2,
         test_ets/2]).


test_dict(Size, Time) ->
    Time1 = partition_vnode:now_microsec(),
    List = lists:seq(1, Size),
    Dict = dict:new(),
    FinalDict = lists:foldl(fun(X, NewDict) -> dict:store(X, X, NewDict) end, Dict, List),
    Time2 = partition_vnode:now_microsec(),
    lager:info("Initialization used: ~w",[Time2-Time1]),
    iterate_dict(FinalDict, Time, 0),
    Time3 = partition_vnode:now_microsec(),
    lager:info("Iteration used: ~w",[Time3-Time2]).


iterate_dict(_, 0, Acc) ->
    Acc;
iterate_dict(Dict, Num, Acc) ->
    V = dict:fetch(Num, Dict),
    iterate_dict(Dict, Num-1, Acc+V).

test_ets(Size, Time) ->
    Time1 = partition_vnode:now_microsec(),
    List = lists:seq(1, Size),
    Ets =ets:new(test_ets,
        [set,protected,named_table]),
    _ = lists:map(fun(X) -> ets:insert(Ets, {X, X}) end, List),
    Time2 = partition_vnode:now_microsec(),
    lager:info("Initialization used: ~w",[Time2-Time1]),
    iterate_ets(Ets, Time, 0),
    Time3 = partition_vnode:now_microsec(),
    lager:info("Iteration used: ~w",[Time3-Time2]),
    ets:delete(Ets).

iterate_ets(_, 0, Acc) ->
    Acc;
iterate_ets(Ets, Num, Acc) ->
    [{Num, V}] = ets:lookup(Ets, Num),
    iterate_ets(Ets, Num-1, Acc+V).

