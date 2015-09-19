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
-module(update_object).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([update/3, update/2]).

update(increment, Param)->
    Param;
update(decrement, Param)->
    0-Param;
update(assign, Param)->
    Param;
update(add, Param)->
    [Param];
update(remove, _Param)->
    [].

update(Object, increment, Param)->
    case Object of
        nil -> update(increment, Param);
        _ -> Object+Param
    end;
update(Object, decrement, Param)->
    case Object of
        nil -> update(decrement, Param);
        _ -> Object-Param
    end;
update(Object, add, Param)->
    case Object of 
        nil -> update(add, Param);
        _ -> Object++[Param]
    end;
update(Object, remove, Param)->
    case Object of 
        nil -> update(remove, Param);
        _ -> lists:delete(Param, Object)
    end;
update(_Object, assign, Param)->
    Param.


