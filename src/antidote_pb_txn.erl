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
-module(antidote_pb_txn).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/antidote_pb.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #fpbtxnreq{} ->
            {ok, Msg, {"antidote.generaltxn",<<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbtxnreq{ops = Ops}, State) ->
    Updates = decode_general_txn(Ops),
    case antidote:execute_tx(Updates) of
        {error, _Reason} ->
            {reply, #fpbtxnresp{success = false}, State};
        {ok, {_Txid, ReadSet, CommitTime}} ->
            ReadReqs = lists:filter(fun(Op) -> case Op of 
                            {update, _, _, _} -> false; {read, _} -> true end end, Updates),
            Zipped = lists:zip(ReadReqs, ReadSet), 
            Reply = encode_general_txn_response(Zipped),
            {reply, #fpbtxnresp{success=true,
                                            clock= term_to_binary(CommitTime),
                                            results=Reply}, State}
    end.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.

decode_general_txn(Ops) ->
    lists:map(fun(Op) -> decode_general_txn_op(Op) end, Ops). 
    
decode_general_txn_op(#fpbtxnop{type=0, key=Key, operation=Op, parameter=Param}) ->
    {update, Key, get_op_by_id(Op), binary_to_term(Param)};
decode_general_txn_op(#fpbtxnop{type=1, key=Key}) ->
    {read, Key}.

encode_general_txn_response(Zipped) ->
    lists:map(fun(Resp) ->
                      encode_general_txn_read_resp(Resp)
              end, Zipped).

encode_general_txn_read_resp({{read, Key}, Result}) ->
    #fpbvalue{key=Key, value=term_to_binary(Result)}.

get_op_by_id(0) ->
    increment;
get_op_by_id(1) ->
    decrement;
get_op_by_id(2) ->
    assign;
get_op_by_id(3) ->
    add;
get_op_by_id(4) ->
    remove.
