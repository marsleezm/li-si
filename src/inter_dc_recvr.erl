-module(inter_dc_recvr).

-behaviour(gen_server).

%%public API
-export([start/0, replicate/3, stop/1]).

%%gen_server call backs
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%API
start()->
    {ok, PID} = gen_server:start_link(?MODULE, [], []),
    register(inter_dc_recvr, PID).

replicate(Node, Payload, Origin) ->
    io:format("Sending update ~p to ~p ~n",[Payload, Node]),
    gen_server:cast(Node, {replicate, Payload, from, Origin}).

stop(Pid)->
    gen_server:call(Pid, terminate).    

%Server functions
init([]) ->
     {ok, []}.

handle_cast({replicate, Payload, from, {PID,Node}}, _StateData) ->
    apply(Payload),
    gen_server:cast({inter_dc_recvr,Node},{acknowledge,Payload,PID}),
    {noreply,_StateData};
handle_cast({acknowledge, Payload, PID}, _StateData) ->
    inter_dc_repl:acknowledge(PID,Payload),
    {noreply, _StateData}.

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State}.
   
handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Inter_dc_repl_recvr stopping"),
    ok;
terminate(_Reason, _State) -> ok.


code_change(_OldVsn, State, _Extra) ->
    %% No change planned. The function is there for the behaviour,
    %% but will not be used. Only a version on the next
    {ok, State}. 

%private

apply(Payload) ->
    io:format("Recieved update ~p ~n",[Payload]),
    {Key, Op} = Payload,
    proxy:update(Key, Op, self()),
    io:format("Updated operation ~p ~n", [Payload]),
    ok.
