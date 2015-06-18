-module(antidote_config).

-export([load/1,
         set/2,
         get/1, get/2]).

%% ===================================================================
%% Public API
%% ===================================================================

load(Files) ->
    TermsList =
        [ case file:consult(File) of
              {ok, Terms} ->
                  Terms;
              {error, Reason} ->
                  erlang:error("Failed to parse config file ~s: ~p\n", [File, Reason])
          end || File <- Files ],
    load_config(lists:append(TermsList)).

set(Key, Value) ->
    ok = application:set_env(basho_bench, Key, Value).

get(Key) ->
    case application:get_env(basho_bench, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            erlang:error("Missing configuration key", [Key])
    end.

get(Key, Default) ->
    case application:get_env(basho_bench, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

load_config([]) ->
    ok;
load_config([{Key, Value} | Rest]) ->
    ?MODULE:set(Key, Value),
    load_config(Rest);
load_config([ Other | Rest]) ->
    io:format("Ignoring non-tuple config value: ~p\n", [Other]),
    load_config(Rest).
