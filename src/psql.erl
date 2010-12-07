%%==============================================================================
%% Copyright 2010 Erlang Solutions Ltd.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================
-module(psql).

-author('martin@erlang-solutions.com').

-behaviour(application).
-export([start/2, stop/1]).
-export([connect/2, close/1, authenticate/3, authenticate_md5/4]).
-export([q/3]).
-export([
	parse/2,
	parse/4,
	bind/2,
	bind/4,
	describe/1,
	describe/3,
	execute/1,
	execute/3,
	sync/1,
	retrive_result/1,
	retrive_result/2,
	retrive_result_raw/2,
	convert_parameters/1
]).
-import(proplists, [get_value/2, get_value/3]).

start(_, _) ->
	psql_sup:start_link().

stop(_) ->
	ok.

connect(IP, Port) ->
	psql_socket:connect(IP, Port).

close(Con) ->
	psql_socket:send(Con, psql_proto:terminate()),
	psql_socket:close(Con).

authenticate(Con, Username, Database) ->
	ok = psql_socket:send(Con, psql_proto:startup_message(
		[{"user", Username}, {"database", Database}])),
	{Meta, _} = psql_utils:retrive_response(Con, infinity),
	case get_value(authentication, Meta) of
		undefined -> {error, get_value(error, Meta)};
		ok -> ok;
		Continue -> Continue
	end.

authenticate_md5(Con, Username, Password, Continuation) ->
	ok = psql_socket:send(Con, psql_proto:password_message(
		psql_utils:md5auth(Username, Password, Continuation))),
	{Meta, _} = psql_utils:retrive_response(Con, infinity),
	case get_value(authentication, Meta) of
		undefined -> {error, get_value(error, Meta)};
		ok -> ok
	end.

q(Con, Query, Parameters) ->
	ok = parse(Con, Query),
	ok = bind(Con, convert_parameters(Parameters)),
	ok = describe(Con),
	ok = execute(Con),
	ok = sync(Con),
	retrive_result(Con).

parse(Con, Query) ->
	parse(Con, "", Query, []).
parse(Con, Statement, Query, Types) ->
	psql_socket:send(Con, psql_proto:parse(Statement, Query, Types)).

bind(Con, Parameters) ->
	bind(Con, "", "", Parameters).
bind(Con, Statement, Portal, Parameters) ->
	psql_socket:send(Con, psql_proto:bind(
		Statement,
		Portal,
		Parameters
	)).

describe(Con) ->
	describe(Con, portal, "").
describe(Con, Type, Name) when Type == portal; Type == statement ->
	psql_socket:send(Con, psql_proto:describe(Type, Name)).

execute(Con) ->
	execute(Con, "", 0).
execute(Con, Portal, ResultSetSize) ->
	psql_socket:send(Con, psql_proto:execute(Portal, ResultSetSize)).

sync(Con) ->
	psql_socket:send(Con, psql_proto:sync()).

retrive_result(Con) ->
	retrive_result(Con, infinity).

retrive_result(Con, Timeout) ->
	retrive_result(Con, Timeout, {[],[]}).

retrive_result(Con, Timeout, {MetaAcc,DataSetAcc}) ->
	case psql_utils:retrive_response(Con, Timeout) of
		{error, Reason} ->
			{error, Reason};
		{Meta, DataSet} ->
			case process_response(MetaAcc ++ Meta, DataSetAcc ++ DataSet) of
				{error, undefined} ->
					retrive_result(Con, Timeout,
						{MetaAcc ++ Meta, DataSetAcc ++ DataSet});
				Result -> Result
			end
	end.

retrive_result_raw(Con, Timeout) ->
	psql_utils:retrive_response(Con, Timeout).

process_response(Meta, DataSet) ->
	case get_value(command_complete, Meta) of
		undefined ->
			{error, get_value(error, Meta)};
		select ->
			{select, psql_utils:convert_rows(
				get_value(row_description, Meta), DataSet),
				get_value(ready_for_query, Meta)};
		{copy, _Rows} ->
			{copy, DataSet, get_value(ready_for_query, Meta)};
		{Command, OID, Rows} ->
			{Command, OID, Rows, get_value(ready_for_query, Meta)};
		{Command, Rows} ->
			{Command, Rows, get_value(ready_for_query, Meta)};
		Command when is_atom(Command) ->
			{Command, get_value(ready_for_query, Meta)}
	end.

convert_parameters(Params) ->
	lists:map(fun
		({Type, Param}) ->
			case psql_typer:convert(Type, Param) of
				{error, Error} -> exit(Error);
				Value -> Value
			end;
		(Spec) when Spec == sql_null; Spec == sql_default ->
			%% These are special cases...
			Spec
	end, Params).

