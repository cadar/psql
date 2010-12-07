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
-module(psql_utils).

-author('martin@erlang-solutions.com').

-export([retrive_response/2, convert_rows/2, md5auth/3, md5/1]).

retrive_response(Con, Timeout) ->
	case psql_socket:recv(Con, Timeout) of
		{error, timeout} ->
			{error, timeout};
		{ok, Packet} ->
			case psql_parser:parse(Packet) of
				{incomplete, Acc, Cont} ->
					retrive_response(Con, Timeout, Acc, Cont);
				{Meta, DataSet} ->
					{Meta, DataSet}
			end
	end.
retrive_response(Con, Timeout, Acc, Cont) ->
	case psql_socket:recv(Con, Timeout) of
		{error, timeout} ->
			{error, timeout};
		{ok, Packet} ->
			case psql_parser:parse(<<Acc/binary, Packet/binary>>, Cont) of
				{incomplete, NewAcc, NewCont} ->
					retrive_response(Con, Timeout, NewAcc, NewCont);
				{Meta, DataSet} ->
					{Meta, DataSet}
			end
	end.

convert_rows(Desc, Rows) ->
	lists:map(fun(Row) -> convert_row(Desc, Row) end, Rows).

convert_row(Desc, Row) ->
	list_to_tuple(lists:map(fun(N) ->
		Type = psql_typer:oid_from_rowdesc(element(N, Desc)),
		case psql_typer:convert(Type, element(N, Row)) of
			{error, {bad_type, Type}} ->
				error_logger:warning_report({psql_typer, {bad_type, Type}}),
				element(N, Row);
			Converted ->
				Converted
		end
	end, lists:seq(1, tuple_size(Row)))).

md5auth(Username, Password, Salt) ->
	Auth = md5([md5([Password, Username]), Salt]),
	<<"md5", Auth/binary>>.

md5(Data) ->
	list_to_binary(to_hex(erlang:md5(Data))).

to_hex(<<H:4, L:4, Rest/binary>>) ->
	[hex(H), hex(L) | to_hex(Rest)];
to_hex(<<>>) ->
	[].

hex(Dec) when Dec < 10 -> $0 + Dec;
hex(Dec) -> $a - 10 + Dec.
