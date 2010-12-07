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
-module(psql_parser).

-author('martin@erlang-solutions.com').

-export([parse/1, parse/2]).

%% Parses a psql response
parse(Packet) ->
	parse(Packet, {[], []}).

parse(<<ID, Size:32, T/binary>> = Bin, {Meta, DataSet}) ->
	PacketSize = Size - 4,
	case T of
		<<Packet:PacketSize/binary, Rest/binary>> ->
			case parse_packet(ID, Packet) of
				{datarow, DR} -> parse(Rest, {Meta, [DR|DataSet]});
				MetaData -> parse(Rest, {[MetaData|Meta], DataSet})
			end;
		T ->
			{incomplete, Bin, {Meta, DataSet}}
	end;
parse(<<>>, {Meta, DataSet}) when Meta /= [] ->
	{lists:reverse(Meta), lists:reverse(DataSet)};
parse(Rest, Continuation) ->
	{incomplete, Rest, Continuation}. 



%% Parses the ID byte of the protocol
%% Subparsers parses each individual packet.
%% Datasets are NOT automatically converted
parse_packet($R, Token) ->
	{authentication, parse_authentication(Token)};
parse_packet($K, KeyData) ->
	{backend_keydata, parse_backend_keydata(KeyData)};
parse_packet($S, Param) ->
	{parameter, parse_parameter(Param)};
parse_packet($1, <<>>) ->
	parse_complete;
parse_packet($2, <<>>) ->
	bind_complete;
parse_packet($n, <<>>) ->
	no_data;
parse_packet($C, Command) ->
	{command_complete, parse_command_complete(Command)};
parse_packet($D, Row) ->
	{datarow, parse_datarow(Row)};
parse_packet($T, Description) ->
	{row_description, parse_row_description(Description)}; 
parse_packet($Z, Status) ->
	{ready_for_query, parse_ready_for_query(Status)};
parse_packet($N, Notice) ->
	{notice, parse_notice(Notice)};
parse_packet($H, CopyOutResponse) ->
    ok = parse_copy_out_response(CopyOutResponse),
    no_data;
parse_packet($d, CopyData) ->
    {datarow, parse_copydata(CopyData)};
parse_packet($c, <<>>) ->
    no_data;
parse_packet($E, Reason) ->
	{error, parse_error(Reason)}.

%% Parses authentication packets
parse_authentication(<<0:32>>) -> ok;
parse_authentication(<<2:32>>) -> kerberos_v5;
parse_authentication(<<3:32>>) -> clear_text;
parse_authentication(<<4:32, Salt/binary>>) -> {crypt, Salt};
parse_authentication(<<5:32, Salt/binary>>) -> {md5, Salt};
parse_authentication(<<6:32>>) -> scm;
parse_authentication(<<7:32>>) -> gssapi;
parse_authentication(<<9:32>>) -> sspi;
parse_authentication(<<8:32, Data/binary>>) -> {gssapi_continue, Data}.

%% Parses parameter packets: {Parameter, Value}
parse_parameter(Param) ->
	list_to_tuple(string:tokens(binary_to_list(Param), [0])).

%% Parses backend keydata: {PID, Secret}
parse_backend_keydata(<<PID:32, KeyData/binary>>) ->
	{PID, KeyData}.

%% Parses the command: Command | {Command, Rows}
parse_command_complete(Command) ->
	case string:tokens(binary_to_list(Command), [$ , 0]) of
		["INSERT", OID, Rows] ->
			{insert, list_to_integer(OID), list_to_integer(Rows)};
		["DELETE", Rows] ->
			{delete, list_to_integer(Rows)};
		["UPDATE", Rows] ->
			{update, list_to_integer(Rows)};
		["MOVE", Rows] ->
			{move, list_to_integer(Rows)};
		["FETCH", Rows] ->
			{fetch, list_to_integer(Rows)};
		["COPY", Rows] ->
			{copy, list_to_integer(Rows)};
		[Cmd, Arg] ->
			list_to_atom(lists:concat([
				string:to_lower(Cmd), "_", string:to_lower(Arg)
			]));
		[Cmd] ->
			list_to_atom(string:to_lower(Cmd))
	end.

%% LIMITATION only textual format is supported
parse_copydata(<<DataRow/binary>>) -> 
    binary_to_list(DataRow).

%% LIMITATION only textual format is supported
parse_copy_out_response(<<0:8, _NumColumns:16, 
                         _FormatCodes/binary>>) ->
    ok;
parse_copy_out_response(<<1:8, _Rest/binary>>) ->
    throw({error, binary_copy_not_supported}).

parse_datarow(<<Col:16, Cols/binary>>) ->
	parse_datarow(Col, Cols, []).
parse_datarow(0, <<>>, Acc) ->
	list_to_tuple(lists:reverse(Acc));
parse_datarow(Col, <<-1:32/signed-integer, Rest/binary>>, Acc) ->
	parse_datarow(Col - 1, Rest, [sql_null|Acc]);
parse_datarow(Col, <<Size:32, Data:(Size)/binary, Rest/binary>>, Acc) ->
	parse_datarow(Col - 1, Rest, [Data|Acc]).

parse_row_description(<<Col:16, Description/binary>>) ->
	parse_row_description(Col, Description, []).
parse_row_description(0, <<>>, Acc) ->
	list_to_tuple(lists:reverse(Acc));
parse_row_description(Col, Description, Acc) ->
	NameSize = strlen(Description),
	<<Name:(NameSize)/binary, 0:8, TID:32, Att:16, OID:32, LenSize:16,
		Mod:32, Format:16, Rest/binary>> = Description,
	parse_row_description(Col - 1, Rest,
		[{binary_to_list(Name), TID, Att, OID, LenSize, Mod, Format}|Acc]).


%% Parses error packets: [{Field, Value}]
parse_error(Reason) ->
	[{parse_error_field(hd(S)), tl(S)} 
		|| S <- string:tokens(binary_to_list(Reason), [0])].

%% Parses notices
parse_notice(<<0:8>>) ->
	[];
parse_notice(Notice) ->
	[{parse_error_field(hd(S)), tl(S)} 
		|| S <- string:tokens(binary_to_list(Notice), [0])].

%% Parses Error fields: atom
parse_error_field($S) -> severity;
parse_error_field($C) -> code;
parse_error_field($M) -> message;
parse_error_field($D) -> description;
parse_error_field($H) -> hint;
parse_error_field($P) -> position;
parse_error_field($p) -> internal_position;
parse_error_field($q) -> internal_query;
parse_error_field($W) -> where;
parse_error_field($F) -> file;
parse_error_field($L) -> line;
parse_error_field($R) -> routine.

parse_ready_for_query(<<"I">>) -> idle;
parse_ready_for_query(<<"T">>) -> transaction;
parse_ready_for_query(<<"E">>) -> failed.

strlen(Bin) ->
	strlen(0, Bin).
strlen(Offset, Bin) ->
	case Bin of
		<<_:(Offset)/binary, 0:8, _/binary>> ->
			Offset;
		Bin ->
			strlen(Offset + 1, Bin)
	end.

