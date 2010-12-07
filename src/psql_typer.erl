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
-module(psql_typer).

-author('martin@erlang-solutions.com').

-behaviour(gen_server).
-export([start_link/0]).
-export([install_type/2, delete_type/1, install_std/0]).
-export([convert/2, oid_from_rowdesc/1]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([code_change/3]).

%% Installs standard converters
install_std() ->
	install_type(16, fun bool_from_bin/2),			%% BOOL
	install_type(17, fun bin_from_bin/2),			%% BYTE ARRAY
	install_type(20, fun int_from_bin/2),			%% INT 8
	install_type(21, fun int_from_bin/2),			%% INT 2
	install_type(23, fun int_from_bin/2),			%% INT 4
	install_type(1007, fun intarray_from_bin/2),	%% INT 4
	install_type(18, fun char_from_bin/2),			%% CHAR
	install_type(19, fun varchar_from_bin/2),		%% NAME
	install_type(25, fun varchar_from_bin/2),		%% TEXT
	install_type(1042, fun bpchar_from_bin/2),		%% CHAR ARRAY
	install_type(1043, fun varchar_from_bin/2),		%% VARCHAR
	install_type(700, fun float_from_bin/2),		%% FLOAT4
	install_type(701, fun float_from_bin/2),		%% FLOAT8
	install_type(1082, fun date_from_bin/2),		%% DATE
	install_type(1083, fun time_from_bin/2),		%% TIME
	install_type(1114, fun datetime_from_bin/2),	%% TIMESTAMP
	install_type(2278, fun void_from_bin/2),		%% VOID
	install_type(sql_byte_array, fun bin_to_bin/2),
	install_type(sql_bool, fun bool_to_bin/2),
	install_type(sql_int, fun int_to_bin/2),
	install_type(sql_char, fun char_to_bin/2),
	install_type(sql_float, fun float_to_bin/2),
	install_type(sql_char_array, fun bpchar_to_bin/2),
	install_type(sql_varchar, fun varchar_to_bin/2),
	install_type(sql_date, fun date_to_bin/2),
	install_type(sql_time, fun time_to_bin/2),
	install_type(sql_datetime, fun datetime_to_bin/2).

install_type(Type, Conv) when is_integer(Type); is_atom(Type) ->
	case ets:insert_new(?MODULE, {Type, Conv}) of
		true -> ok;
		false -> {error, exits}
	end.

delete_type(Type) when is_integer(Type); is_atom(Type) ->
	ets:delete(?MODULE, Type).

convert(Type, sql_null) when is_integer(Type); is_atom(Type) ->
	sql_null;
convert(Type, Data) when is_integer(Type); is_atom(Type) ->
	case ets:lookup(?MODULE, Type) of
		[{Type, Conv}] -> Conv(Type, Data);
		[] -> {error, {bad_type, Type}}
	end.

oid_from_rowdesc({_, _, _, OID, _, _, _}) ->
	OID.

start_link() ->
	gen_server:start_link(?MODULE, [], []).

init([]) ->
	process_flag(trap_exit, true),
	case ets:file2tab(filename()) of
		{ok, Tab} ->
			{ok, Tab};
		{error, _} ->
			Tab = ets:new(?MODULE, [named_table, public, ordered_set]),
			install_std(),
			{ok, Tab}
	end.

handle_call(_Req, _From, State) ->
	{reply, ok, State}.

handle_cast(_Req, State) ->
	{noreply, State}.

handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_, Tab) ->
	ets:tab2file(Tab, filename()).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

filename() ->
	case code:priv_dir(psql) of
		{error, _} -> "typer";
		PrivDir -> filename:join(PrivDir, "typer")
	end.

%% Standard converters
%% To psql text format
bin_to_bin(_, B) -> B.

int_to_bin(_, N) -> list_to_binary(integer_to_list(N)).

char_to_bin(_, C) -> list_to_binary([C]).

float_to_bin(_, N) when is_float(N) -> list_to_binary(float_to_list(N));
float_to_bin(_, N) -> list_to_binary(integer_to_list(N)).

varchar_to_bin(_, VC) -> iolist_to_binary(VC).

bpchar_to_bin(_, VC) -> iolist_to_binary([$", VC, $"]).

bool_to_bin(_, true) -> <<"t">>;
bool_to_bin(_, false) -> <<"f">>.

date_to_bin(_, {Year, Month, Day}) ->
	iolist_to_binary([
		string:right(integer_to_list(Year), 4, $0), "-",
		string:right(integer_to_list(Month), 2, $0), "-",
		string:right(integer_to_list(Day), 2, $0)
	]).

time_to_bin(_, {Hour, Minute, Second}) ->
	iolist_to_binary([
		string:right(integer_to_list(Hour), 2, $0), ":",
		string:right(integer_to_list(Minute), 2, $0), ":",
		string:right(integer_to_list(Second), 2, $0)
	]).

datetime_to_bin(T, {Date, Time}) ->
	iolist_to_binary([date_to_bin(T, Date), " ",
		time_to_bin(T, Time)]).

%% From psql text format
bin_from_bin(_, B) -> B.

int_from_bin(_, N) -> list_to_integer(binary_to_list(N)).

char_from_bin(_, <<C:8>>) -> C.

float_from_bin(_, N) ->
	StrN = binary_to_list(N),
	case string:chr(StrN, $.) of
		0 -> list_to_integer(StrN) * 1.0;
		_ -> list_to_float(StrN)
	end.

varchar_from_bin(_, Bin) -> binary_to_list(Bin).

bpchar_from_bin(_, <<C:8>>) -> C;
bpchar_from_bin(_, Bin) ->
	string:strip(string:strip(binary_to_list(Bin)), both, $").

bool_from_bin(_, <<"t">>) -> true;
bool_from_bin(_, <<"f">>) -> false.

void_from_bin(_, <<>>) -> sql_void.

%% Note the following functions are limited to one representation
%% of date and time
%% YYYY-MM-DD
%% HH:MM:SS
%% TODO: Add checks for valid date/time
date_from_bin(_, Date) ->
	strdate_to_tuple(binary_to_list(Date)).

time_from_bin(_, Time) ->
	strtime_to_tuple(binary_to_list(Time)).

datetime_from_bin(_, DateTime) ->
	[Date,Time] = string:tokens(binary_to_list(DateTime), " "),
	{strdate_to_tuple(Date), strtime_to_tuple(Time)}.

%% NOTE: this function discards timezones and fractions of seconds
strtime_to_tuple(Time) ->
	[Hr,Mn,Sc|_] = string:tokens(Time, ":.+"),
	{list_to_integer(Hr),
		list_to_integer(Mn),
		list_to_integer(Sc)}.

strdate_to_tuple(Date) ->
	[Yr,Mh,Dy] = string:tokens(Date, "-"),
	{list_to_integer(Yr),
		list_to_integer(Mh),
		list_to_integer(Dy)}.

intarray_from_bin(_, N)->
    intarray_from_string(binary_to_list(N)).

%% String format "{1,2,3,..}"
intarray_from_string([${|Rest]) ->
    intarray_from_string(Rest, []).

intarray_from_string([], Acc) ->
    lists:reverse(Acc);
intarray_from_string([_|Rest1]=N, Acc) ->
    case string:to_integer(N) of
        {error, _Reason} ->
            intarray_from_string(Rest1, Acc);
        {Value, Rest2} ->
            intarray_from_string(Rest2, [Value|Acc])
    end.
