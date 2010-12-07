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
-module(psql_proto).

-author('martin@erlang-solutions.com').

-export([
	startup_message/1,
	password_message/1,
	parse/3,
	bind/3,
	describe/2,
	execute/2,
	sync/0,
	terminate/0
]).
-define(PROTOCOL_VERSION, 196608). %% 3.0


startup_message(Parameters) ->
	Packet = <<?PROTOCOL_VERSION:32,
		(list_to_binary([[P, 0, V, 0]
			|| {P, V} <- Parameters]))/binary, 0:8>>,
	<<(byte_size(Packet) + 4):32, Packet/binary>>.

password_message(AuthString) ->
	Packet = <<(iolist_to_binary(AuthString))/binary, 0:8>>,
	<<$p:8, (byte_size(Packet) + 4):32, Packet/binary>>.

parse(Statement, Query, Params) ->
	Packet = <<(iolist_to_binary(Statement))/binary, 0:8,
		(iolist_to_binary(Query))/binary, 0:8,
		(length(Params)):16,
		(iolist_to_binary([<<P:16>> || P <- Params]))/binary>>,
	<<$P:8, (byte_size(Packet) + 4):32, Packet/binary>>.

bind(Portal, Statement, Parameters) ->
	Pair = fun
		(sql_null) -> <<-1:32>>; %% Null case
		(sql_default) -> <<0:32>>; %% Default case
		(P) -> <<(iolist_size(P)):32, (iolist_to_binary(P))/binary>>
	end,
	Packet = <<(iolist_to_binary(Portal))/binary, 0:8,
		(iolist_to_binary(Statement))/binary, 0:8,
		0:16, %% Parameter codes hardcoded to text
		(length(Parameters)):16,
		(iolist_to_binary( [Pair(P) || P <- Parameters]))/binary,
		0:16 %% Result codes hardcoded to text
		>>,
	<<$B, (byte_size(Packet) + 4):32, Packet/binary>>.

describe(Type, Name) ->
	BType = if Type == portal -> $P; true -> $S end,
	Packet = <<BType:8, (iolist_to_binary(Name))/binary, 0:8>>,
	<<$D, (byte_size(Packet) + 4):32, Packet/binary>>.

execute(Portal, MaxSet) ->
	Packet = <<(iolist_to_binary(Portal))/binary, 0:8, MaxSet:32>>,
	<<$E, (byte_size(Packet) + 4):32, Packet/binary>>.

sync() ->
	<<$S, 4:32>>.

terminate() ->
	<<$X, 4:32>>.
