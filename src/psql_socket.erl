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
-module(psql_socket).

-author('martin@erlang-solutions.com').

-export([connect/2]).
-export([send/2, recv/2]).
-export([close/1]).

connect(IP, Port) ->
	gen_tcp:connect(IP, Port, [binary, {active, false},{nodelay,true}]).

send(Socket, Packet) ->
	gen_tcp:send(Socket, Packet).

recv(Socket, Timeout) ->
	gen_tcp:recv(Socket, 0, Timeout).

close(Socket) ->
	gen_tcp:close(Socket).

