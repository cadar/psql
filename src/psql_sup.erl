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
-module(psql_sup).

-author('martin@erlang-solutions.com').

-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link(?MODULE, []).

init([]) ->
	Typer = {typer, {psql_typer, start_link, []},
		permanent, 1000, worker, [psql_typer]},
	{ok, {{one_for_one, 1, 1}, [Typer]}}.

