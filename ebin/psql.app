{application, psql, 
 [{description, "Postgresql Interface"},
  {vsn, "0.2.0"},
  {modules, [psql, psql_parser, psql_proto, psql_socket, psql_sup, psql_typer, psql_utils]},
  {applications, [kernel, stdlib]},
  {mod, {psql, []}},
  {env, []}
]}.
