open ExecutorContext
open Catalog

module CreateExecutor = struct
  let create_executor (node : CreatePlanner) (executor_context : ExecutorContext) : CreateExecutor =
    (node, executor_context)

  let dexecute (node : CreatePlanner) (executor_context : ExecutorContext) : unit =
    print_endline "Executing Create...";
    let create_type = node.get_type in
    let result =
      match create_type with
      | `DB ->
          create_database node
      | `Schema ->
          create_schema node
      | `Table ->
          create_table node
      | `Index ->
          create_index node
      | `Trigger ->
          create_trigger node
      | _ ->
          Printf.printf "Not supported create type %s\n"
    in
    false

  let create_database (node : createPlanner) : Database =
    let database_name = node.get_database_name in
    let txn = node.get_txn in
    let result = Catalog.create_database txn database_name in
    print_endline ("Result is: " ^ (string_of_result result));
    true
end