open DataTable
open Common

module Database : Database = struct
  type t = {
    database_id : int;
    mutable database_name : string;
    mutable tables : DataTable.t list;
  }

  let create ?(database_id=0) () =
    {
      database_id;
      database_name = "";
      tables = [];
    }

  let add_table t table is_catalog =
    t.tables <- table :: t.tables;

  let get_table_with_id t table_id =
    let rec find_table tables =
      match tables with
      | [] -> None
      | table :: rest ->
          if table.oid = table_id then Some table
          else find_table rest
    in
    find_table t.tables

  let drop_table_with_id t table_id =
    t.tables <- List.filter (fun table -> table.oid != table_id) t.tables

  let get_table t ~table_offset =
    if table_offset < List.length t.tables then Some (List.nth t.tables table_offset)
    else None

  let get_table_count t = List.length t.tables

  let get_info t =
    let buffer = Buffer.create 256 in
    Buffer.add_string buffer "===========================\n";
    Buffer.add_string buffer "DATABASE:\n";
    Buffer.add_string buffer (Printf.sprintf "Database ID: %d\n" t.database_id);
    Buffer.add_string buffer (Printf.sprintf "Database Name: %s\n" t.database_name);
    Buffer.add_string buffer (Printf.sprintf "Table Count: %d\n" (get_table_count t));
    Buffer.add_string buffer "Tables:\n";
    List.iteri (fun i table ->
      Buffer.add_string buffer (Printf.sprintf "Table %d: %s\n" (i + 1) table.name)
    ) t.tables;
    Buffer.add_string buffer "===========================\n";
    Buffer.contents buffer

  let get_name t = t.database_name

  let set_name t name = t.database_name <- name
end