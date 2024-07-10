open Core
open Data_table
open Index
open Schema
open Foreign_key_constraint

type t = {
  mutable database_oid: int;
  mutable tables: DataTable.t list;
  mutable database_name: string;
  database_mutex: Mutex.t;
}

let create database_oid =
  { database_oid;
    tables = [];
    database_name = "";
    database_mutex = Mutex.create ();
  }

let finalize t =
  Log.trace (fun m -> m "Deleting tables from database");
  List.iter (fun table -> ignore table) t.tables;  
  Log.trace (fun m -> m "Finish deleting tables from database")

let add_table t table ~is_catalog =
  Mutex.lock t.database_mutex;
  t.tables <- table :: t.tables;
  if not is_catalog then
    let gc_manager = GCManagerFactory.get_instance () in
    gc_manager |> GCManager.register_table (DataTable.get_oid table);
  Mutex.unlock t.database_mutex

let get_table_with_oid t table_oid =
  match List.find t.tables ~f:(fun table -> DataTable.get_oid table = table_oid) with
  | Some table -> table
  | None ->
      let msg = Printf.sprintf "Table with oid = %d is not found" table_oid in
      raise (CatalogException msg)

let drop_table_with_oid t table_oid =
  Mutex.lock t.database_mutex;
  let gc_manager = GCManagerFactory.get_instance () in
  gc_manager |> GCManager.deregister_table table_oid;

  QueryCache.get_instance () |> QueryCache.remove table_oid;

  t.tables <- List.filter t.tables ~f:(fun table -> 
    if DataTable.get_oid table = table_oid then
      (ignore table; false)  
    else
      true
  );
  Mutex.unlock t.database_mutex

let get_table t table_offset =
  List.nth t.tables table_offset
  |> Option.value_exn ~message:"Table not found at given offset"

let get_table_count t =
  List.length t.tables

let get_info t =
  let buf = Buffer.create 256 in
  Buffer.add_string buf (Printf.sprintf "%s\nDATABASE(%d) : \n" peloton_getinfo_thick_line t.database_oid);
  let table_count = get_table_count t in
  Buffer.add_string buf (Printf.sprintf "Table Count : %d\n" table_count);

  List.iteri t.tables ~f:(fun i table ->
    Buffer.add_string buf (Printf.sprintf "(%d/%d) Table Oid : %d\n" (i + 1) table_count (DataTable.get_oid table));
    
    let index_count = DataTable.get_index_count table in
    if index_count > 0 then begin
      Buffer.add_string buf (Printf.sprintf "Index Count : %d\n" index_count);
      for index_itr = 0 to index_count - 1 do
        match DataTable.get_index table index_itr with
        | Some index ->
          begin match Index.get_index_type index with
          | PRIMARY_KEY -> Buffer.add_string buf "primary key index \n"
          | UNIQUE -> Buffer.add_string buf "unique index \n"
          | DEFAULT -> Buffer.add_string buf "default index \n"
          end;
          Buffer.add_string buf (Index.to_string index ^ "\n")
        | None -> ()
      done
    end;

    match DataTable.get_schema table with
    | Some schema when Schema.has_foreign_keys schema ->
      Buffer.add_string buf "foreign tables \n";
      List.iter (Schema.get_foreign_key_constraints schema) ~f:(fun foreign_key ->
        let sink_table_oid = ForeignKeyConstraint.get_fk_sink_table_oid foreign_key in
        let sink_table = get_table_with_oid t sink_table_oid in
        Buffer.add_string buf (Printf.sprintf "table name : %s\n" (DataTable.get_name sink_table))
      )
    | _ -> ()
  );

  Buffer.add_string buf peloton_getinfo_thick_line;
  Buffer.contents buf

let get_db_name t = t.database_name

let set_db_name t database_name = 
  t.database_name <- database_name