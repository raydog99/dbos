open Core
open Database
open Data_table
open Tile_group

type t = {
  mutable databases: Database.t list;
  tile_group_locator: (int, TileGroup.t) Hashtbl.t;
}

let empty_tile_group : TileGroup.t option ref = ref None

let create () =
  { databases = [];
    tile_group_locator = Hashtbl.create (module Int) }

let instance = lazy (create ())

let get_instance () = Lazy.force instance

let get_database_with_oid t database_oid =
  match List.find t.databases ~f:(fun db -> Database.get_oid db = database_oid) with
  | Some db -> db
  | None -> 
      raise (Failure ("Database with oid = " ^ Int.to_string database_oid ^ " is not found"))

let get_table_with_oid t database_oid table_oid =
  let database = get_database_with_oid t database_oid in
  Database.get_table_with_oid database table_oid

let get_index_with_oid t database_oid table_oid index_oid =
  let table = get_table_with_oid t database_oid table_oid in
  DataTable.get_index_with_oid table index_oid

let get_database_with_offset t database_offset =
  if database_offset < List.length t.databases then
    List.nth_exn t.databases database_offset
  else
    failwith "Database offset out of range"

let has_database t db_oid =
  List.exists t.databases ~f:(fun db -> Database.get_oid db = db_oid)

let destroy_databases t =
  Log.trace (fun m -> m "Deleting databases");
  t.databases <- [];
  Log.trace (fun m -> m "Finish deleting database")

let remove_database_from_storage_manager t database_oid =
  let (removed, remaining) = List.partition_tf t.databases ~f:(fun db -> 
    Database.get_oid db = database_oid
  ) in
  t.databases <- remaining;
  not (List.is_empty removed)

let add_tile_group t oid tile_group =
  Hashtbl.set t.tile_group_locator ~key:oid ~data:tile_group

let drop_tile_group t oid =
  Hashtbl.remove t.tile_group_locator oid

let get_tile_group t oid =
  match Hashtbl.find t.tile_group_locator oid with
  | Some tile_group -> Some tile_group
  | None -> !empty_tile_group

let clear_tile_group t =
  Hashtbl.clear t.tile_group_locator

let add_database t db =
  t.databases <- db :: t.databases

let get_database_count t =
  List.length t.databases

let get_tile_group_count t =
  Hashtbl.length t.tile_group_locator