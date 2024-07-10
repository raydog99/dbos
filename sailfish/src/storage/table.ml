open Core
open Schema_type
open Data_table
open Temp_table
open Storage_manager

module Make
    (Schema : Schema)
    (DataTable : DataTable)
    (TempTable : TempTable)
    (StorageManager : StorageManager) = struct

  let invalid_oid = -1

  let get_data_table 
      database_id relation_id schema table_name
      tuples_per_tilegroup_count own_schema adapt_table
      is_catalog layout_type =
    DataTable.create 
      schema table_name database_id relation_id
      tuples_per_tilegroup_count own_schema adapt_table
      is_catalog layout_type

  let get_temp_table schema own_schema =
    TempTable.create invalid_oid schema own_schema

  let drop_data_table database_oid table_oid =
    let storage_manager = StorageManager.get_instance () in
    match StorageManager.get_table_with_oid storage_manager database_oid table_oid with
    | Some _table -> 
        true
    | None -> 
        false

end