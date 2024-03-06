module StorageManager = struct
  type t = {
    tables : (string, Table.t) Hashtbl.t;
    views : (string, LQPView.t) Hashtbl.t;
    prepared_plans : (string, PreparedPlan.t) Hashtbl.t;
  }

  let create () =
    {
      tables = Hashtbl.create 100;
      views = Hashtbl.create 100;
      prepared_plans = Hashtbl.create 100;
    }

  let add_table storage_manager name table =
    Hashtbl.replace storage_manager.tables name table

  let drop_table storage_manager name =
    Hashtbl.remove storage_manager.tables name

  let get_table storage_manager name =
    try Some (Hashtbl.find storage_manager.tables name)
    with Not_found -> None

  let has_table storage_manager name =
    Hashtbl.mem storage_manager.tables name

  let table_names storage_manager =
    Hashtbl.fold (fun name _ acc -> name :: acc) storage_manager.tables []

  let tables storage_manager =
    Hashtbl.fold (fun name table acc -> (name, table) :: acc) storage_manager.tables []

  let add_view storage_manager name view =
    Hashtbl.replace storage_manager.views name view

  let drop_view storage_manager name =
    Hashtbl.remove storage_manager.views name

  let get_view storage_manager name =
    try Some (Hashtbl.find storage_manager.views name)
    with Not_found -> None

  let has_view storage_manager name =
    Hashtbl.mem storage_manager.views name

  let view_names storage_manager =
    Hashtbl.fold (fun name _ acc -> name :: acc) storage_manager.views []

  let views storage_manager =
    Hashtbl.fold (fun name view acc -> (name, view) :: acc) storage_manager.views []

  let add_prepared_plan storage_manager name prepared_plan =
    Hashtbl.replace storage_manager.prepared_plans name prepared_plan

  let get_prepared_plan storage_manager name =
    try Some (Hashtbl.find storage_manager.prepared_plans name)
    with Not_found -> None

  let has_prepared_plan storage_manager name =
    Hashtbl.mem storage_manager.prepared_plans name

  let drop_prepared_plan storage_manager name =
    Hashtbl.remove storage_manager.prepared_plans name

  let prepared_plans storage_manager =
    Hashtbl.fold (fun name plan acc -> (name, plan) :: acc) storage_manager.prepared_plans

  let export_all_tables_as_csv storage_manager path =
    Printf.printf "Exporting tables to CSV at path: %s\n" path
end