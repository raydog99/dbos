module Catalog = struct
  type oid = int64

  type catalog_entry = {
    oid: oid;
    name: string;
    kind: [`Table | `Index | `Schema | `Type | `Function];
    schema_oid: oid;
    owner: string;
    created_at: float;
    updated_at: float;
  }

  let catalog_entries : (oid, catalog_entry) Hashtbl.t = Hashtbl.create 1000
  let next_oid = ref 1L

  let generate_oid () =
    let oid = !next_oid in
    next_oid := Int64.add !next_oid 1L;
    oid

  let add_entry entry =
    Hashtbl.add catalog_entries entry.oid entry

  let find_entry oid =
    Hashtbl.find_opt catalog_entries oid

  let find_entry_by_name name =
    Hashtbl.fold (fun _ entry acc ->
      if entry.name = name then Some entry else acc
    ) catalog_entries None

  let update_entry oid update_fn =
    match find_entry oid with
    | Some entry ->
        let updated_entry = update_fn entry in
        Hashtbl.replace catalog_entries oid updated_entry;
        Some updated_entry
    | None -> None

  let delete_entry oid =
    Hashtbl.remove catalog_entries oid

  let list_entries () =
    Hashtbl.fold (fun _ entry acc -> entry :: acc) catalog_entries []
end

module Schema = struct
  type schema = {
    oid: Catalog.oid;
    name: string;
    owner: string;
    tables: Catalog.oid list;
    created_at: float;
    updated_at: float;
  }

  let schemas : (Catalog.oid, schema) Hashtbl.t = Hashtbl.create 100

  let create_schema name owner =
    let oid = Catalog.generate_oid () in
    let timestamp = Unix.gettimeofday () in
    let schema = { oid; name; owner; tables = []; created_at = timestamp; updated_at = timestamp } in
    Hashtbl.add schemas oid schema;
    Catalog.add_entry { oid; name; kind = `Schema; schema_oid = oid; owner; created_at = timestamp; updated_at = timestamp };
    schema

  let find_schema oid =
    Hashtbl.find_opt schemas oid

  let find_schema_by_name name =
    Hashtbl.fold (fun _ schema acc ->
      if schema.name = name then Some schema else acc
    ) schemas None

  let add_table_to_schema schema_oid table_oid =
    match find_schema schema_oid with
    | Some schema ->
        let updated_schema = { schema with tables = table_oid :: schema.tables; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace schemas schema_oid updated_schema;
        true
    | None -> false

  let remove_table_from_schema schema_oid table_oid =
    match find_schema schema_oid with
    | Some schema ->
        let updated_tables = List.filter ((<>) table_oid) schema.tables in
        let updated_schema = { schema with tables = updated_tables; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace schemas schema_oid updated_schema;
        true
    | None -> false

  let list_schemas () =
    Hashtbl.fold (fun _ schema acc -> schema :: acc) schemas []
end

module DataType = struct
  type t =
    | Int
    | Float
    | String of int option
    | Bool
    | Date
    | Timestamp
    | Array of t
    | Custom of string

  let to_string = function
    | Int -> "INTEGER"
    | Float -> "FLOAT"
    | String None -> "TEXT"
    | String (Some n) -> Printf.sprintf "VARCHAR(%d)" n
    | Bool -> "BOOLEAN"
    | Date -> "DATE"
    | Timestamp -> "TIMESTAMP"
    | Array t -> Printf.sprintf "%s[]" (to_string t)
    | Custom name -> name

  let from_string = function
    | "INTEGER" -> Int
    | "FLOAT" -> Float
    | "TEXT" -> String None
    | s when Str.string_match (Str.regexp "VARCHAR(\\([0-9]+\\))") s 0 ->
        String (Some (int_of_string (Str.matched_group 1 s)))
    | "BOOLEAN" -> Bool
    | "DATE" -> Date
    | "TIMESTAMP" -> Timestamp
    | s when Str.string_match (Str.regexp "\\(.*\\)\\[\\]") s 0 ->
        Array (from_string (Str.matched_group 1 s))
    | s -> Custom s
end

module Table = struct
  type column = {
    name: string;
    data_type: DataType.t;
    is_nullable: bool;
    default_value: string option;
  }

  type table = {
    oid: Catalog.oid;
    name: string;
    schema_oid: Catalog.oid;
    columns: column list;
    primary_key: string list;
    indexes: Catalog.oid list;
    constraints: Catalog.oid list;
    created_at: float;
    updated_at: float;
  }

  let tables : (Catalog.oid, table) Hashtbl.t = Hashtbl.create 1000

  let create_table schema_oid name columns primary_key =
    let oid = Catalog.generate_oid () in
    let timestamp = Unix.gettimeofday () in
    let table = { oid; name; schema_oid; columns; primary_key; indexes = []; constraints = []; created_at = timestamp; updated_at = timestamp } in
    Hashtbl.add tables oid table;
    Catalog.add_entry { oid; name; kind = `Table; schema_oid; owner = ""; created_at = timestamp; updated_at = timestamp };
    Schema.add_table_to_schema schema_oid oid;
    table

  let find_table oid =
    Hashtbl.find_opt tables oid

  let find_table_by_name schema_oid name =
    Hashtbl.fold (fun _ table acc ->
      if table.schema_oid = schema_oid && table.name = name then Some table else acc
    ) tables None

  let add_column table_oid column =
    match find_table table_oid with
    | Some table ->
        let updated_columns = table.columns @ [column] in
        let updated_table = { table with columns = updated_columns; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace tables table_oid updated_table;
        true
    | None -> false

  let remove_column table_oid column_name =
    match find_table table_oid with
    | Some table ->
        let updated_columns = List.filter (fun col -> col.name <> column_name) table.columns in
        let updated_table = { table with columns = updated_columns; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace tables table_oid updated_table;
        true
    | None -> false

  let add_index table_oid index_oid =
    match find_table table_oid with
    | Some table ->
        let updated_indexes = index_oid :: table.indexes in
        let updated_table = { table with indexes = updated_indexes; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace tables table_oid updated_table;
        true
    | None -> false

  let remove_index table_oid index_oid =
    match find_table table_oid with
    | Some table ->
        let updated_indexes = List.filter ((<>) index_oid) table.indexes in
        let updated_table = { table with indexes = updated_indexes; updated_at = Unix.gettimeofday () } in
        Hashtbl.replace tables table_oid updated_table;
        true
    | None -> false

  let list_tables () =
    Hashtbl.fold (fun _ table acc -> table :: acc) tables []
end

module Index = struct
  type index_type = BTree | Hash

  type index = {
    oid: Catalog.oid;
    name: string;
    table_oid: Catalog.oid;
    columns: string list;
    index_type: index_type;
    is_unique: bool;
    created_at: float;
    updated_at: float;
  }

  let indexes : (Catalog.oid, index) Hashtbl.t = Hashtbl.create 1000

  let create_index table_oid name columns index_type is_unique =
    let oid = Catalog.generate_oid () in
    let timestamp = Unix.gettimeofday () in
    let index = { oid; name; table_oid; columns; index_type; is_unique; created_at = timestamp; updated_at = timestamp } in
    Hashtbl.add indexes oid index;
    Catalog.add_entry { oid; name; kind = `Index; schema_oid = 0L (* TODO: Get correct schema_oid *); owner = ""; created_at = timestamp; updated_at = timestamp };
    Table.add_index table_oid oid;
    index

  let find_index oid =
    Hashtbl.find_opt indexes oid

  let find_index_by_name table_oid name =
    Hashtbl.fold (fun _ index acc ->
      if index.table_oid = table_oid && index.name = name then Some index else acc
    ) indexes None

  let delete_index oid =
    match find_index oid with
    | Some index ->
        Hashtbl.remove indexes oid;
        Table.remove_index index.table_oid oid;
        Catalog.delete_entry oid;
        true
    | None -> false

  let list_indexes () =
    Hashtbl.fold (fun _ index acc -> index :: acc) indexes []
end

module Constraint = struct
  type constraint_type =
    | PrimaryKey
    | ForeignKey of { ref_table: Catalog.oid; ref_columns: string list }
    | Unique
    | Check of string  (* Check constraint expression *)

  type constraint_def = {
    oid: Catalog.oid;
    name: string;
    table_oid: Catalog.oid;
    constraint_type: constraint_type;
    columns: string list;
    created_at: float;
    updated_at: float;
  }

  let constraints : (Catalog.oid, constraint_def) Hashtbl.t = Hashtbl.create 1000

  let create_constraint table_oid name constraint_type columns =
    let oid = Catalog.generate_oid () in
    let timestamp = Unix.gettimeofday () in
    let constraint_def = { oid; name; table_oid; constraint_type; columns; created_at = timestamp; updated_at = timestamp } in
    Hashtbl.add constraints oid constraint_def;
    match Table.find_table table_oid with
    | Some table ->
        let updated_table = { table with constraints = oid :: table.constraints; updated_at = timestamp } in
        Hashtbl.replace Table.tables table_oid updated_table;
        constraint_def
    | None -> failwith "Table not found"

  let find_constraint oid =
    Hashtbl.find_opt constraints oid

  let find_constraint_by_name table_oid name =
    Hashtbl.fold (fun _ constraint_def acc ->
      if constraint_def.table_oid = table_oid && constraint_def.name = name then Some constraint_def else acc
    ) constraints None

  let delete_constraint oid =
    match find_constraint oid with
    | Some constraint_def ->
        Hashtbl.remove constraints oid;
        (match Table.find_table constraint_def.table_oid with
         | Some table ->
             let updated_constraints = List.filter ((<>) oid) table.constraints in
             let updated_table = { table with constraints = updated_constraints; updated_at = Unix.gettimeofday () } in
             Hashtbl.replace Table.tables constraint_def.table_oid updated_table
         | None -> ());
        true
    | None -> false

  let list_constraints () =
    Hashtbl.fold (fun _ constraint_def acc -> constraint_def :: acc) constraints []
end

module Function = struct
  type function_def = {
    oid: Catalog.oid;
    name: string;
    args: (string * DataType.t) list;
    return_type: DataType.t;
    body: string;
    language: string;
    created_at: float;
    updated_at: float;
  }

  let functions : (Catalog.oid, function_def) Hashtbl.t = Hashtbl.create 100

  let create_function name args return_type body language =
    let oid = Catalog.generate_oid () in
    let timestamp = Unix.gettimeofday () in
    let func = { oid; name; args; return_type; body; language; created_at = timestamp; updated_at = timestamp } in
    Hashtbl.add functions oid func;
    Catalog.add_entry { oid; name; kind = `Function; schema_oid = 0L (* TODO: Get correct schema_oid *); owner = ""; created_at = timestamp; updated_at = timestamp };
    func

  let find_function oid =
    Hashtbl.find_opt functions oid

  let find_function_by_name name =
    Hashtbl.fold (fun _ func acc ->
      if func.name = name then Some func else acc
    ) functions None

  let delete_function oid =
    match find_function oid with
    | Some func ->
        Hashtbl.remove functions oid;
        Catalog.delete_entry oid;
        true
    | None -> false

  let list_functions () =
    Hashtbl.fold (fun _ func acc -> func :: acc) functions []
end

module CatalogManager = struct
  let default_schema_name = "bluefin"
  let default_schema_abbreviation = "bf"

  let initialize () =
    (* Create default schema *)
    let default_schema = Schema.create_schema default_schema_name "system" in
    
    (* Create system catalogs *)
    let create_system_catalogs () =
      let create_system_table name columns =
        match default_schema with
        | Some s -> Table.create_table s.oid name columns []
        | None -> failwith "Default schema not found"
      in
      
      (* Create bf_class equivalent *)
      let _ = create_system_table "bf_class" [
        { name = "oid"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "relname"; data_type = DataType.String (Some 64); is_nullable = false; default_value = None };
        { name = "relnamespace"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "reltype"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "relowner"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "relkind"; data_type = DataType.String (Some 1); is_nullable = false; default_value = None };
        { name = "reltuples"; data_type = DataType.Float; is_nullable = false; default_value = Some "0.0" };
      ] in

      (* Create bf_attribute equivalent *)
      let _ = create_system_table "bf_attribute" [
        { name = "attrelid"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "attname"; data_type = DataType.String (Some 64); is_nullable = false; default_value = None };
        { name = "atttypid"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "attnum"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "attnotnull"; data_type = DataType.Bool; is_nullable = false; default_value = Some "false" };
        { name = "atthasdef"; data_type = DataType.Bool; is_nullable = false; default_value = Some "false" };
      ] in

      (* Create bf_namespace equivalent *)
      let _ = create_system_table "bf_namespace" [
        { name = "oid"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "nspname"; data_type = DataType.String (Some 64); is_nullable = false; default_value = None };
        { name = "nspowner"; data_type = DataType.Int; is_nullable = false; default_value = None };
      ] in

      (* Create bf_type equivalent *)
      let _ = create_system_table "bf_type" [
        { name = "oid"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "typname"; data_type = DataType.String (Some 64); is_nullable = false; default_value = None };
        { name = "typnamespace"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "typlen"; data_type = DataType.Int; is_nullable = false; default_value = None };
        { name = "typtype"; data_type = DataType.String (Some 1); is_nullable = false; default_value = None };
      ] in

      ()
    in

    create_system_catalogs ();
    Printf.printf "Catalog initialized with default schema '%s' (abbreviation: '%s')\n" 
      default_schema_name default_schema_abbreviation

  let get_schema_oid name =
    match Schema.find_schema_by_name name with
    | Some schema -> schema.oid
    | None -> raise (Not_found)

  let get_default_schema_oid () =
    get_schema_oid default_schema_name

  let create_table schema_name table_name columns primary_key =
    let schema_oid = get_schema_oid schema_name in
    let table = Table.create_table schema_oid table_name columns primary_key in
    
    (* Add entry to bf_class *)
    let bf_class_oid = get_schema_oid default_schema_name in
    let bf_class = Table.find_table_by_name bf_class_oid "bf_class" in
    match bf_class with
    | Some t ->
        let new_row = [
          ("oid", string_of_int (Int64.to_int table.oid));
          ("relname", table_name);
          ("relnamespace", string_of_int (Int64.to_int schema_oid));
          ("reltype", "0"); 
          ("relowner", "0"); 
          ("relkind", "r"); (* 'r' for regular table *)
          ("reltuples", "0.0");
        ] in
        (* You'd need to implement an insert_row function for Table *)
        (* Table.insert_row t new_row; *)
        table
    | None -> failwith "System catalog bf_class not found"

  let create_index table_name index_name columns index_type is_unique =
    let schema_oid = get_default_schema_oid () in
    match Table.find_table_by_name schema_oid table_name with
    | Some table ->
        let index = Index.create_index table.oid index_name columns index_type is_unique in
        
        (* Add entry to bf_class *)
        let bf_class_oid = get_schema_oid default_schema_name in
        let bf_class = Table.find_table_by_name bf_class_oid "bf_class" in
        (match bf_class with
        | Some t ->
            let new_row = [
              ("oid", string_of_int (Int64.to_int index.oid));
              ("relname", index_name);
              ("relnamespace", string_of_int (Int64.to_int schema_oid));
              ("reltype", "0");
              ("relowner", "0");
              ("relkind", "i"); (* 'i' for index *)
              ("reltuples", "0.0");
            ] in
            (* Table.insert_row t new_row; *)
            ()
        | None -> failwith "System catalog bf_class not found");
        index
    | None -> failwith ("Table " ^ table_name ^ " not found")

  let drop_table schema_name table_name =
    let schema_oid = get_schema_oid schema_name in
    match Table.find_table_by_name schema_oid table_name with
    | Some table ->
        (* Remove from bf_class *)
        let bf_class_oid = get_schema_oid default_schema_name in
        let bf_class = Table.find_table_by_name bf_class_oid "bf_class" in
        (match bf_class with
        | Some t ->
            (* Table.delete_row t [("oid", string_of_int (Int64.to_int table.oid))]; *)
            ()
        | None -> failwith "System catalog bf_class not found");
        
        (* Remove indexes *)
        List.iter (fun index_oid ->
          ignore (Index.delete_index index_oid)
        ) table.indexes;
        
        (* Remove constraints *)
        List.iter (fun constraint_oid ->
          ignore (Constraint.delete_constraint constraint_oid)
        ) table.constraints;
        
        (* Remove table *)
        Hashtbl.remove Table.tables table.oid;
        Schema.remove_table_from_schema schema_oid table.oid;
        Catalog.delete_entry table.oid;
        true
    | None -> false

  let list_tables schema_name =
    let schema_oid = get_schema_oid schema_name in
    Table.list_tables ()
    |> List.filter (fun table -> table.schema_oid = schema_oid)

  let get_table_info schema_name table_name =
    let schema_oid = get_schema_oid schema_name in
    match Table.find_table_by_name schema_oid table_name with
    | Some table ->
        let indexes = List.map (fun index_oid ->
          match Index.find_index index_oid with
          | Some index -> index
          | None -> failwith ("Index " ^ string_of_int (Int64.to_int index_oid) ^ " not found")
        ) table.indexes in
        let constraints = List.map (fun constraint_oid ->
          match Constraint.find_constraint constraint_oid with
          | Some c -> c
          | None -> failwith ("Constraint " ^ string_of_int (Int64.to_int constraint_oid) ^ " not found")
        ) table.constraints in
        (table, indexes, constraints)
    | None -> failwith ("Table " ^ table_name ^ " not found in schema " ^ schema_name)
end