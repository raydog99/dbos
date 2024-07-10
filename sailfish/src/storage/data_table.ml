open Core
open Layout

module TriggerList = struct
  type t = Trigger.t list
  let create () = []
  let add_trigger t trigger = trigger :: t
  let get_trigger_list_size = List.length
  let get t n = List.nth_opt t n
end

type item_pointer = {
  block : int;
  offset : int;
}

type t = {
  mutable table_oid: int;
  mutable schema: Schema.t;
  mutable own_schema: bool;
  mutable table_name: string;
  mutable database_oid: int;
  mutable tuples_per_tilegroup: int;
  mutable current_layout_oid: int Atomic.t;
  mutable adapt_table: bool;
  mutable trigger_list: TriggerList.t;
  mutable active_tilegroup_count: int;
  mutable active_indirection_array_count: int;
  mutable active_tile_groups: TileGroup.t option array;
  mutable active_indirection_arrays: IndirectionArray.t option array;
  mutable tile_groups: TileGroup.t array;
  mutable number_of_tuples: int;
  mutable indexes: Index.t option array;
  mutable indexes_columns: int Set.Poly.t list;
  mutable default_layout: Layout.t;
  mutable dirty: bool;
  mutable layout_samples: Sample.t list;
  mutable index_samples: Sample.t list;
  layout_samples_mutex: Mutex.t;
  index_samples_mutex: Mutex.t;
}

let invalid_tile_group_id = -1

let default_active_tilegroup_count = 1
let default_active_indirection_array_count = 1

let create schema table_name database_oid table_oid tuples_per_tilegroup
    own_schema adapt_table is_catalog layout_type =
  let current_layout_oid = Atomic.make Column_store_layout_oid in
  let active_tilegroup_count, active_indirection_array_count =
    if is_catalog then (1, 1)
    else (default_active_tilegroup_count, default_active_indirection_array_count)
  in
  let t = {
    table_oid;
    schema;
    own_schema;
    table_name;
    database_oid;
    tuples_per_tilegroup;
    current_layout_oid;
    adapt_table;
    trigger_list = TriggerList.create ();
    active_tilegroup_count;
    active_indirection_array_count;
    active_tile_groups = Array.create ~len:active_tilegroup_count None;
    active_indirection_arrays = Array.create ~len:active_indirection_array_count None;
    tile_groups = [||];
    number_of_tuples = 0;
    indexes = [||];
    indexes_columns = [];
    default_layout = Layout.create (Schema.get_column_count schema) layout_type;
    dirty = false;
    layout_samples = [];
    index_samples = [];
    layout_samples_mutex = Mutex.create ();
    index_samples_mutex = Mutex.create ();
  } in
  
  (* Create tile groups *)
  for i = 0 to active_tilegroup_count - 1 do
    ignore (add_default_tile_group t i)
  done;

  (* Create indirection layers *)
  for i = 0 to active_indirection_array_count - 1 do
    ignore (add_default_indirection_array t i)
  done;
  
  t

let finalize t =
  if t.own_schema then
    Schema.delete t.schema

let check_not_nulls t tuple column_idx =
  not (Tuple.get_value tuple column_idx |> Value.is_null)

let check_constraints t tuple =
  (* Check NOT NULL constraints *)
  Schema.get_not_null_columns t.schema
  |> List.for_all ~f:(fun column_id ->
      if not (Schema.allow_null t.schema column_id) then
        check_not_nulls t tuple column_id
      else
        true)
  &&
  (* Check multi-column constraints *)
  Schema.get_constraints t.schema
  |> List.for_all ~f:(fun (_, cons) ->
      match Constraint.get_type cons with
      | Check -> true  
      | Unique -> true  
      | Primary -> true  
      | Foreign -> true  
      | Exclusion -> true  
      | _ -> 
          let error = Printf.sprintf "ConstraintType '%s' is not supported"
            (Constraint.type_to_string (Constraint.get_type cons))
          in
          Log.debug (fun m -> m "%s" error);
          false)

let get_empty_tuple_slot t tuple =
  
  let active_tile_group_id = t.number_of_tuples % t.active_tilegroup_count in
  let rec find_slot tile_group =
    match TileGroup.insert_tuple tile_group tuple with
    | Some tuple_slot -> 
        let tile_group_id = TileGroup.get_tile_group_id tile_group in
        (tile_group_id, tuple_slot)
    | None ->
        let next_tile_group = 
          match Array.get t.active_tile_groups ((active_tile_group_id + 1) % t.active_tilegroup_count) with
          | Some tg -> tg
          | None -> 
              let new_tile_group = add_default_tile_group t active_tile_group_id in
              Array.set t.active_tile_groups active_tile_group_id (Some new_tile_group);
              new_tile_group
        in
        find_slot next_tile_group
  in
  let tile_group = Option.value_exn (Array.get t.active_tile_groups active_tile_group_id) in
  let (tile_group_id, tuple_slot) = find_slot tile_group in
  {block = tile_group_id; offset = tuple_slot}

let insert_empty_version t =
  let location = get_empty_tuple_slot t None in
  if location.block = invalid_tile_group_id then begin
    Log.trace (fun m -> m "Failed to get tuple slot.");
    None
  end else begin
    Log.trace (fun m -> m "Location: %d, %d" location.block location.offset);
    t.number_of_tuples <- t.number_of_tuples + 1;
    Some location
  end

let acquire_version t =
  let location = get_empty_tuple_slot t None in
  if location.block = invalid_tile_group_id then begin
    Log.trace (fun m -> m "Failed to get tuple slot.");
    None
  end else begin
    Log.trace (fun m -> m "Location: %d, %d" location.block location.offset);
    t.number_of_tuples <- t.number_of_tuples + 1;
    Some location
  end

let install_version t tuple targets_ptr transaction index_entry_ptr =
  if not (check_constraints t tuple) then begin
    Log.trace (fun m -> m "InstallVersion(): Constraint violated");
    false
  end else
    insert_in_secondary_indexes t tuple targets_ptr transaction index_entry_ptr

let insert_tuple t tuple transaction index_entry_ptr check_fk =
  let location = get_empty_tuple_slot t (Some tuple) in
  if location.block = invalid_tile_group_id then begin
    Log.trace (fun m -> m "Failed to get tuple slot.");
    None
  end else
    match insert_tuple_at_location t tuple location transaction index_entry_ptr check_fk with
    | true -> Some location
    | false -> None

let insert_tuple_at_location t tuple location transaction index_entry_ptr check_fk =
  if not (check_constraints t tuple) then begin
    Log.trace (fun m -> m "InsertTuple(): Constraint violated");
    false
  end else begin
    let temp_ptr = ref None in
    let index_entry_ptr = Option.value index_entry_ptr ~default:temp_ptr in
    
    Log.trace (fun m -> m "Location: %d, %d" location.block location.offset);

    let index_count = get_index_count t in
    if index_count = 0 then begin
      if check_fk && not (check_foreign_key_constraints t tuple transaction) then begin
        Log.trace (fun m -> m "ForeignKey constraint violated");
        false
      end else begin
        t.number_of_tuples <- t.number_of_tuples + 1;
        true
      end
    end else
      match insert_in_indexes t tuple location transaction index_entry_ptr with
      | false ->
          Log.trace (fun m -> m "Index constraint violated");
          false
      | true ->
          if check_fk && not (check_foreign_key_constraints t tuple transaction) then begin
            Log.trace (fun m -> m "ForeignKey constraint violated");
            false
          end else begin
            assert (!index_entry_ptr |> Option.map ~f:(fun ptr -> ptr.block = location.block && ptr.offset = location.offset) |> Option.value ~default:true);
            t.number_of_tuples <- t.number_of_tuples + 1;
            true
          end
  end

let insert_tuple_without_index t tuple =
  let location = get_empty_tuple_slot t (Some tuple) in
  if location.block = invalid_tile_group_id then begin
    Log.trace (fun m -> m "Failed to get tuple slot.");
    None
  end else begin
    Log.trace (fun m -> m "Location: %d, %d" location.block location.offset);
    assert (get_index_count t = 0);
    t.number_of_tuples <- t.number_of_tuples + 1;
    Some location
  end

let insert_in_indexes t tuple location transaction index_entry_ptr =
  let index_count = get_index_count t in
  
  let active_indirection_array_id = t.number_of_tuples % t.active_indirection_array_count in
  
  let rec allocate_indirection () =
    match Array.get t.active_indirection_arrays active_indirection_array_id with
    | Some array ->
        begin match IndirectionArray.allocate_indirection array with
        | Some offset -> offset
        | None -> allocate_indirection ()
        end
    | None -> allocate_indirection ()
  in
  
  let indirection_offset = allocate_indirection () in
  
  let index_entry = IndirectionArray.get_indirection_by_offset 
    (Array.get t.active_indirection_arrays active_indirection_array_id |> Option.value_exn)
    indirection_offset
  in
  
  index_entry_ptr := Some index_entry;
  index_entry.block <- location.block;
  index_entry.offset <- location.offset;
  
  if indirection_offset = IndirectionArray.max_size - 1 then
    add_default_indirection_array t active_indirection_array_id;
  
  let transaction_manager = TransactionManager.get_instance () in
  
  let is_occupied ptr = 
    TransactionManager.is_occupied transaction_manager transaction ptr
  in
  
  let rec insert_in_indexes' index_itr success_count =
    if index_itr < 0 then
      true
    else
      match get_index t index_itr with
      | None -> insert_in_indexes' (index_itr - 1) success_count
      | Some index ->
          let index_schema = Index.get_key_schema index in
          let indexed_columns = Schema.get_indexed_columns index_schema in
          let key = Tuple.create index_schema true in
          Tuple.set_from_tuple key tuple indexed_columns (Index.get_pool index);
          
          let res = match Index.get_index_type index with
            | Primary_key | Unique ->
                Index.cond_insert_entry index key !index_entry_ptr is_occupied
            | _ ->
                Index.insert_entry index key !index_entry_ptr;
                true
          in
          
          if not res then begin
            index_entry_ptr := None;
            false
          end else begin
            Log.trace (fun m -> m "Index constraint check on %s passed." (Index.get_name index));
            insert_in_indexes' (index_itr - 1) (success_count + 1)
          end
  in
  
  insert_in_indexes' (index_count - 1) 0

let insert_in_secondary_indexes t tuple targets_ptr transaction index_entry_ptr =
  let index_count = get_index_count t in
  
  let targets_set = 
    targets_ptr 
    |> Option.value ~default:[] 
    |> List.fold_left (fun set (col, _) -> Int.Set.add col set) Int.Set.empty
  in
  
  let transaction_manager = TransactionManager.get_instance () in
  
  let is_occupied ptr = 
    TransactionManager.is_occupied transaction_manager transaction ptr
  in
  
  let rec insert_in_indexes index_itr =
    if index_itr < 0 then true
    else
      match get_index t index_itr with
      | None -> insert_in_indexes (index_itr - 1)
      | Some index ->
          if Index.get_index_type index = Primary_key then
            insert_in_indexes (index_itr - 1)
          else
            let index_schema = Index.get_key_schema index in
            let indexed_columns = Schema.get_indexed_columns index_schema in
            
            let updated = 
              List.exists (fun col -> Int.Set.mem col targets_set) indexed_columns
            in
            
            if not updated then
              insert_in_indexes (index_itr - 1)
            else
              let key = Tuple.create index_schema true in
              Tuple.set_from_tuple key tuple indexed_columns (Index.get_pool index);
              
              let res = match Index.get_index_type index with
                | Primary_key | Unique ->
                    Index.cond_insert_entry index key index_entry_ptr is_occupied
                | _ ->
                    Index.insert_entry index key index_entry_ptr;
                    true
              in
              
              if res then begin
                Log.trace (fun m -> m "Index constraint check on %s passed." (Index.get_name index));
                insert_in_indexes (index_itr - 1)
              end else
                false
  in
  
  insert_in_indexes (index_count - 1)

let check_foreign_key_src_and_cascade t prev_tuple new_tuple transaction context is_update =
  if not (Schema.has_foreign_key_sources t.schema) then
    true
  else
    let transaction_manager = TransactionManager.get_instance () in
    
    let check_constraint cons =
      let source_table_id = Constraint.get_table_oid cons in
      match StorageManager.get_table_with_oid t.database_oid source_table_id with
      | None -> 
          Log.trace (fun m -> m "Can't find table %d! Return false" source_table_id);
          false
      | Some src_table ->
          let src_table_index_count = get_index_count src_table in
          
          let rec check_indexes index_itr =
            if index_itr < 0 then true
            else
              match get_index src_table index_itr with
              | None -> check_indexes (index_itr - 1)
              | Some index ->
                  if Index.get_oid index = Constraint.get_index_oid cons &&
                     Index.get_key_attrs index = Constraint.get_column_ids cons then
                    begin
                      Log.debug (fun m -> m "Searching in source tables's fk index...");
                      
                      let key_attrs = Constraint.get_column_ids cons in
                      let fk_schema = Schema.copy_schema (get_schema src_table) key_attrs in
                      let key = Tuple.create fk_schema true in
                      Tuple.set_from_tuple key prev_tuple (Constraint.get_fk_sink_column_ids cons) (Index.get_pool index);
                      
                      let location_ptrs = Index.scan_key index key in
                      
                      if List.length location_ptrs > 0 then
                        begin
                          Log.debug (fun m -> m "Something found in the source table!");
                          
                          let check_location ptr =
                            let src_tile_group = get_tile_group_by_id src_table ptr.block in
                            let src_tile_group_header = TileGroup.get_header src_tile_group in
                            
                            let visibility = TransactionManager.is_visible 
                                transaction_manager transaction src_tile_group_header 
                                ptr.offset Visibility_id_type.Commit_id
                            in
                            
                            if visibility <> Visibility_type.Ok then
                              true
                            else
                              match Constraint.get_fk_update_action cons with
                              | No_action | Restrict -> false
                              | Cascade ->
                                  let src_is_owner = TransactionManager.is_owner
                                      transaction_manager transaction src_tile_group_header
                                      ptr.offset
                                  in
                                  
                                  let ret = TransactionManager.perform_read
                                      transaction_manager transaction ptr
                                      src_tile_group_header true
                                  in
                                  
                                  if not ret then
                                    begin
                                      if src_is_owner then
                                        TransactionManager.yield_ownership
                                          transaction_manager transaction
                                          src_tile_group_header ptr.offset;
                                      false
                                    end
                                  else
                                    true
                          in
                          
                          List.for_all check_location location_ptrs
                        end
                      else
                        true
                    end
                  else
                    check_indexes (index_itr - 1)
          in
          
          check_indexes (src_table_index_count - 1)
    in
    
    List.for_all check_constraint (Schema.get_foreign_key_sources t.schema)

let check_foreign_key_constraints t tuple transaction =
  List.for_all 
    (fun foreign_key ->
       let sink_table_id = Constraint.get_fk_sink_table_oid foreign_key in
       match StorageManager.get_table_with_oid t.database_oid sink_table_id with
       | None -> 
           Log.error (fun m -> m "Can't find table %d! Return false" sink_table_id);
           false
       | Some ref_table ->
           let ref_table_index_count = get_index_count ref_table in
           
           let rec check_indexes index_itr =
             if index_itr < 0 then false
             else
               match get_index ref_table index_itr with
               | None -> check_indexes (index_itr - 1)
               | Some index ->
                   if Index.get_index_type index = Primary_key then
                     let key_attrs = Constraint.get_fk_sink_column_ids foreign_key in
                     let foreign_key_schema = Schema.copy_schema (get_schema ref_table) key_attrs in
                     let key = Tuple.create foreign_key_schema true in
                     Tuple.set_from_tuple key tuple (Constraint.get_column_ids foreign_key) (Index.get_pool index);
                     
                     Log.trace (fun m -> m "check key: %s" (Tuple.get_info key));
                     let location_ptrs = Index.scan_key index key in
                     
                     if List.length location_ptrs = 0 then
                       begin
                         Log.debug (fun m -> m "The key: %s does not exist in table %s"
                                      (Tuple.get_info key) (get_name ref_table));
                         false
                       end
                     else
                       let tile_group = get_tile_group_by_id ref_table (List.hd location_ptrs).block in
                       let tile_group_header = TileGroup.get_header tile_group in
                       
                       let transaction_manager = TransactionManager.get_instance () in
                       let visibility = TransactionManager.is_visible
                           transaction_manager transaction tile_group_header
                           (List.hd location_ptrs).offset
                           Visibility_id_type.Read_id
                       in
                       
                       if visibility <> Visibility_type.Ok then
                         begin
                           Log.debug (fun m -> m "The key: %s is not yet visible in table %s, visibility type: %s"
                                        (Tuple.get_info key) (get_name ref_table)
                                        (Visibility_type.to_string visibility));
                           false
                         end
                       else
                         true
                   else
                     check_indexes (index_itr - 1)
           in
           
           check_indexes (ref_table_index_count - 1))
    (Schema.get_foreign_key_constraints t.schema)

let increase_tuple_count t amount =
  t.number_of_tuples <- t.number_of_tuples + amount;
  t.dirty <- true

let decrease_tuple_count t amount =
  t.number_of_tuples <- t.number_of_tuples - amount;
  t.dirty <- true

let set_tuple_count t num_tuples =
  t.number_of_tuples <- num_tuples;
  t.dirty <- true

let get_tuple_count t =
  t.number_of_tuples

let is_dirty t =
  t.dirty

let reset_dirty t =
  t.dirty <- false

let get_tile_group_with_layout t layout =
  let tile_group_id = StorageManager.get_next_tile_group_id () in
  AbstractTable.get_tile_group_with_layout t.database_oid tile_group_id layout t.tuples_per_tilegroup

let add_default_indirection_array t active_indirection_array_id =
  let manager = Catalog.Manager.get_instance () in
  let indirection_array_id = Manager.get_next_indirection_array_id manager in
  
  let indirection_array = IndirectionArray.create indirection_array_id in
  Manager.add_indirection_array manager indirection_array_id indirection_array;
  
  t.active_indirection_arrays.(active_indirection_array_id) <- Some indirection_array;
  
  indirection_array_id

let add_default_tile_group t =
  let active_tile_group_id = t.number_of_tuples mod t.active_tilegroup_count in
  add_default_tile_group_at t active_tile_group_id

let add_default_tile_group_at t active_tile_group_id =
  let tile_group = get_tile_group_with_layout t t.default_layout in
  let tile_group_id = TileGroup.get_tile_group_id tile_group in
  
  Log.trace (fun m -> m "Added a tile group");
  t.tile_groups <- Array.append t.tile_groups [|tile_group|];
  
  (* add tile group metadata in locator *)
  StorageManager.add_tile_group tile_group_id tile_group;
  
  t.active_tile_groups.(active_tile_group_id) <- Some tile_group;
  
  t.tile_group_count <- t.tile_group_count + 1;
  
  Log.trace (fun m -> m "Recording tile group : %d" tile_group_id);
  
  tile_group_id

let add_tile_group_with_oid_for_recovery t tile_group_id =
  assert (tile_group_id <> 0);
  
  let schemas = [t.schema] in
  let layout =
    if Layout.is_row_store t.default_layout then
      t.default_layout
    else
      Layout.create (Schema.get_column_count t.schema)
  in
  
  let tile_group = TileGroup.create
      t.database_oid t.table_oid tile_group_id t schemas layout t.tuples_per_tilegroup
  in
  
  let tile_groups_exists = Array.exists (fun tg -> TileGroup.get_tile_group_id tg = tile_group_id) t.tile_groups in
  
  if not tile_groups_exists then begin
    t.tile_groups <- Array.append t.tile_groups [|tile_group|];
    
    Log.trace (fun m -> m "Added a tile group");
    
    (* add tile group metadata in locator *)
    StorageManager.add_tile_group tile_group_id tile_group;
    
    t.tile_group_count <- t.tile_group_count + 1;
    
    Log.trace (fun m -> m "Recording tile group : %d" tile_group_id);
  end

let add_tile_group t tile_group =
  let active_tile_group_id = t.number_of_tuples mod t.active_tilegroup_count in
  
  t.active_tile_groups.(active_tile_group_id) <- Some tile_group;
  
  let tile_group_id = TileGroup.get_tile_group_id tile_group in
  
  t.tile_groups <- Array.append t.tile_groups [|tile_group|];
  
  (* add tile group in catalog *)
  StorageManager.add_tile_group tile_group_id tile_group;
  
  t.tile_group_count <- t.tile_group_count + 1;
  
  Log.trace (fun m -> m "Recording tile group : %d" tile_group_id)

let get_tile_group_count t =
  t.tile_group_count

let get_tile_group t tile_group_offset =
  if tile_group_offset >= t.tile_group_count then
    None
  else
    Some t.tile_groups.(tile_group_offset)

let get_tile_group_by_id t tile_group_id =
  StorageManager.get_tile_group tile_group_id

let drop_tile_groups t =
  let storage_manager = StorageManager.get_instance () in
  
  Array.iter
    (fun tile_group ->
       let tile_group_id = TileGroup.get_tile_group_id tile_group in
       (* drop tile group in catalog *)
       StorageManager.drop_tile_group storage_manager tile_group_id)
    t.tile_groups;
  
  (* Clear array *)
  t.tile_groups <- [||];
  
  t.tile_group_count <- 0

let add_index t index =
  (* Add index *)
  t.indexes <- Array.append t.indexes [|Some index|];
  
  (* Add index column info *)
  let index_columns = Index.get_metadata index |> IndexMetadata.get_key_attrs in
  let index_columns_set = Int.Set.of_list index_columns in
  
  t.indexes_columns <- t.indexes_columns @ [index_columns_set]

let get_index_with_oid t index_oid =
  let rec find_index i =
    if i >= Array.length t.indexes then
      None
    else
      match t.indexes.(i) with
      | Some index when Index.get_oid index = index_oid -> Some index
      | _ -> find_index (i + 1)
  in
  match find_index 0 with
  | Some index -> index
  | None -> failwith ("No index with oid = " ^ string_of_int index_oid ^ " is found")

let drop_index_with_oid t index_oid =
  let rec find_and_drop i =
    if i >= Array.length t.indexes then
      ()
    else
      match t.indexes.(i) with
      | Some index when Index.get_oid index = index_oid ->
        (* Drop the index *)
        t.indexes.(i) <- None;
        (* Drop index column info *)
        t.indexes_columns <- List.filteri (fun j _ -> j <> i) t.indexes_columns
      | _ -> find_and_drop (i + 1)
  in
  find_and_drop 0

let drop_indexes t =
  t.indexes <- [||];
  t.indexes_columns <- []

let get_index t index_offset =
  if index_offset >= Array.length t.indexes then
    None
  else
    t.indexes.(index_offset)

let get_index_attrs t index_offset =
  if index_offset >= List.length t.indexes_columns then
    Int.Set.empty
  else
    List.nth t.indexes_columns index_offset

let get_index_count t =
  Array.length t.indexes

let get_valid_index_count t =
  Array.fold_left (fun count index ->
    if Option.is_some index then count + 1 else count
  ) 0 t.indexes

let transform_tile_group_schema tile_group layout =
  let tile_group_layout = TileGroup.get_layout tile_group in
  
  (* First, get info from the original tile group's schema *)
  let schemas = Hashtbl.create 10 in
  
  let column_count = Layout.get_column_count layout in
  for col_id = 0 to column_count - 1 do
    let orig_tile_offset, orig_tile_column_offset =
      Layout.locate_tile_and_column tile_group_layout col_id in
    let new_tile_offset, new_tile_column_offset =
      Layout.locate_tile_and_column layout col_id in
    
    (* Get the column info from original tile *)
    let tile = TileGroup.get_tile tile_group orig_tile_offset in
    let orig_schema = Tile.get_schema tile in
    let column_info = Schema.get_column orig_schema orig_tile_column_offset in
    
    let tile_columns =
      match Hashtbl.find_opt schemas new_tile_offset with
      | Some cols -> cols
      | None -> Hashtbl.create 10
    in
    Hashtbl.add tile_columns new_tile_column_offset column_info;
    Hashtbl.replace schemas new_tile_offset tile_columns
  done;
  
  (* Then, build the new schema *)
  Hashtbl.fold (fun _ tile_columns acc ->
    let columns =
      Hashtbl.fold (fun _ column acc ->
        column :: acc
      ) tile_columns []
    in
    Schema.create columns :: acc
  ) schemas []

let set_transformed_tile_group orig_tile_group new_tile_group =
  let new_layout = TileGroup.get_layout new_tile_group in
  let orig_layout = TileGroup.get_layout orig_tile_group in
  
  (* Check that both tile groups have the same schema *)
  let new_column_count = Layout.get_column_count new_layout in
  let orig_column_count = Layout.get_column_count orig_layout in
  assert (new_column_count = orig_column_count);
  
  let column_count = new_column_count in
  let tuple_count = TileGroup.get_allocated_tuple_count orig_tile_group in
  
  (* Go over each column copying onto the new tile group *)
  for column_itr = 0 to column_count - 1 do
    let orig_tile_offset, orig_tile_column_offset =
      Layout.locate_tile_and_column orig_layout column_itr in
    let new_tile_offset, new_tile_column_offset =
      Layout.locate_tile_and_column new_layout column_itr in
    
    let orig_tile = TileGroup.get_tile orig_tile_group orig_tile_offset in
    let new_tile = TileGroup.get_tile new_tile_group new_tile_offset in
    
    (* Copy the column over to the new tile group *)
    for tuple_itr = 0 to tuple_count - 1 do
      let val_ = Tile.get_value orig_tile tuple_itr orig_tile_column_offset in
      Tile.set_value new_tile val_ tuple_itr new_tile_column_offset
    done
  done;
  
  (* Finally, copy over the tile header *)
  let header = TileGroup.get_header orig_tile_group in
  let new_header = TileGroup.get_header new_tile_group in
  TileGroupHeader.copy_from new_header header

let transform_tile_group t tile_group_offset theta =
  (* First, check if the tile group is in this table *)
  if tile_group_offset >= Array.length t.tile_groups then begin
    Log.error (fun m -> m "Tile group offset not found in table : %d" tile_group_offset);
    None
  end else
    let tile_group_id = TileGroup.get_tile_group_id t.tile_groups.(tile_group_offset) in
    
    (* Get orig tile group from catalog *)
    let storage_manager = StorageManager.get_instance () in
    let tile_group = StorageManager.get_tile_group storage_manager tile_group_id in
    let diff = Layout.get_layout_difference (TileGroup.get_layout tile_group) t.default_layout in
    
    (* Check threshold for transformation *)
    if diff < theta then
      None
    else begin
      Log.trace (fun m -> m "Transforming tile group : %d" tile_group_offset);
      
      (* Get the schema for the new transformed tile group *)
      let new_schema = transform_tile_group_schema tile_group t.default_layout in
      
      (* Allocate space for the transformed tile group *)
      let new_tile_group = TileGroup.create
          (TileGroup.get_database_id tile_group)
          (TileGroup.get_table_id tile_group)
          (TileGroup.get_tile_group_id tile_group)
          (TileGroup.get_abstract_table tile_group)
          new_schema
          t.default_layout
          (TileGroup.get_allocated_tuple_count tile_group)
      in
      
      (* Set the transformed tile group column-at-a-time *)
      set_transformed_tile_group tile_group new_tile_group;
      
      (* Set the location of the new tile group
         and clean up the orig tile group *)
      StorageManager.add_tile_group storage_manager tile_group_id new_tile_group;
      
      Some new_tile_group
    end

let record_layout_sample t sample =
  Mutex.lock t.layout_samples_mutex;
  t.layout_samples <- sample :: t.layout_samples;
  Mutex.unlock t.layout_samples_mutex

let get_layout_samples t =
  Mutex.lock t.layout_samples_mutex;
  let samples = t.layout_samples in
  Mutex.unlock t.layout_samples_mutex;
  samples

let clear_layout_samples t =
  Mutex.lock t.layout_samples_mutex;
  t.layout_samples <- [];
  Mutex.unlock t.layout_samples_mutex

let record_index_sample t sample =
Mutex.lock t.index_samples_mutex;
t.index_samples <- sample :: t.index_samples;
Mutex.unlock t.index_samples_mutex

let get_index_samples t =
Mutex.lock t.index_samples_mutex;
let samples = t.index_samples in
Mutex.unlock t.index_samples_mutex;
samples

let clear_index_samples t =
Mutex.lock t.index_samples_mutex;
t.index_samples <- [];
Mutex.unlock t.index_samples_mutex

let add_trigger t new_trigger =
t.trigger_list <- TriggerList.add_trigger t.trigger_list new_trigger

let get_trigger_number t =
TriggerList.get_trigger_list_size t.trigger_list

let get_trigger_by_index t n =
TriggerList.get t.trigger_list n

let get_trigger_list t =
if TriggerList.get_trigger_list_size t.trigger_list > 0 then
  Some t.trigger_list
else
  None

let update_trigger_list_from_catalog t txn =
t.trigger_list <-
  Catalog.get_instance ()
  |> Catalog.get_system_catalogs t.database_oid
  |> SystemCatalogs.get_trigger_catalog
  |> TriggerCatalog.get_triggers txn t.table_oid

let hash t =
let oid = t.table_oid in
let hash = Hash.hash oid in
let hash = Hash.combine_hashes hash (Hash.hash_bytes (Bytes.of_string t.table_name)) in
let db_oid = t.database_oid in
Hash.combine_hashes hash (Hash.hash db_oid)

let equals t other =
t.table_oid = other.table_oid &&
t.database_oid = other.database_oid &&
t.table_name = other.table_name

let set_current_layout_oid t new_layout_oid =
let rec try_set () =
  let old_oid = Atomic.get t.current_layout_oid in
  if old_oid <= new_layout_oid then
    if Atomic.compare_and_set t.current_layout_oid old_oid new_layout_oid then
      true
    else
      try_set ()
  else
    false
in
try_set ()

let get_schema t = t.schema

let get_name t = t.table_name

let get_oid t = t.table_oid

let get_database_oid t = t.database_oid

let get_default_layout t = t.default_layout

let set_default_layout t layout = t.default_layout <- layout

let get_active_tile_group_count t = t.active_tilegroup_count

let set_active_tile_group_count t count = t.active_tilegroup_count <- count

let get_active_indirection_array_count t = t.active_indirection_array_count

let set_active_indirection_array_count t count = t.active_indirection_array_count <- count

let get_current_layout_oid t = Atomic.get t.current_layout_oid

let is_base_table t =
  true

let get_table_constraint_type t =
  ConstraintType.Default

let has_primary_key t =
  List.exists (fun (_, constraint_) -> 
    Constraint.get_type constraint_ = ConstraintType.Primary
  ) (Schema.get_constraints t.schema)

let has_foreign_key t =
  List.exists (fun (_, constraint_) -> 
    Constraint.get_type constraint_ = ConstraintType.Foreign
  ) (Schema.get_constraints t.schema)

let get_schema_columns t =
  Schema.get_columns t.schema

let get_column_count t =
  Schema.get_column_count t.schema

let get_column_name t column_id =
  Schema.get_column_name t.schema column_id

let get_column_names t =
  Schema.get_column_names t.schema

let get_column_types t =
  Schema.get_column_types t.schema

let get_column_lengths t =
  Schema.get_column_lengths t.schema

let validate_columns t column_ids =
  List.for_all (fun id -> id >= 0 && id < get_column_count t) column_ids