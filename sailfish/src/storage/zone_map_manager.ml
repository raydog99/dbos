open Core
open Data_table
open Tile_group
open Tile_group_header
open Transaction_context

module type ValueType = sig
  type t
  val compare_greater_than : t -> t -> CmpBool.t
  val compare_less_than : t -> t -> CmpBool.t
  val to_string : t -> string
  val get_type_id : t -> TypeId.t
end

module ZoneMapManager(DataTable : DataTableType)
                     (TileGroup : TileGroupType)
                     (TileGroupHeader : TileGroupHeaderType)
                     (TransactionContext : TransactionContextType)
                     (Value : ValueType) = struct
  type column_statistics = {
    min_value: Value.t;
    max_value: Value.t;
  }

  type t = {
    mutable zone_map_table_exists: bool;
    pool: EphemeralPool.t;
  }

  let instance = 
    { zone_map_table_exists = false;
      pool = EphemeralPool.create () }

  let get_instance () = instance

  let create_zone_map_table_in_catalog t =
    Log.debug (fun m -> m "Create the Zone Map table");
    let txn_manager = TransactionManagerFactory.get_instance () in
    let txn = TransactionManager.begin_transaction txn_manager in
    ZoneMapCatalog.get_instance txn;
    TransactionManager.commit_transaction txn_manager txn;
    t.zone_map_table_exists <- true

  let create_zone_maps_for_table t table txn =
    assert (table <> None);
    let num_tile_groups = DataTable.get_tile_group_count table in
    for i = 0 to num_tile_groups - 1 do
      let tile_group = DataTable.get_tile_group table i in
      let tile_group_header = TileGroup.get_header tile_group in
      let immutable = TileGroupHeader.get_immutability tile_group_header in
      if immutable then
        create_or_update_zone_map_for_tile_group t table i txn
    done

  let create_or_update_zone_map_for_tile_group t table tile_group_idx txn =
    Log.debug (fun m -> m "Creating Zone Maps for TileGroupId : %d" tile_group_idx);
    let database_id = DataTable.get_database_oid table in
    let table_id = DataTable.get_oid table in
    let schema = DataTable.get_schema table in
    let num_columns = Schema.get_column_count schema in
    let tile_group = DataTable.get_tile_group table tile_group_idx in

    for col_itr = 0 to num_columns - 1 do
      let min = ref (TileGroup.get_value tile_group 0 col_itr) in
      let max = ref (TileGroup.get_value tile_group 0 col_itr) in
      let num_tuple_slots = TileGroup.get_allocated_tuple_count tile_group in
      for tuple_itr = 0 to num_tuple_slots - 1 do
        let current_val = TileGroup.get_value tile_group tuple_itr col_itr in
        if Value.compare_greater_than current_val !max = CmpBool.True then
          max := current_val;
        if Value.compare_less_than current_val !min = CmpBool.True then
          min := current_val
      done;
      let val_type = Value.get_type_id !min in
      let converted_min = Value.to_string !min in
      let converted_max = Value.to_string !max in
      let converted_type = TypeId.to_string val_type in

      create_or_update_zone_map_in_catalog t database_id table_id tile_group_idx
        col_itr converted_min converted_max converted_type txn
    done

  let create_or_update_zone_map_in_catalog t database_id table_id tile_group_idx column_id
      min max type_ txn =
    let stats_catalog = ZoneMapCatalog.get_instance None in
    let txn_manager = TransactionManagerFactory.get_instance () in
    let single_statement_txn = txn = None in
    let txn = if single_statement_txn then TransactionManager.begin_transaction txn_manager else txn in
    ZoneMapCatalog.delete_column_statistics stats_catalog txn database_id table_id tile_group_idx column_id;
    ZoneMapCatalog.insert_column_statistics stats_catalog txn database_id table_id tile_group_idx
      column_id min max type_ t.pool;
    if single_statement_txn then
      TransactionManager.commit_transaction txn_manager txn

  let get_zone_map_from_catalog t database_id table_id tile_group_idx column_id =
    let stats_catalog = ZoneMapCatalog.get_instance None in
    let txn_manager = TransactionManagerFactory.get_instance () in
    let txn = TransactionManager.begin_transaction txn_manager in
    let result_vector = ZoneMapCatalog.get_column_statistics stats_catalog txn
      database_id table_id tile_group_idx column_id in
    TransactionManager.commit_transaction txn_manager txn;
    match result_vector with
    | None -> None
    | Some vector -> get_result_vector_as_zone_map t vector

  let get_result_vector_as_zone_map t result_vector =
    let min_varchar = List.nth_exn result_vector (ZoneMapCatalog.ZoneMapOffset.minimum_off |> Int.of_int) in
    let max_varchar = List.nth_exn result_vector (ZoneMapCatalog.ZoneMapOffset.maximum_off |> Int.of_int) in
    let type_varchar = List.nth_exn result_vector (ZoneMapCatalog.ZoneMapOffset.type_off |> Int.of_int) in
    Some { min_value = get_value_as_original min_varchar type_varchar;
           max_value = get_value_as_original max_varchar type_varchar }

end