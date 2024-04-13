module type LogicalTileType = sig
  type column_info = {
    position_list_idx : int;
    base_tile : int;
    origin_column_id : int;
  }

  val schema_preallocation_size : int
  val schema : column_info list
  val position_list : int list
  val position_lists : int list
  val visible_rows : bool list
  val total_tuples : int
  val visible_tuples : int
end

module type L : sig
  val add_column : t -> Tile.t -> int -> int -> unit
  val add_columns : t -> TileGroup.t -> oid_t list -> unit
  val project_columns : t -> oid_t list -> oid_t list -> unit
  val add_position_list : t -> oid_t list -> int
  val remove_visibility : t -> oid_t -> unit
  val get_base_tile : t -> oid_t -> storage::Tile.t
  val get_value : t -> oid_t -> oid_t -> type::Value.t
  val set_value : t -> type::Value.t -> oid_t -> oid_t -> unit
  val get_tuple_count : t -> int
  val get_column_count : t -> int
  val get_schema : t -> column_info list
  val get_column_info : t -> oid_t -> column_info
  val get_physical_schema : t -> catalog::Schema.t
  val set_schema : t -> column_info list -> unit
  val get_position_lists : t -> position_lists
  val get_position_list : t -> oid_t -> position_list
  val set_position_lists : t -> position_lists -> unit
  val set_position_lists_and_visibility : t -> position_lists -> unit
  val get_all_values_as_strings : t -> int list -> bool -> string list list
  val get_info : t -> string
end

module Make(LogicalTile: LogicalTileType) : L = struct
  let schema_preallocation_size = LogicalTile.schema_preallocation_size
  let schema = LogicalTile.schema
  let position_list = LogicalTile.position_list
  let position_lists = LogicalTile.position_lists
  let visible_rows = LogicalTile.visible_rows
  let total_tuples = LogicalTile.total_tuples
  let visible_tuples = LogicalTile.visible_tuples

  let get_schema logical_tile = logical_tile.schema

  let get_column_info logical_tile column_id =
    List.nth logical_tile.schema column_id

  let get_physical_schema logical_tile =
    let physical_columns =
      List.map
        (fun column ->
          let schema = Tile.get_schema column.base_tile in
          Schema.get_column schema column.origin_column_id)
        logical_tile.schema
    in
    Schema.create physical_columns

  let get_position_lists logical_tile = logical_tile.position_lists

  let get_position_list logical_tile column_id =
    List.nth logical_tile.position_lists column_id

  let set_position_lists logical_tile position_lists =
    logical_tile.position_lists <- position_lists

  let set_position_lists_and_visibility logical_tile position_lists =
    logical_tile.position_lists <- position_lists;
    if List.length position_lists > 0 then (
      logical_tile.total_tuples <- List.length (List.hd position_lists);
      logical_tile.visible_rows <-
        Array.make (List.length (List.hd position_lists)) true;
      logical_tile.visible_tuples <- List.length (List.hd position_lists)
    )

  let add_position_list logical_tile position_list =
    match logical_tile.position_lists with
    | [] ->
        logical_tile.visible_tuples <- List.length position_list;
        logical_tile.visible_rows <-
          Array.make (List.length position_list) true;
        logical_tile.total_tuples <- logical_tile.visible_tuples
    | hd :: _ -> assert (List.length hd = List.length position_list);
    logical_tile.position_lists <- position_list :: logical_tile.position_lists;
    List.length logical_tile.position_lists - 1

  let remove_visibility logical_tile tuple_id =
    assert (tuple_id < logical_tile.total_tuples);
    assert (logical_tile.visible_rows.(tuple_id));
    logical_tile.visible_rows.(tuple_id) <- false;
    logical_tile.visible_tuples <- logical_tile.visible_tuples - 1

  let get_base_tile logical_tile column_id =
    (List.nth logical_tile.schema column_id).base_tile

  let get_value logical_tile tuple_id column_id =
    assert (column_id < List.length logical_tile.schema);
    assert (tuple_id < logical_tile.total_tuples);
    let cp = List.nth logical_tile.schema column_id in
    let base_tuple_id =
      List.nth logical_tile.position_lists cp.position_list_idx tuple_id
    in
    let base_tile = cp.base_tile in
    if base_tuple_id = Tile.NULL_OID then
      ValueFactory.get_null_value_by_type
        (Schema.get_type (Tile.get_schema base_tile) cp.origin_column_id)
    else Tile.get_value base_tile base_tuple_id cp.origin_column_id

  let get_tuple_count logical_tile = logical_tile.visible_tuples

  let get_column_count logical_tile = List.length logical_tile.schema

  let add_column logical_tile base_tile origin_column_id position_list_idx =
    let cp =
      {
        base_tile;
        origin_column_id;
        position_list_idx;
      }
    in
    logical_tile.schema <- cp :: logical_tile.schema

  let add_columns logical_tile tile_group column_ids =
    let position_list_idx = 0 in
    let layout = TileGroup.get_layout tile_group in
    List.iter
      (fun origin_column_id ->
        let base_tile_offset, tile_column_id =
          Layout.locate_tile_and_column layout origin_column_id
        in
        let base_tile = TileGroup.get_tile_reference tile_group base_tile_offset in
        add_column logical_tile base_tile tile_column_id position_list_idx)
      column_ids

  let project_columns logical_tile original_column_ids column_ids =
    let new_schema =
      List.map
        (fun id ->
          let ret =
            List.findi
              (fun _ orig_id -> orig_id = id)
              original_column_ids
          in
          let idx, _ = ret in
          assert (idx <> -1);
          List.nth logical_tile.schema idx)
        column_ids
    in
    logical_tile.schema <- new_schema

  let generate_tile_to_col_map logical_tile old_to_new_cols =
    let map = Hashtbl.create 10 in
    List.iter
      (fun (old_col_id, new_col_id) ->
        let column_info = List.nth logical_tile.schema old_col_id in
        let base_tile = column_info.base_tile in
        let cols =
          try Hashtbl.find map base_tile with Not_found -> []
        in
        Hashtbl.replace map base_tile (new_col_id :: cols))
      old_to_new_cols;
    map *)
end