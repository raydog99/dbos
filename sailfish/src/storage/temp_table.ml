open Core
open Schema
open Tuple
open Tile_group
open Tile_group_header

module Make (Schema : SchemaType) (Tuple : TupleType) (TileGroup : TileGroupType) (TileGroupHeader : TileGroupHeaderType) = struct
  type t = {
    mutable table_oid: int;
    schema: Schema.t;
    own_schema: bool;
    layout_type: LayoutType.t;
    mutable tile_groups: TileGroup.t list;
    mutable tuple_count: int;
  }

  let invalid_oid = -1 
  let initial_txn_id = 0 
  let temptable_tilegroup_id = 100000 
  let temptable_default_size = 100

  let create table_oid schema own_schema layout_type =
    let t = {
      table_oid;
      schema;
      own_schema;
      layout_type;
      tile_groups = [];
      tuple_count = 0;
    } in
    ignore (t.add_default_tile_group ());
    t

  let insert_tuple t tuple =
    let rec find_slot tile_groups =
      match tile_groups with
      | [] -> None
      | tile_group :: rest ->
          match TileGroup.insert_tuple tile_group tuple with
          | Some tuple_slot ->
              let tile_group_id = TileGroup.get_tile_group_id tile_group in
              Some (tile_group_id, tuple_slot)
          | None -> find_slot rest
    in
    match find_slot t.tile_groups with
    | Some (tile_group_id, tuple_slot) ->
        let location = (tile_group_id, tuple_slot) in
        t.tuple_count <- t.tuple_count + 1;
        let tile_group = List.hd_exn t.tile_groups in
        let tile_group_header = TileGroup.get_header tile_group in
        TileGroupHeader.set_transaction_id tile_group_header tuple_slot initial_txn_id;
        if tuple_slot = TileGroup.get_allocated_tuple_count tile_group - 1 then
          ignore (t.add_default_tile_group ());
        Some location
    | None ->
        Log.warn (fun m -> m "Failed to get tuple slot.");
        None

  let get_tile_group t tile_group_offset =
    List.nth t.tile_groups tile_group_offset

  let get_tile_group_by_id t tile_group_id =
    List.find t.tile_groups ~f:(fun tg -> TileGroup.get_tile_group_id tg = tile_group_id)

  let add_default_tile_group t =
    let tile_group_id = temptable_tilegroup_id + List.length t.tile_groups in
    let tile_group = TileGroup.create invalid_oid tile_group_id t.layout_type temptable_default_size in
    t.tile_groups <- t.tile_groups @ [tile_group];
    Log.trace (fun m -> m "Created TileGroup for %s\n%s\n" (t.get_name ()) (TileGroup.get_info tile_group));
    tile_group_id

  let get_name t =
    sprintf "TEMP_TABLE[%d]" t.table_oid

  let get_tile_group_count t =
    List.length t.tile_groups

  let get_tuple_count t =
    t.tuple_count

  let get_schema t =
    t.schema
end