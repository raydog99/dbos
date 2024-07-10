open Core
open Schema
open Tile
open Tile_group_header
open Abstract_table
open Layout

module Make
    (Schema : SchemaType)
    (Tile : TileType)
    (TileGroupHeader : TileGroupHeaderType)
    (AbstractTable : AbstractTableType)
    (Layout : LayoutType) = struct

  type t = {
    mutable database_id: int;
    mutable table_id: int;
    mutable tile_group_id: int;
    backend_type: BackendType.t;
    tile_group_header: TileGroupHeader.t;
    table: AbstractTable.t;
    num_tuple_slots: int;
    tile_group_layout: Layout.t;
    mutable tile_count: int;
    mutable tiles: Tile.t list;
  }

  let invalid_oid = -1 
  let max_cid = Int.max_value 
  let initial_txn_id = 0 

  let create backend_type tile_group_header table schemas layout tuple_count =
    let t = {
      database_id = invalid_oid;
      table_id = invalid_oid;
      tile_group_id = invalid_oid;
      backend_type;
      tile_group_header;
      table;
      num_tuple_slots = tuple_count;
      tile_group_layout = layout;
      tile_count = List.length schemas;
      tiles = [];
    } in
    let storage_manager = StorageManager.get_instance () in
    t.tiles <- List.mapi schemas ~f:(fun tile_itr schema ->
      let tile_id = StorageManager.get_next_tile_id storage_manager in
      Tile.create backend_type t.database_id t.table_id t.tile_group_id tile_id
        tile_group_header schema t tuple_count
    );
    t

  let get_tile_id t tile_id =
    Tile.get_tile_id (List.nth_exn t.tiles tile_id)

  let get_tile_pool t tile_id =
    Tile.get_pool (List.nth_exn t.tiles tile_id)

  let get_tile_group_id t =
    t.tile_group_id

  let get_next_tuple_slot t =
    TileGroupHeader.get_current_next_tuple_slot t.tile_group_header

  let get_active_tuple_count t =
    TileGroupHeader.get_active_tuple_count t.tile_group_header

  let copy_tuple t tuple tuple_slot_id =
    let column_itr = ref 0 in
    List.iteri t.tiles ~f:(fun tile_itr tile ->
      let schema = Tile.get_schema tile in
      let tile_column_count = Schema.get_column_count schema in
      let tile_tuple_location = Tile.get_tuple_location tile tuple_slot_id in
      for tile_column_itr = 0 to tile_column_count - 1 do
        let val_ = Tuple.get_value tuple !column_itr in
        Tile.set_value tile val_ tuple_slot_id tile_column_itr;
        incr column_itr
      done
    )

  let insert_tuple t tuple =
    let tuple_slot_id = TileGroupHeader.get_next_empty_tuple_slot t.tile_group_header in
    if tuple_slot_id = invalid_oid then
      invalid_oid
    else
      match tuple with
      | None -> tuple_slot_id
      | Some tuple ->
          copy_tuple t tuple tuple_slot_id;
          assert (TileGroupHeader.get_transaction_id t.tile_group_header tuple_slot_id = invalid_oid);
          assert (TileGroupHeader.get_begin_commit_id t.tile_group_header tuple_slot_id = max_cid);
          assert (TileGroupHeader.get_end_commit_id t.tile_group_header tuple_slot_id = max_cid);
          tuple_slot_id

  let insert_tuple_from_recovery t commit_id tuple_slot_id tuple =
    let status = TileGroupHeader.get_empty_tuple_slot t.tile_group_header tuple_slot_id in
    if not status then
      invalid_oid
    else
      let current_begin_cid = TileGroupHeader.get_begin_commit_id t.tile_group_header tuple_slot_id in
      if current_begin_cid <> max_cid && current_begin_cid > commit_id then
        tuple_slot_id
      else begin
        copy_tuple t tuple tuple_slot_id;
        TileGroupHeader.set_transaction_id t.tile_group_header tuple_slot_id initial_txn_id;
        TileGroupHeader.set_begin_commit_id t.tile_group_header tuple_slot_id commit_id;
        TileGroupHeader.set_end_commit_id t.tile_group_header tuple_slot_id max_cid;
        TileGroupHeader.set_next_item_pointer t.tile_group_header tuple_slot_id ItemPointer.invalid;
        tuple_slot_id
      end

  let delete_tuple_from_recovery t commit_id tuple_slot_id =
    let status = TileGroupHeader.get_empty_tuple_slot t.tile_group_header tuple_slot_id in
    let current_begin_cid = TileGroupHeader.get_begin_commit_id t.tile_group_header tuple_slot_id in
    if current_begin_cid <> max_cid && current_begin_cid > commit_id then
      tuple_slot_id
    else if not status then
      invalid_oid
    else begin
      TileGroupHeader.set_transaction_id t.tile_group_header tuple_slot_id invalid_oid;
      TileGroupHeader.set_begin_commit_id t.tile_group_header tuple_slot_id commit_id;
      TileGroupHeader.set_end_commit_id t.tile_group_header tuple_slot_id commit_id;
      TileGroupHeader.set_next_item_pointer t.tile_group_header tuple_slot_id ItemPointer.invalid;
      tuple_slot_id
    end

  let update_tuple_from_recovery t commit_id tuple_slot_id new_location =
    let status = TileGroupHeader.get_empty_tuple_slot t.tile_group_header tuple_slot_id in
    let current_begin_cid = TileGroupHeader.get_begin_commit_id t.tile_group_header tuple_slot_id in
    if current_begin_cid <> max_cid && current_begin_cid > commit_id then
      tuple_slot_id
    else if not status then
      invalid_oid
    else begin
      TileGroupHeader.set_transaction_id t.tile_group_header tuple_slot_id invalid_oid;
      TileGroupHeader.set_begin_commit_id t.tile_group_header tuple_slot_id commit_id;
      TileGroupHeader.set_end_commit_id t.tile_group_header tuple_slot_id commit_id;
      TileGroupHeader.set_next_item_pointer t.tile_group_header tuple_slot_id new_location;
      tuple_slot_id
    end

  let insert_tuple_from_checkpoint t tuple_slot_id tuple commit_id =
    let status = TileGroupHeader.get_empty_tuple_slot t.tile_group_header tuple_slot_id in
    if not status then
      invalid_oid
    else begin
      copy_tuple t tuple tuple_slot_id;
      TileGroupHeader.set_transaction_id t.tile_group_header tuple_slot_id initial_txn_id;
      TileGroupHeader.set_begin_commit_id t.tile_group_header tuple_slot_id commit_id;
      TileGroupHeader.set_end_commit_id t.tile_group_header tuple_slot_id max_cid;
      TileGroupHeader.set_next_item_pointer t.tile_group_header tuple_slot_id ItemPointer.invalid;
      tuple_slot_id
    end

  let get_value t tuple_id column_id =
    assert (tuple_id < get_next_tuple_slot t);
    let tile_column_id, tile_offset = Layout.locate_tile_and_column t.tile_group_layout column_id in
    Tile.get_value (List.nth_exn t.tiles tile_offset) tuple_id tile_column_id

  let set_value t value tuple_id column_id =
    assert (tuple_id < get_next_tuple_slot t);
    let tile_column_id, tile_offset = Layout.locate_tile_and_column t.tile_group_layout column_id in
    Tile.set_value (List.nth_exn t.tiles tile_offset) value tuple_id tile_column_id

  let get_tile_reference t tile_offset =
    assert (tile_offset < t.tile_count);
    List.nth_exn t.tiles tile_offset

  let sync t =
    List.iter t.tiles ~f:Tile.sync

  let get_info t =
    let buf = Buffer.create 256 in
    Buffer.add_string buf (Printf.sprintf "** TILE GROUP[#%d] **\n" t.tile_group_id);
    Buffer.add_string buf (Printf.sprintf "Database[%d] // Table[%d]\n" t.database_id t.table_id);
    Buffer.add_string buf (Printf.sprintf "Layout[%s]\n" (Layout.to_string t.tile_group_layout));
    Buffer.add_string buf (TileGroupHeader.to_string t.tile_group_header);
    List.iteri t.tiles ~f:(fun tile_itr tile ->
      Buffer.add_string buf "\n";
      Buffer.add_string buf (Tile.to_string tile)
    );
    StringUtil.prefix (StringBoxUtil.box (Buffer.contents buf)) "  "

end

module Make
    (TileGroup : sig
       type t
       val create : BackendType.t -> TileGroupHeader.t -> AbstractTable.t ->
                    Schema.t list -> Layout.t -> int -> t
       val set_database_id : t -> int -> unit
       val set_tile_group_id : t -> int -> unit
       val set_table_id : t -> int -> unit
     end)
    (TileGroupHeader : sig
       type t
       val create : BackendType.t -> int -> t
       val set_tile_group : t -> TileGroup.t -> unit
     end) = struct

  let get_tile_group database_id table_id tile_group_id table schemas layout tuple_count =
    let backend_type = BackendType.MM in
    if layout = None then
      raise (Invalid_argument "Layout of the TileGroup must be non-null.")
    else
      let tile_header = TileGroupHeader.create backend_type tuple_count in
      let tile_group = TileGroup.create backend_type tile_header table schemas (Option.value_exn layout) tuple_count in
      TileGroupHeader.set_tile_group tile_header tile_group;
      TileGroup.set_database_id tile_group database_id;
      TileGroup.set_tile_group_id tile_group tile_group_id;
      TileGroup.set_table_id tile_group table_id;
      tile_group

end