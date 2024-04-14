module TileGroup : sig
  type backend_type = 
    | InMemory
    | Disk

  type t = {
    database_id : int;
    table_id : int;
    tile_group_id : int;
    backend_type : backend_type;
    tiles : Tile.t list;
    tile_group_header : TileGroupHeader.t;
    table : AbstractTable.t;
    num_tuple_slots : int;
    tile_count : int;
    tile_group_layout : Layout.t option;
  }

  val create_tile_group :
    backend_type:backend_type ->
    tile_group_header:TileGroupHeader.t ->
    table:AbstractTable.t ->
    schemas:Schema.t list ->
    layout:Layout.t option ->
    tuple_count:int ->
    database_id:int ->
    table_id:int ->
    tile_group_id:int ->
    t
end