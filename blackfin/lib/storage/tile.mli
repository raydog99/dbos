module Tile : sig
  type t = {
    database_id : int;
    table_id : int;
    table_group_id : int;
    tile_id : int;
    schema : Schema.t;
    tile_group : TileGroup.t;
    num_tuple_slots : int;
    column_count : int;
    data : bytes;
    tuple_length : int;
    tile_size : int;
    column_header : string;
    column_header_size : string;
  }

  val create :
    tile_header:string ->
    schema:Schema.t ->
    tile_group:TileGroup.t ->
    tile_count:int ->
    tile:int ->
    database_id:int ->
    table_id:int ->
    tile_group_id:int ->
    tile_id:int ->
    Schema.t ->
    t

  val insert_tuple : tuple_offset:int -> tuple -> unit

  val get_allocated_tuple_count : unit -> int

  val get_active_tuple_count : unit -> int

  val get_tuple_offset : tuple_address:int -> int

  val get_column_offset : name:string -> int

  val get_value : tuple_offset:int -> column_id:int -> 'a

  val set_value : 'a -> tuple_offset:int -> column_id:int -> unit

  val get_tuple : catalog:'a -> tuple_location:int -> int

  val get_schema : unit -> Schema.t

  val get_column_name : column_index:int -> string

  val get_column_count : unit -> int

  val get_header : unit -> string

  val get_tile_group : unit -> TileGroup.t

  val get_tile_id : unit -> int
end