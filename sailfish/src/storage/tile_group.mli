open Core
open Schema
open Tile
open Tile_group_header
open Abstract_table
open Layout

module Make (Schema : SchemaType) (Tile : TileType) (TileGroupHeader : TileGroupHeaderType)
            (AbstractTable : AbstractTableType) (Layout : LayoutType) : sig
  type t

  val create : BackendType.t -> TileGroupHeader.t -> AbstractTable.t -> Schema.t list -> Layout.t -> int -> t
  val get_tile_id : t -> int -> int
  val get_tile_pool : t -> int -> AbstractPool.t option
  val get_tile_group_id : t -> int
  val get_next_tuple_slot : t -> int
  val get_active_tuple_count : t -> int
  val insert_tuple : t -> Tuple.t option -> int
  val insert_tuple_from_recovery : t -> int -> int -> Tuple.t -> int
  val delete_tuple_from_recovery : t -> int -> int -> int
  val update_tuple_from_recovery : t -> int -> int -> ItemPointer.t -> int
  val insert_tuple_from_checkpoint : t -> int -> Tuple.t -> int -> int
  val get_value : t -> int -> int -> Value.t
  val set_value : t -> Value.t -> int -> int -> unit
  val get_tile_reference : t -> int -> Tile.t
  val sync : t -> unit
  val get_info : t -> string
end