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
                     (Value : ValueType) : sig

  type column_statistics = {
    min_value: Value.t;
    max_value: Value.t;
  }

  type t

  val instance : t

  val get_instance : unit -> t

  val create_zone_map_table_in_catalog : t -> unit

  val create_zone_maps_for_table : t -> DataTable.t -> TransactionContext.t option -> unit

  val create_or_update_zone_map_for_tile_group : t -> DataTable.t -> int -> TransactionContext.t option -> unit

  val create_or_update_zone_map_in_catalog : t -> int -> int -> int -> int -> string -> string -> string -> TransactionContext.t option -> unit

  val get_zone_map_from_catalog : t -> int -> int -> int -> int -> column_statistics option

  val get_result_vector_as_zone_map : t -> string list -> column_statistics option
end