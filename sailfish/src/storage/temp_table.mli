open Core

module type S = sig
  type t

  val invalid_oid : int
  val initial_txn_id : int
  val temptable_tilegroup_id : int
  val temptable_default_size : int

  val create :
    int -> Schema.t -> bool -> LayoutType.t -> t

  val insert_tuple :
    t -> Tuple.t -> (int * int) option

  val get_tile_group :
    t -> int -> TileGroup.t option

  val get_tile_group_by_id :
    t -> int -> TileGroup.t option

  val add_default_tile_group :
    t -> int

  val get_name :
    t -> string

  val get_tile_group_count :
    t -> int

  val get_tuple_count :
    t -> int

  val get_schema :
    t -> Schema.t
end