module type TileGroupHeader = sig
  type t = {
    tile_group : TileGroup.t option;
    num_tuple_slots : int;
    mutable next_tuple_slot : int;
    tile_header_lock : unit;
    tuple_headers : TupleHeader.t array;
    mutable immutable : bool;
  }

  val create : int -> t
  val get_active_tuple_count : t -> int
end
