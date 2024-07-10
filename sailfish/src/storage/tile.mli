open Core

module type S = sig
  type t

  val invalid_oid : int
  val initial_txn_id : int
  val temptable_tilegroup_id : int
  val temptable_default_size : int

  val create :
    BackendType.t -> TileGroupHeader.t -> Schema.t -> TileGroup.t -> int -> t

  val get_allocated_tuple_count :
    t -> int

  val get_tuple_location :
    t -> int -> string

  val insert_tuple :
    t -> int -> Tuple.t -> unit

  val get_value :
    t -> int -> int -> Value.t

  val get_value_fast :
    t -> int -> int -> Type.t -> bool -> Value.t

  val set_value :
    t -> Value.t -> int -> int -> unit

  val set_value_fast :
    t -> Value.t -> int -> int -> int -> unit

  val copy_tile :
    t -> BackendType.t -> t

  val get_info :
    t -> string

  val serialize_to :
    t -> SerializeOutput.t -> int -> bool

  val serialize_header_to :
    t -> SerializeOutput.t -> bool

  val serialize_tuples_to :
    t -> SerializeOutput.t -> Tuple.t array -> int -> bool

  val deserialize_tuples_from :
    t -> SerializeInput.t -> AbstractPool.t -> unit

  val deserialize_tuples_from_without_header :
    t -> SerializeInput.t -> AbstractPool.t -> unit
end
