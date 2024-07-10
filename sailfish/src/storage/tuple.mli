open Core
open Schema

module type ValueType = sig
  type t

  val deserialize_from : string -> Type.t -> bool -> t
  val serialize_to : t -> string -> bool -> AbstractPool.t option -> unit
  val cast_as : t -> Type.t -> t
  val get_type_id : t -> Type.t
  val get_length : t -> int
  val is_null : t -> bool
  val compare_not_equals : t -> t -> CmpBool.t
  val compare_greater_than : t -> t -> CmpBool.t
  val compare_less_than : t -> t -> CmpBool.t
  val hash_combine : t -> int -> int
  val to_string : t -> string
end

module type AbstractPoolType = sig
  type t
end

module Make
    (Schema : SchemaType)
    (Value : ValueType)
    (AbstractPool : AbstractPoolType) : sig

  type t

  val create : Schema.t -> t
  val get_value : t -> int -> Value.t
  val set_value : t -> int -> Value.t -> AbstractPool.t option -> unit
  val set_from_tuple : t -> Value.t -> int list -> AbstractPool.t option -> unit
  val copy : t -> string -> AbstractPool.t option -> unit
  val export_serialization_size : t -> int
  val get_uninlined_memory_size : t -> int
  val serialize_with_header_to : t -> SerializeOutput.t -> unit
  val serialize_to : t -> SerializeOutput.t -> unit
  val serialize_to_export : t -> SerializeOutput.t -> int -> char array -> unit
  val equals_no_schema_check : t -> t -> bool
  val equals_no_schema_check_columns : t -> t -> int list -> bool
  val set_all_nulls : t -> unit
  val set_all_zeros : t -> unit
  val compare : t -> t -> int
  val compare_columns : t -> t -> int list -> int
  val hash_code : t -> int -> int
  val move_to_tuple : t -> string -> unit
  val hash_code_no_seed : t -> int
  val get_data_ptr : t -> int -> string
  val get_info : t -> string
end
