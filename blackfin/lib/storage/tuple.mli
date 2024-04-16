open Schema

module type Tuple = sig
  type tuple = {
    tuple_schema : Schema;
    tuple_data : char array;
    allocated : bool;
  }

  val create : Schema -> tuple
  val create_with_data : Schema -> char array -> tuple
  val copy : tuple -> tuple
  val move : tuple -> char array -> unit
  val equal : tuple -> tuple -> bool
  val not_equal : tuple -> tuple -> bool
  val set_value_without_pool : tuple -> int -> 'a -> unit
  val get_length : tuple -> int
  val is_null : tuple -> int -> bool
  val is_tuple_null : tuple -> bool
  val get_type : tuple -> int -> 'a
  val get_schema : tuple -> Schema
end