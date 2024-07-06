open B2_tree_types

type slot = {
  mutable head: int;
  mutable key_len: int;
  mutable payload_len: int;
  mutable offset: int;
}

type fence_key = {
  mutable offset: int;
  mutable length: int;
}

type t = {
  mutable is_leaf: bool;
  mutable count: int;
  mutable data_offset: int;
  mutable space_used: int;
  mutable prefix_length: int;
  mutable lower_fence: fence_key;
  mutable upper_fence: fence_key;
  mutable slots: slot array;
  mutable data: bytes;
  mutable upper: swip_type;
}

val create : bool -> t
val get_key : t -> int -> string
val get_payload : t -> int -> string
val get_full_key_len : t -> int -> int
val get_lower_fence_key : t -> string
val get_upper_fence_key : t -> string
val compare_keys : string -> int -> string -> int -> int
val lower_bound : t -> string -> int -> int
val insert_do_not_copy_payload : t -> string -> int -> int -> int -> int
val set_fences : t -> string -> int -> string -> int -> unit
val find_sep : t -> { length: int; slot: int; trunc: bool }
val get_sep : t -> { length: int; slot: int; trunc: bool } -> string
val split : t -> t -> t -> int -> string -> int -> t * t
val remove_slot : t -> int -> unit
val remove : t -> string -> int -> bool