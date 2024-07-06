open B2_tree_types
open B2_tree_node
open B2_tree_generic

type t = {
  mutable btree: B2_tree_generic.t;
  mutable leaf: B2_tree_node.t;
  mutable pos: int;
  mutable key_buffer: bytes;
}

val create : B2_tree_generic.t -> t

val seek_exact : t -> string -> status

val seek : t -> string -> status

val next : t -> status

val prev : t -> status

val key : t -> bytes_with_length

val value : t -> bytes_with_length

val is_valid : t -> bool

val reset : t -> unit

val key_in_current_boundaries : t -> string -> bool