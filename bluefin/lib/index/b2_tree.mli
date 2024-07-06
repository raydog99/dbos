open B2_tree_types

type t = B2_tree_generic.t

val create : dtid -> Config.t -> t
val find : t -> string -> string option
val insert : t -> string -> string -> unit
val remove : t -> string -> status

module Iterator : sig
  type t = B2_tree_iterator.t

  val create : B2_tree_generic.t -> t
  val seek_exact : t -> string -> status
  val seek : t -> string -> status
  val next : t -> status
  val prev : t -> status
  val key : t -> bytes_with_length
  val value : t -> bytes_with_length
  val is_valid : t -> bool
  val reset : t -> unit
end
