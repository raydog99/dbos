open B2_tree_types
open B2_tree_node

type t = {
  mutable dt_id: dtid;
  mutable config: Config.t;
  mutable meta_node: B2_tree_node.t;
  mutable height: int;
}

val create : dtid -> Config.t -> t

val find : t -> string -> string option

val insert : t -> string -> string -> unit

val remove : t -> string -> status