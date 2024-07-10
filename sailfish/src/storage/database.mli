open Core

module TriggerList : sig
  type t
  val create : unit -> t
  val add_trigger : t -> Trigger.t -> t
  val get_trigger_list_size : t -> int
  val get : t -> int -> Trigger.t option
end

type item_pointer = {
  block : int;
  offset : int;
}

type t

val create : Schema.t -> string -> int -> int -> int -> bool -> bool -> bool -> LayoutType.t -> t

val finalize : t -> unit

val check_constraints : t -> Tuple.t -> bool

val get_empty_tuple_slot : t -> Tuple.t option -> item_pointer

val insert_empty_version : t -> item_pointer option

val acquire_version : t -> item_pointer option

val install_version : t -> Tuple.t -> (int * 'a) list option -> TransactionContext.t -> item_pointer option ref -> bool

val insert_tuple : t -> Tuple.t -> TransactionContext.t -> item_pointer option ref -> bool -> item_pointer option

val insert_tuple_without_index : t -> Tuple.t -> item_pointer option

val insert_in_indexes : t -> Tuple.t -> item_pointer -> TransactionContext.t -> item_pointer option ref -> bool

val insert_in_secondary_indexes : t -> Tuple.t -> (int * 'a) list option -> TransactionContext.t -> item_pointer option -> bool

val check_foreign_key_src_and_cascade : t -> Tuple.t -> Tuple.t -> TransactionContext.t -> ExecutorContext.t -> bool -> bool

val check_foreign_key_constraints : t -> Tuple.t -> TransactionContext.t -> bool

val increase_tuple_count : t -> int -> unit

val decrease_tuple_count : t -> int -> unit

val set_tuple_count : t -> int -> unit

val get_tuple_count : t -> int

val is_dirty : t -> bool

val reset_dirty : t -> unit

val get_tile_group_with_layout : t -> Layout.t -> TileGroup.t

val add_default_indirection_array : t -> int -> int

val add_default_tile_group : t -> int

val add_tile_group_with_oid_for_recovery : t -> int -> unit

val add_tile_group : t -> TileGroup.t -> unit

val get_tile_group_count : t -> int

val get_tile_group : t -> int -> TileGroup.t option

val get_tile_group_by_id : t -> int -> TileGroup.t

val drop_tile_groups : t -> unit

val add_index : t -> Index.t -> unit

val get_index_with_oid : t -> int -> Index.t

val drop_index_with_oid : t -> int -> unit

val drop_indexes : t -> unit

val get_index : t -> int -> Index.t option

val get_index_attrs : t -> int -> Int.Set.t

val get_index_count : t -> int

val get_valid_index_count : t -> int

val transform_tile_group : t -> int -> float -> TileGroup.t option

val record_layout_sample : t -> Sample.t -> unit

val get_layout_samples : t -> Sample.t list

val clear_layout_samples : t -> unit

val record_index_sample : t -> Sample.t -> unit

val get_index_samples : t -> Sample.t list

val clear_index_samples : t -> unit

val add_trigger : t -> Trigger.t -> unit

val get_trigger_number : t -> int

val get_trigger_by_index : t -> int -> Trigger.t option

val get_trigger_list : t -> TriggerList.t option

val update_trigger_list_from_catalog : t -> TransactionContext.t -> unit

val hash : t -> int

val equals : t -> t -> bool

val set_current_layout_oid : t -> int -> bool

val get_schema : t -> Schema.t

val get_name : t -> string

val get_oid : t -> int

val get_database_oid : t -> int

val get_default_layout : t -> Layout.t

val set_default_layout : t -> Layout.t -> unit

val get_active_tile_group_count : t -> int

val set_active_tile_group_count : t -> int -> unit

val get_active_indirection_array_count : t -> int

val set_active_indirection_array_count : t -> int -> unit

val get_current_layout_oid : t -> int

val is_base_table : t -> bool

val get_table_constraint_type : t -> ConstraintType.t

val has_primary_key : t -> bool

val has_foreign_key : t -> bool

val get_schema_columns : t -> Column.t list

val get_column_count : t -> int

val get_column_name : t -> int -> string

val get_column_names : t -> string list

val get_column_types : t -> TypeId.t list

val get_column_lengths : t -> int list

val validate_columns : t -> int list -> bool