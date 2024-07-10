open Core

type t

val create : int -> t

val finalize : t -> unit

val add_table : t -> DataTable.t -> is_catalog:bool -> unit

val get_table_with_oid : t -> int -> DataTable.t

val drop_table_with_oid : t -> int -> unit

val get_table : t -> int -> DataTable.t

val get_table_count : t -> int

val get_info : t -> string

val get_db_name : t -> string

val set_db_name : t -> string -> unit