open Core

type t

val empty_tile_group : TileGroup.t option ref

val create : unit -> t

val instance : t Lazy.t

val get_instance : unit -> t

val get_database_with_oid : t -> int -> Database.t

val get_table_with_oid : t -> int -> int -> DataTable.t

val get_index_with_oid : t -> int -> int -> int -> Index.t

val get_database_with_offset : t -> int -> Database.t

val has_database : t -> int -> bool

val destroy_databases : t -> unit

val remove_database_from_storage_manager : t -> int -> bool

val add_tile_group : t -> int -> TileGroup.t -> unit

val drop_tile_group : t -> int -> unit

val get_tile_group : t -> int -> TileGroup.t option

val clear_tile_group : t -> unit

val add_database : t -> Database.t -> unit

val get_database_count : t -> int

val get_tile_group_count : t -> int
