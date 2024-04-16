open DataTable

module type Database : sig
  type t = {
    database_id : int;
    database_name : string;
    tables : DataTable.t list;
  }

  val create : ?database_id:int -> unit -> t
  val get_id : t -> int
  val add_table : t -> DataTable.t -> is_catalog:bool -> unit
  val get_table : t -> table_offset:int -> DataTable.t option
  val get_table_with_id : t -> table_id:int -> DataTable.t option
  val get_table_count : t -> int
  val get_info : t -> string
  val get_name : t -> string
  val set_name : t -> string -> unit
end