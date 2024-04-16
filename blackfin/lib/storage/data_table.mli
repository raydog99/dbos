module type DataTable = sig
  type t = {
    mutable active_tilegroup_count_ : int;
    mutable active_indirection_array_count_ : int;
    database_oid : oid_t;
    table_name : string;
    tuples_per_tilegroup_ : int;
  }

  val invalid_tile_group_id : int
  val default_active_tilegroup_count_ : int
  val default_active_indirection_array_count_ : int

  val create : schema:catalog_schema -> table_name:string -> database_oid:oid_t -> table_oid:oid_t -> tuples_per_tilegroup:int -> t
end
