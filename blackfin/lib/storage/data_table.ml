open Printf

module DataTable = struct
  type t = {
    mutable active_tilegroup_count_ : int;
    mutable active_indirection_array_count_ : int;
    database_oid : oid_t;
    table_name : string;
    tuples_per_tilegroup_ : int;
  }

  let invalid_tile_group_id = -1
  let default_active_tilegroup_count_ = 1
  let default_active_indirection_array_count_ = 1

  let create ~schema ~table_name ~database_oid ~table_oid ~tuples_per_tilegroup =
    let active_tilegroup_count_, active_indirection_array_count_ =
      if is_catalog then
        1, 1
      else
        default_active_tilegroup_count_, default_active_indirection_array_count_ in
    {
      active_tilegroup_count_;
      active_indirection_array_count_;
      database_oid;
      table_name;
      tuples_per_tilegroup_;
      adapt_table_;
      trigger_list_;
    }
end