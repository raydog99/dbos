open B2_tree_types

type wal_log_type =
  | WALInitPage
  | WALLogicalSplit

type wal_init_page = {
  typ: wal_log_type;
  dt_id: dtid;
}

type wal_logical_split = {
  typ: wal_log_type;
  right_pid: int64;
  parent_pid: int64;
  left_pid: int64;
}

val init_wal : string -> unit

val close_wal : unit -> unit

val write_wal_entry : wal_log_type -> unit

val recover_from_wal : string -> unit