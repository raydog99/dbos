open Types
open Latches

val global_lock : HybridLock.t

val create_version_vector : int -> version_vector
val get_visible_version : transaction_id -> timestamp -> version_delta option -> balance option
val read_version : transaction -> version_vector -> int -> balance option
val update_version : transaction -> version_vector -> int -> balance -> unit
val create_versioned_positions : int -> versioned_positions
val update_versioned_positions : versioned_positions -> int -> unit
val get_scan_ranges : versioned_positions -> scan_range array
val scan_records : transaction -> version_vector -> versioned_positions -> (int -> balance -> unit) -> unit