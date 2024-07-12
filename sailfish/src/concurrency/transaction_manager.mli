open Types
open Latches

val global_timestamp : int64 ref
val timestamp_lock : HybridLock.t

val new_timestamp : unit -> int64
val start_transaction : unit -> transaction
val commit_transaction : transaction -> timestamp
val abort_transaction : transaction -> unit