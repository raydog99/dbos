open Types
open Latches

let global_timestamp = ref 0L
let timestamp_lock = HybridLock.create ()

let new_timestamp () =
  HybridLock.lock timestamp_lock;
  global_timestamp := Int64.succ !global_timestamp;
  let ts = !global_timestamp in
  HybridLock.unlock timestamp_lock;
  ts

let start_transaction () =
  let start_time = new_timestamp () in
  { id = Int64.add start_time 1L; start_time; commit_time = None; status = Active; undo_buffer = { deltas = [] } }

let commit_transaction transaction =
  let commit_time = new_timestamp () in
  transaction.commit_time <- Some commit_time;
  transaction.status <- Committed;
  commit_time

let abort_transaction transaction =
  transaction.status <- Aborted