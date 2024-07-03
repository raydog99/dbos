module TransactionContext = struct
  type t = {
    txn_id: int64;
    start_time: int64;
    mutable commit_time: int64 option;
    mutable status: [`Active | `Committed | `Aborted];
    read_set: (string * int64) list ref;
    write_set: (string * Version.t) list ref;
  }

  let create txn_id start_time =
    { txn_id; start_time; commit_time = None; status = `Active; 
      read_set = ref []; write_set = ref [] }

  let add_read_entry context key timestamp =
    context.read_set := (key, timestamp) :: !(context.read_set)

  let add_write_entry context key version =
    context.write_set := (key, version) :: !(context.write_set)

  let set_committed context commit_time =
    context.status <- `Committed;
    context.commit_time <- Some commit_time

  let set_aborted context =
    context.status <- `Aborted

  let is_active context = context.status = `Active
  let is_committed context = context.status = `Committed
  let is_aborted context = context.status = `Aborted
end