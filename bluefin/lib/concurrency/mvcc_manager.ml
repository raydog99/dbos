module MVCCManager = struct
  type t = {
    data: VersionedData.t;
    mutable next_txn_id: int64;
    mutable next_timestamp: int64;
    active_transactions: (int64, TransactionContext.t) Hashtbl.t;
  }

  let create () = {
    data = VersionedData.create ();
    next_txn_id = 1L;
    next_timestamp = 0L;
    active_transactions = Hashtbl.create 100;
  }

  let start_transaction mvcc =
    let txn_id = mvcc.next_txn_id in
    let start_time = mvcc.next_timestamp in
    mvcc.next_txn_id <- Int64.succ mvcc.next_txn_id;
    mvcc.next_timestamp <- Int64.succ mvcc.next_timestamp;
    let context = TransactionContext.create txn_id start_time in
    Hashtbl.add mvcc.active_transactions txn_id context;
    context

  let read mvcc context key =
    let version_opt = VersionedData.get_version mvcc.data key context.TransactionContext.start_time context.TransactionContext.txn_id in
    match version_opt with
    | Some version ->
        TransactionContext.add_read_entry context key (Version.get_timestamp version);
        Some (Version.get_data version)
    | None -> None

  let write mvcc context key data =
    let new_version = Version.create data context.TransactionContext.start_time context.TransactionContext.txn_id None in
    TransactionContext.add_write_entry context key new_version;
    VersionedData.add_or_update mvcc.data key new_version

  let commit mvcc context =
    let commit_time = mvcc.next_timestamp in
    mvcc.next_timestamp <- Int64.succ mvcc.next_timestamp;
    TransactionContext.set_committed context commit_time;
    List.iter (fun (key, version) ->
      VersionedData.add_or_update mvcc.data key version
    ) !(context.TransactionContext.write_set);
    Hashtbl.remove mvcc.active_transactions context.TransactionContext.txn_id

  let abort mvcc context =
    TransactionContext.set_aborted context;
    Hashtbl.remove mvcc.active_transactions context.TransactionContext.txn_id
end