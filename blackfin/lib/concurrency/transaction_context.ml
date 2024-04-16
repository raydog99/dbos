module TransactionContext : TransactionContext = struct
  type t = {
    txn_id : int;
    thread_id : int;
    read_id : int;
    commit_id : int;
    timestamp : Int64.t;
    isolation_level : IsolationLevel.t;
  }

  let create thread_id isolation_level read_id ?(commit_id=0) =
    {
      txn_id = commit_id;
      thread_id;
      read_id;
      commit_id;
      timestamp = Int64.zero;
      isolation_level;
    }

  let get_thread_id (t : t) = t.thread_id

  let get_transaction_id (t : t) = t.txn_id

  let get_read_id (t : t) = t.read_id

  let get_commit_id (t : t) = t.commit_id

  let get_timestamp (t : t) = t.timestamp

  let get_transaction_level (t : t) = t.isolation_level

  let set_commit_id t commit_id = { t with commit_id }

  let set_timestamp t timestamp = { t with timestamp }
end
