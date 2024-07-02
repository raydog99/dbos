open Buffer_manager

module Lsn = struct
  type t = int64
  let compare = Int64.compare
  let to_string = Int64.to_string
  let of_int = Int64.of_int
  let next lsn = Int64.add lsn 1L
end

type log_record_type =
  | Update
  | Commit
  | Abort
  | Begin
  | End

type log_record = {
  lsn: Lsn.t;
  txn_id: int;
  record_type: log_record_type;
  page_id: int option;
  offset: int option;
  before_image: string option;
  after_image: string option;
  prev_lsn: Lsn.t option;
}

module LogManager = struct
  type t = {
    mutable log: log_record list;
    mutable last_lsn: Lsn.t;
    mutable flushed_lsn: Lsn.t;
  }

  let create () = {
    log = [];
    last_lsn = Lsn.of_int 0;
    flushed_lsn = Lsn.of_int 0;
  }

  let append_log_record lm record =
    let new_lsn = Lsn.next lm.last_lsn in
    let new_record = { record with lsn = new_lsn } in
    lm.log <- new_record :: lm.log;
    lm.last_lsn <- new_lsn;
    new_lsn

  let flush_log lm =
    lm.flushed_lsn <- lm.last_lsn

  let get_log_records lm = List.rev lm.log
end

module TransactionTable = struct
  type transaction_status = Active | Committed | Aborted

  type transaction_entry = {
    txn_id: int;
    mutable status: transaction_status;
    mutable last_lsn: Lsn.t option;
  }

  type t = (int, transaction_entry) Hashtbl.t

  let create () = Hashtbl.create 16

  let begin_transaction tt txn_id =
    let entry = { txn_id; status = Active; last_lsn = None } in
    Hashtbl.add tt txn_id entry

  let update_transaction tt txn_id lsn =
    match Hashtbl.find_opt tt txn_id with
    | Some entry -> entry.last_lsn <- Some lsn
    | None -> failwith "Transaction not found"

  let end_transaction tt txn_id status =
    match Hashtbl.find_opt tt txn_id with
    | Some entry -> entry.status <- status
    | None -> failwith "Transaction not found"

  let get_active_transactions tt =
    Hashtbl.fold (fun _ entry acc ->
      if entry.status = Active then entry :: acc else acc
    ) tt []
end

module DirtyPageTable = struct
  type dirty_page_entry = {
    page_id: int;
    recovery_lsn: Lsn.t;
  }

  type t = (int, dirty_page_entry) Hashtbl.t

  let create () = Hashtbl.create 16

  let add_dirty_page dpt page_id recovery_lsn =
    if not (Hashtbl.mem dpt page_id) then
      Hashtbl.add dpt page_id { page_id; recovery_lsn }

  let remove_dirty_page dpt page_id =
    Hashtbl.remove dpt page_id

  let get_recovery_lsn dpt page_id =
    match Hashtbl.find_opt dpt page_id with
    | Some entry -> Some entry.recovery_lsn
    | None -> None
end

module AriesRecovery = struct
  type recovery_state = {
    bpm: BufferPoolManager.t;
    log_manager: LogManager.t;
    transaction_table: TransactionTable.t;
    dirty_page_table: DirtyPageTable.t;
    mutable redo_lsn: Lsn.t;
  }

  let create bpm log_manager =
    {
      bpm;
      log_manager;
      transaction_table = TransactionTable.create ();
      dirty_page_table = DirtyPageTable.create ();
      redo_lsn = Lsn.of_int 0;
    }

  let analysis phase state =
    let log_records = LogManager.get_log_records state.log_manager in
    List.iter (fun record ->
      match record.record_type with
      | Begin ->
          TransactionTable.begin_transaction state.transaction_table record.txn_id
      | Update ->
          TransactionTable.update_transaction state.transaction_table record.txn_id record.lsn;
          (match record.page_id with
           | Some page_id -> DirtyPageTable.add_dirty_page state.dirty_page_table page_id record.lsn
           | None -> ())
      | Commit | Abort ->
          TransactionTable.end_transaction state.transaction_table record.txn_id
            (if record.record_type = Commit then TransactionTable.Committed else TransactionTable.Aborted)
      | End -> ()
    ) log_records;
    state.redo_lsn <- Lsn.of_int 0

  let redo phase state =
    let log_records = LogManager.get_log_records state.log_manager in
    List.iter (fun record ->
      if Lsn.compare record.lsn state.redo_lsn >= 0 then
        match record.record_type with
        | Update ->
            (match record.page_id, record.offset, record.after_image with
             | Some page_id, Some offset, Some after_image ->
                 let page = BufferPoolManager.get_page state.bpm page_id in
                 BufferFrame.write page offset after_image;
                 BufferPoolManager.unpin_page state.bpm page_id false
             | _ -> ())
        | _ -> ()
    ) log_records

  let undo phase state =
    let active_transactions = TransactionTable.get_active_transactions state.transaction_table in
    let rec undo_transaction txn_entry =
      match txn_entry.TransactionTable.last_lsn with
      | Some lsn ->
          let record = List.find (fun r -> r.lsn = lsn) (LogManager.get_log_records state.log_manager) in
          (match record.record_type with
           | Update ->
               (match record.page_id, record.offset, record.before_image with
                | Some page_id, Some offset, Some before_image ->
                    let page = BufferPoolManager.get_page state.bpm page_id in
                    BufferFrame.write page offset before_image;
                    BufferPoolManager.unpin_page state.bpm page_id false
                | _ -> ())
           | _ -> ());
          undo_transaction { txn_entry with TransactionTable.last_lsn = record.prev_lsn }
      | None -> ()
    in
    List.iter undo_transaction active_transactions

  let recover state =
    analysis state;
    redo state;
    undo state;
    ()
end