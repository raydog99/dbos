open Core
open Backend_manager

module type ItemPointer = sig
  type t = { block : int; offset : int }
  val invalid : t
end

module Make (Backend : BackendType) (ItemPointer : ItemPointer) = struct
  type tuple_header = {
    mutable transaction_id: int;
    mutable last_reader_commit_id: int;
    mutable begin_commit_id: int;
    mutable end_commit_id: int;
    mutable next_item_pointer: ItemPointer.t;
    mutable prev_item_pointer: ItemPointer.t;
    mutable indirection: int option;
  }

  type t = {
    backend_type: Backend.t;
    mutable tile_group: TileGroup.t;
    num_tuple_slots: int;
    mutable next_tuple_slot: int;
    tile_header_lock: Mutex.t;
    tuple_headers: tuple_header array;
    mutable immutable: bool;
  }

  let invalid_txn_id = -1
  let invalid_cid = -1
  let max_cid = Int.max_value
  let max_txn_id = Int.max_value
  let start_oid = 0

  let create backend_type tuple_count =
    let t = {
      backend_type;
      tile_group = ();
      num_tuple_slots = tuple_count;
      next_tuple_slot = 0;
      tile_header_lock = Mutex.create ();
      tuple_headers = Array.create ~len:tuple_count {
        transaction_id = invalid_txn_id;
        last_reader_commit_id = invalid_cid;
        begin_commit_id = max_cid;
        end_commit_id = max_cid;
        next_item_pointer = ItemPointer.invalid;
        prev_item_pointer = ItemPointer.invalid;
        indirection = None;
      };
      immutable = false;
    } in
    t

  let get_info t =
    let buf = Buffer.create 256 in
    Printf.bprintf buf "TILE GROUP HEADER (Address:%s, NumActiveTuples:%d, Immutable: %b)\n"
      (Stdlib.Obj.magic t :> string) (get_active_tuple_count t) t.immutable;
    
    let active_tuple_slots = get_current_next_tuple_slot t in
    for header_itr = 0 to active_tuple_slots - 1 do
      let txn_id = get_transaction_id t header_itr in
      let beg_commit_id = get_begin_commit_id t header_itr in
      let end_commit_id = get_end_commit_id t header_itr in

      Printf.bprintf buf "%04d: TxnId:%s BeginCommitId:%s EndCId:%s\n"
        header_itr
        (if txn_id = max_txn_id then "MAX_TXN_ID" else Printf.sprintf "%010d" txn_id)
        (if beg_commit_id = max_cid then "MAX_CID" else Printf.sprintf "%010d" beg_commit_id)
        (if end_commit_id = max_cid then "MAX_CID" else Printf.sprintf "%010d" end_commit_id);

      let next_pointer = get_next_item_pointer t header_itr in
      let prev_pointer = get_prev_item_pointer t header_itr in

      Printf.bprintf buf "      Next:[%s, %s] Prev:[%s, %s]\n"
        (if next_pointer.block = -1 then "INVALID_OID" else string_of_int next_pointer.block)
        (if next_pointer.offset = -1 then "INVALID_OID" else string_of_int next_pointer.offset)
        (if prev_pointer.block = -1 then "INVALID_OID" else string_of_int prev_pointer.block)
        (if prev_pointer.offset = -1 then "INVALID_OID" else string_of_int prev_pointer.offset);
    done;
    Buffer.contents buf

  let print_visibility t txn_id at_cid =
    let active_tuple_slots = get_current_next_tuple_slot t in
    let buf = Buffer.create 256 in
    Printf.bprintf buf "%s\n" (String.make 80 '-');

    for header_itr = 0 to active_tuple_slots - 1 do
      let own = (txn_id = get_transaction_id t header_itr) in
      let activated = (at_cid >= get_begin_commit_id t header_itr) in
      let invalidated = (at_cid >= get_end_commit_id t header_itr) in

      let txn_id = get_transaction_id t header_itr in
      let beg_commit_id = get_begin_commit_id t header_itr in
      let end_commit_id = get_end_commit_id t header_itr in

      Printf.bprintf buf " slot :: %10d txn id : %10s beg cid : %10s end cid : %10s"
        header_itr
        (if txn_id = max_txn_id then "MAX_TXN_ID" else string_of_int txn_id)
        (if beg_commit_id = max_cid then "MAX_CID" else string_of_int beg_commit_id)
        (if end_commit_id = max_cid then "MAX_CID" else string_of_int end_commit_id);

      let location = get_next_item_pointer t header_itr in
      Printf.bprintf buf " prev : [ %d , %d ]" location.block location.offset;

      Printf.bprintf buf " own : %b activated : %b invalidated : %b  [ %b  ]\n"
        own activated invalidated
        (((not own) && activated && (not invalidated)) ||
         (own && (not activated) && (not invalidated)));
    done;

    Printf.bprintf buf "%s\n" (String.make 80 '-');
    Log.trace (fun m -> m "%s" (Buffer.contents buf))

  let get_active_tuple_count t =
    Array.fold t.tuple_headers ~init:0 ~f:(fun acc header ->
      if header.transaction_id <> invalid_txn_id then
        begin
          assert (header.transaction_id = 0); (* INITIAL_TXN_ID *)
          acc + 1
        end
      else
        acc
    )

end