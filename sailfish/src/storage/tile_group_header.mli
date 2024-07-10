open Core
open Backend_manager

module type ItemPointer = sig
  type t = { block : int; offset : int }

  val invalid : t
end

module Make (Backend : BackendType) (ItemPointer : ItemPointer) : sig

  type tuple_header = {
    mutable transaction_id: int;
    mutable last_reader_commit_id: int;
    mutable begin_commit_id: int;
    mutable end_commit_id: int;
    mutable next_item_pointer: ItemPointer.t;
    mutable prev_item_pointer: ItemPointer.t;
    mutable indirection: int option;
  }

  type t

  val invalid_txn_id : int

  val invalid_cid : int

  val max_cid : int

  val max_txn_id : int

  val start_oid : int

  val create : Backend.t -> int -> t

  val get_info : t -> string

  val print_visibility : t -> int -> int -> unit

  val get_active_tuple_count : t -> int
end