open Sync_primitives

val page_size : int

type lid = int64
type dtid = int
type pid = int64
type worker_id = int

type state = FREE | HOT | COOL | LOADED

module Page : sig
  type t
  val create : unit -> t
end

module Counter : sig
  type t
  val create : unit -> t
  val increment : t -> unit
  val reset : t -> unit
end

module ContentionTracker : sig
  type t = {
    restarts_counter: Counter.t;
    access_counter: Counter.t;
    mutable last_modified_pos: int32;
  }
  val create : unit -> t
  val reset : t -> unit
end

module Header : sig
  type t = {
	mutable last_writer_worker_id: worker_id;
	mutable last_written_plsn: lid;
	mutable state: state;
	mutable is_being_written_back: bool;
	mutable keep_in_memory: bool;
	mutable pid: pid;
	mutable latch: Latch.HybridLatch.t;
	mutable contention_tracker: ContentionTracker.t;
	mutable crc: int64;
	}
  val create : unit -> t
end

type t = {
  mutable header : Header.t;
  mutable page : Page.t;
}

val create : unit -> t
val is_dirty : t -> bool
val is_free : t -> bool
val reset : t -> unit