open Sync_primitives

type state = FREE | HOT | COOL | LOADED

let page_size = 4 * 1024

type pid = int64
type worker_id = int
type lid = int64
type dtid = int

module Page : sig
  type t = {
    mutable plsn: lid;
    mutable gsn: lid;
    mutable dt_id: dtid;
    mutable magic_debugging_number: int64;
    mutable dt: bytes;
  }
  val create : unit -> t
end = struct
  type t = {
    mutable plsn: lid;
    mutable gsn: lid;
    mutable dt_id: dtid;
    mutable magic_debugging_number: int64;
    mutable dt: bytes;
  }

  let create () = {
    plsn = 0L;
    gsn = 0L;
    dt_id = 9999;
    magic_debugging_number = 0L;
    dt = Bytes.create page_size;
  }
end

module Counter = struct
  type t = { mutable value: int32 }
  let create () = { value = 0l }
  let increment t = t.value <- Int32.succ t.value
  let reset t = t.value <- 0l
end

module ContentionTracker = struct
  type t = {
    restarts_counter: Counter.t;
    access_counter: Counter.t;
    mutable last_modified_pos: int32;
  }

  let create () = {
    restarts_counter = Counter.create ();
    access_counter = Counter.create ();
    last_modified_pos = -1l;
  }

  let reset t =
    Counter.reset t.restarts_counter;
    Counter.reset t.access_counter;
    t.last_modified_pos <- -1l
end

module Header = struct
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

  let create () = {
    last_writer_worker_id = max_int;
    last_written_plsn = 0L;
    state = FREE;
    is_being_written_back = false;
    keep_in_memory = false;
    pid = 9999L;
    latch = Latch.HybridLatch.create 0L;
    contention_tracker = ContentionTracker.create ();
    crc = 0L;
  }

end
  
type t = {
  mutable header : Header.t;
  mutable page : Page.t;
}

let create () = {
  header = Header.create ();
  page = Page.create ();
}

let is_dirty t = t.page.plsn <> t.header.last_written_plsn

let is_free t = t.header.state = FREE

let reset t =
  t.header.crc <- 0L;
  assert (not t.header.is_being_written_back);
  Latch.HybridLatch.assert_exclusively_latched t.header.latch;
  t.header.last_writer_worker_id <- max_int;
  t.header.last_written_plsn <- 0L;
  t.header.state <- FREE;
  t.header.is_being_written_back <- false;
  t.header.pid <- 9999L;
  ContentionTracker.reset t.header.contention_tracker;
  t.header.keep_in_memory <- false;