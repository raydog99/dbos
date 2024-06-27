(* Constants *)
let page_size = 4 * 1024
let effective_page_size = page_size - 24 (* Adjusted for OCaml's lack of exact C-style struct packing *)

(* Types *)
type worker_id = int
type lid = int64
type pid = int64
type dtid = int

(* Enums *)
type state = Free | Hot | Cool | Loaded

(* Structures *)
type content_tracker = {
  mutable restarts_counter: int;
  mutable access_counter: int;
  mutable last_modified_pos: int;
}

type optimistic_parent_pointer = {
  mutable parent_bf: buffer_frame option;
  mutable parent_pid: pid;
  mutable parent_plsn: lid;
  mutable swip_ptr: buffer_frame option ref option;
  mutable pos_in_parent: int;
}

and buffer_frame = {
  mutable last_writer_worker_id: worker_id;
  mutable last_written_plsn: lid;
  mutable state: state;
  mutable is_being_written_back: bool;
  mutable keep_in_memory: bool;
  mutable pid: pid;
  mutable latch: int64; (* Simplified latch representation *)
  mutable next_free_bf: buffer_frame option;
  mutable contention_tracker: content_tracker;
  mutable optimistic_parent_pointer: optimistic_parent_pointer;
  mutable crc: int64;
  mutable plsn: lid;
  mutable gsn: lid;
  mutable dt_id: dtid;
  mutable magic_debugging_number: int64;
  mutable dt: bytes;
}

(* Functions *)
let create_buffer_frame () =
  {
    last_writer_worker_id = max_int;
    last_written_plsn = 0L;
    state = Free;
    is_being_written_back = false;
    keep_in_memory = false;
    pid = 9999L;
    latch = 0L;
    next_free_bf = None;
    contention_tracker = { restarts_counter = 0; access_counter = 0; last_modified_pos = -1 };
    optimistic_parent_pointer = {
      parent_bf = None;
      parent_pid = 0L;
      parent_plsn = 0L;
      swip_ptr = None;
      pos_in_parent = -1;
    };
    crc = 0L;
    plsn = 0L;
    gsn = 0L;
    dt_id = 9999;
    magic_debugging_number = 0L;
    dt = Bytes.create effective_page_size;
  }

let is_dirty bf = bf.plsn <> bf.last_written_plsn

let is_free bf = bf.state = Free

let reset bf =
  bf.crc <- 0L;
  bf.last_writer_worker_id <- max_int;
  bf.last_written_plsn <- 0L;
  bf.state <- Free;
  bf.is_being_written_back <- false;
  bf.pid <- 9999L;
  bf.next_free_bf <- None;
  bf.contention_tracker <- { restarts_counter = 0; access_counter = 0; last_modified_pos = -1 };
  bf.keep_in_memory <- false;

let update_optimistic_parent_pointer opp new_parent_bf new_parent_pid new_parent_gsn new_swip_ptr new_pos_in_parent =
  if opp.parent_bf <> new_parent_bf || opp.parent_pid <> new_parent_pid || 
     opp.parent_plsn <> new_parent_gsn || opp.swip_ptr <> new_swip_ptr || 
     opp.pos_in_parent <> new_pos_in_parent then
    begin
      opp.parent_bf <- new_parent_bf;
      opp.parent_pid <- new_parent_pid;
      opp.parent_plsn <- new_parent_gsn;
      opp.swip_ptr <- new_swip_ptr;
      opp.pos_in_parent <- new_pos_in_parent
    end