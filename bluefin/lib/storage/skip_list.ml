open Mutex

module FreeList = struct
  type buffer_frame = {
    mutable header: buffer_frame_header;
  }
  and buffer_frame_header = {
    mutable next_free_bf: buffer_frame option;
    mutable state: buffer_frame_state;
    latch: latch;
  }
  and buffer_frame_state = Free | InUse
  and latch = {
    mutable exclusive: bool;
  }

  type t = {
    mutable head: buffer_frame option;
    mutable counter: int64;
    mutex: Mutex.t;
  }

  let create () = {
    head = None;
    counter = 0L;
    mutex = Mutex.create ();
  }

  let batch_push t batch_head batch_tail batch_counter =
    Mutex.lock t.mutex;
    (match t.head with
    | Some old_head -> batch_tail.header.next_free_bf <- Some old_head
    | None -> ());
    t.head <- Some batch_head;
    t.counter <- Int64.add t.counter batch_counter;
    Mutex.unlock t.mutex

  let push t bf =
    assert (bf.header.state = Free);
    assert (not bf.header.latch.exclusive);
    Mutex.lock t.mutex;
    bf.header.next_free_bf <- t.head;
    t.head <- Some bf;
    t.counter <- Int64.succ t.counter;
    Mutex.unlock t.mutex

  let try_pop t =
    Mutex.lock t.mutex;
    match t.head with
    | Some free_bf ->
        t.head <- free_bf.header.next_free_bf;
        t.counter <- Int64.pred t.counter;
        assert (free_bf.header.state = Free);
        Mutex.unlock t.mutex;
        Some free_bf
    | None ->
        Mutex.unlock t.mutex;
        None
end