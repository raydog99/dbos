open Thread
open Mutex
open Condition

module Atomic = struct
  include Atomic

  let compare_and_set r expected new_value =
    Atomic.compare_and_set r expected new_value |> (fun x -> x = expected)
end

module type LOCK = sig
  type t
  val create : unit -> t
  val lock : t -> unit
  val unlock : t -> unit
  val try_lock : t -> bool
end

module type RW_LOCK = sig
  include LOCK
  val lock_shared : t -> unit
  val unlock_shared : t -> unit
  val try_lock_shared : t -> bool
end

module SpinLock : LOCK = struct
  type t = { mutable locked : bool }

  let create () = { locked = false }

  let rec lock t =
    while not (Atomic.compare_and_set t.locked false true) do
      while Atomic.get t.locked do Thread.yield () done
    done

  let unlock t = Atomic.set t.locked false

  let try_lock t = Atomic.compare_and_set t.locked false true
end

module TicketLock : LOCK = struct
  type t = {
    next_ticket: int Atomic.t;
    now_serving: int Atomic.t;
  }

  let create () = {
    next_ticket = Atomic.make 0;
    now_serving = Atomic.make 0;
  }

  let lock t =
    let my_ticket = Atomic.fetch_and_add t.next_ticket 1 in
    while Atomic.get t.now_serving <> my_ticket do
      Thread.yield ()
    done

  let unlock t = Atomic.incr t.now_serving

  let try_lock t =
    let my_ticket = Atomic.get t.next_ticket in
    if my_ticket = Atomic.get t.now_serving then
      if Atomic.compare_and_set t.next_ticket my_ticket (my_ticket + 1) then
        true
      else
        false
    else
      false
end

module RWMutex : RW_LOCK = struct
  type t = {
    readers: int Atomic.t;
    writer: SpinLock.t;
  }

  let create () = { 
    readers = Atomic.make 0; 
    writer = SpinLock.create () 
  }

  let lock_shared t =
    while true do
      Atomic.incr t.readers;
      if not (SpinLock.try_lock t.writer) then
        Atomic.decr t.readers
      else
        (SpinLock.unlock t.writer; break)
    done

  let unlock_shared t = Atomic.decr t.readers

  let lock t =
    SpinLock.lock t.writer;
    while Atomic.get t.readers > 0 do Thread.yield () done

  let unlock t = SpinLock.unlock t.writer

  let try_lock_shared t =
    Atomic.incr t.readers;
    if SpinLock.try_lock t.writer then
      (SpinLock.unlock t.writer; true)
    else
      (Atomic.decr t.readers; false)

  let try_lock t =
    if SpinLock.try_lock t.writer then
      if Atomic.get t.readers = 0 then true
      else (SpinLock.unlock t.writer; false)
    else
      false
end

module ParkingLot = struct
  module ParkingSpace = struct
    type t = {
      mutex: Mutex.t;
      condition: Condition.t;
      mutable waiting: int;
    }

    let create () = {
      mutex = Mutex.create ();
      condition = Condition.create ();
      waiting = 0;
    }
  end

  type t = {
    spaces: (int, ParkingSpace.t) Hashtbl.t;
    spaces_mutex: Mutex.t;
  }

  let create () = {
    spaces = Hashtbl.create 512;
    spaces_mutex = Mutex.create ();
  }

  let get_parking_space t lock_addr =
    Mutex.lock t.spaces_mutex;
    let space =
      try Hashtbl.find t.spaces lock_addr
      with Not_found ->
        let space = ParkingSpace.create () in
        Hashtbl.add t.spaces lock_addr space;
        space
    in
    Mutex.unlock t.spaces_mutex;
    space

  let park t lock_addr callback timeout =
    let space = get_parking_space t lock_addr in
    Mutex.lock space.mutex;
    space.waiting <- space.waiting + 1;

    let rec wait_loop () =
      if not (callback ()) then
        match timeout with
        | None -> 
            Condition.wait space.condition space.mutex;
            wait_loop ()
        | Some timeout_ms ->
            let _ = Condition.wait_timed space.condition space.mutex (float_of_int timeout_ms /. 1000.0) in
            wait_loop ()
    in
    wait_loop ();

    space.waiting <- space.waiting - 1;
    Mutex.unlock space.mutex

  let unpark t lock_addr =
    let space = get_parking_space t lock_addr in
    Mutex.lock space.mutex;
    Condition.broadcast space.condition;
    Mutex.unlock space.mutex
end

module HybridLock : RW_LOCK = struct
  type t = {
    rw_lock: RWMutex.t;
    mutable version: int;
    parking_lot: ParkingLot.t;
    lock_addr: int;
  }

  let create () = {
    rw_lock = RWMutex.create ();
    version = 0;
    parking_lot = ParkingLot.create ();
    lock_addr = Oo.id (object end);
  }

  let lock_shared t =
    if not (RWMutex.try_lock_shared t.rw_lock) then
      ParkingLot.park t.parking_lot t.lock_addr (fun () -> RWMutex.try_lock_shared t.rw_lock) None

  let unlock_shared t = RWMutex.unlock_shared t.rw_lock

  let lock t =
    if not (RWMutex.try_lock t.rw_lock) then
      ParkingLot.park t.parking_lot t.lock_addr (fun () -> RWMutex.try_lock t.rw_lock) None

  let unlock t =
    t.version <- t.version + 1;
    RWMutex.unlock t.rw_lock;
    ParkingLot.unpark t.parking_lot t.lock_addr

  let try_lock_shared = RWMutex.try_lock_shared
  let try_lock = RWMutex.try_lock

  let read_optimistically t read_callback =
    let pre_version = t.version in
    let result = read_callback () in
    if pre_version = t.version then Some result else None

  let read_pessimistically t read_callback =
    lock_shared t;
    let result = read_callback () in
    unlock_shared t;
    result

  let read_with_fallback t read_callback =
    match read_optimistically t read_callback with
    | Some result -> result
    | None -> read_pessimistically t read_callback
end