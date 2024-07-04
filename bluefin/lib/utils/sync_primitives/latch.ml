(* Constants *)
let latch_exclusive_bit = 1L
let latch_version_mask = Int64.max_int

(* Enums *)
type guard_state =
  | Uninitialized
  | Optimistic
  | Shared
  | Exclusive
  | Moved

type latch_fallback_mode =
  | Shared
  | Exclusive
  | Jump
  | Spin
  | ShouldNotHappen

(* Exception *)
exception RestartException

(* HybridLatch module *)
module HybridLatch = struct
  type t = {
    mutable version: int64;
    mutex: Mutex.t;
  }

  let create initial_version =
    { version = initial_version; mutex = Mutex.create () }

  let is_exclusively_latched t =
    Int64.logand t.version latch_exclusive_bit = latch_exclusive_bit

  let assert_exclusively_latched t =
    assert (is_exclusively_latched t)

  let assert_not_exclusively_latched t =
    assert (not (is_exclusively_latched t))
end

(* Guard module *)
module Guard = struct
  type t = {
    mutable latch: HybridLatch.t;
    mutable state: guard_state;
    mutable version: int64;
    mutable faced_contention: bool;
  }

  let create latch =
    { latch; state = Uninitialized; version = 0L; faced_contention = false }

  let create_with_snapshot latch last_seen_version =
    { latch; state = Optimistic; version = last_seen_version; faced_contention = false }

  let create_with_state latch state =
    { latch; state; version = latch.version; faced_contention = false }

  let recheck t =
    match t.state with
    | Optimistic when t.version <> t.latch.version -> raise RestartException
    | _ -> ()

  let unlock t =
    match t.state with
    | Exclusive ->
        t.version <- Int64.add t.version latch_exclusive_bit;
        t.latch.version <- t.version;
        Mutex.unlock t.latch.mutex;
        t.state <- Optimistic
    | Shared ->
        Mutex.unlock t.latch.mutex;
        t.state <- Optimistic
    | _ -> ()

  let to_optimistic_spin t =
    assert (t.state = Uninitialized);
    t.version <- t.latch.version;
    if HybridLatch.is_exclusively_latched t.latch then begin
      t.faced_contention <- true;
      while HybridLatch.is_exclusively_latched t.latch do
        t.version <- t.latch.version
      done
    end;
    t.state <- Optimistic

  let to_optimistic_or_jump t =
    assert (t.state = Uninitialized);
    t.version <- t.latch.version;
    if HybridLatch.is_exclusively_latched t.latch then
      raise RestartException
    else
      t.state <- Optimistic

  let to_optimistic_or_shared t =
    assert (t.state = Uninitialized);
    t.version <- t.latch.version;
    if HybridLatch.is_exclusively_latched t.latch then begin
      Mutex.lock t.latch.mutex;
      t.version <- t.latch.version;
      t.state <- Shared;
      t.faced_contention <- true
    end else
      t.state <- Optimistic

  let to_optimistic_or_exclusive t =
    assert (t.state = Uninitialized);
    t.version <- t.latch.version;
    if HybridLatch.is_exclusively_latched t.latch then begin
      Mutex.lock t.latch.mutex;
      t.version <- Int64.add t.latch.version latch_exclusive_bit;
      t.latch.version <- t.version;
      t.state <- Exclusive;
      t.faced_contention <- true
    end else
      t.state <- Optimistic

  let to_exclusive t =
    match t.state with
    | Exclusive -> ()
    | Optimistic ->
        let new_version = Int64.add t.version latch_exclusive_bit in
        Mutex.lock t.latch.mutex;
        if t.latch.version <> t.version then begin
          Mutex.unlock t.latch.mutex;
          raise RestartException
        end;
        t.latch.version <- new_version;
        t.version <- new_version;
        t.state <- Exclusive
    | _ ->
        Mutex.lock t.latch.mutex;
        t.version <- Int64.add t.latch.version latch_exclusive_bit;
        t.latch.version <- t.version;
        t.state <- Exclusive

  let to_shared t =
    match t.state with
    | Shared -> ()
    | Optimistic ->
        Mutex.lock t.latch.mutex;
        if t.latch.version <> t.version then begin
          Mutex.unlock t.latch.mutex;
          raise RestartException
        end;
        t.state <- Shared
    | _ -> failwith "Unreachable"

  let try_to_exclusive t =
    assert (t.state = Optimistic);
    let new_version = Int64.add t.version latch_exclusive_bit in
    if not (Mutex.try_lock t.latch.mutex) then
      raise RestartException;
    if t.latch.version <> t.version then begin
      Mutex.unlock t.latch.mutex;
      raise RestartException
    end;
    t.latch.version <- new_version;
    t.version <- new_version;
    t.state <- Exclusive

  let try_to_shared t =
    assert (t.state = Optimistic);
    if not (Mutex.try_lock t.latch.mutex) then
      raise RestartException;
    if t.latch.version <> t.version then begin
      Mutex.unlock t.latch.mutex;
      raise RestartException
    end;
    t.state <- Shared
end