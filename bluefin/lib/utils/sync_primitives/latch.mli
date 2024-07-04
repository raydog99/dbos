(* Constants *)
val latch_exclusive_bit : int64
val latch_version_mask : int64

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
module HybridLatch : sig
  type t = {
    mutable version: int64;
    mutex: Mutex.t;
  }

  val create : int64 -> t
  val is_exclusively_latched : t -> bool
  val assert_exclusively_latched : t -> unit
  val assert_not_exclusively_latched : t -> unit
end

(* Guard module *)
module Guard : sig
  type t = {
    mutable latch: HybridLatch.t;
    mutable state: guard_state;
    mutable version: int64;
    mutable faced_contention: bool;
  }

  val create : HybridLatch.t -> t
  val create_with_snapshot : HybridLatch.t -> int64 -> t
  val create_with_state : HybridLatch.t -> guard_state -> t
  val recheck : t -> unit
  val unlock : t -> unit
  val to_optimistic_spin : t -> unit
  val to_optimistic_or_jump : t -> unit
  val to_optimistic_or_shared : t -> unit
  val to_optimistic_or_exclusive : t -> unit
  val to_exclusive : t -> unit
  val to_shared : t -> unit
  val try_to_exclusive : t -> unit
  val try_to_shared : t -> unit
end