open Thread
open Mutex
open Condition

module Atomic : sig
  include module type of Atomic
  val compare_and_set : bool Atomic.t -> bool -> bool -> bool
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

module SpinLock : LOCK

module TicketLock : LOCK

module RWMutex : RW_LOCK

module ParkingLot : sig
  type t
  val create : unit -> t
  val park : t -> int -> (unit -> bool) -> int option -> unit
  val unpark : t -> int -> unit
end

module HybridLock : sig
  include RW_LOCK
  val read_optimistically : t -> (unit -> 'a) -> 'a option
  val read_pessimistically : t -> (unit -> 'a) -> 'a
  val read_with_fallback : t -> (unit -> 'a) -> 'a
end