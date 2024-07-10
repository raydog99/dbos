open Core

type backend_type =
  | MM
  | NVM
  | SSD
  | HDD
  | INVALID

val peloton_data_file_size : int ref

type t = {
  mutable data_file_address: char option;
  mutable data_file_len: int;
  mutable data_file_offset: int;
  mutable allocation_count: int;
  data_file_spinlock: Mutex.t;
}

val create : unit -> t

val instance : t Lazy.t

val get_instance : unit -> t

val allocate : t -> backend_type -> int -> bytes

val release : t -> backend_type -> bytes -> unit

val sync : t -> backend_type -> bytes -> int -> unit
