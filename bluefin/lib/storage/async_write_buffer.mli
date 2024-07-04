open Lwt.Infix

type t

val create : Unix.file_descr -> int -> int -> t

val is_full : t -> bool

val add : t -> BufferFrame.t -> int64 -> unit

val submit : t -> int Lwt.t

val poll_events_sync : t -> int Lwt.t

val get_written_bfs : t -> (BufferFrame.t -> int64 -> int64 -> unit) -> int -> unit Lwt.t

val debug_info : t -> string