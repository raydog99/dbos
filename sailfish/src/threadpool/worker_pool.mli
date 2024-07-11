type task = unit -> unit

module TaskQueue : sig
  type t
  val create : unit -> t
  val enqueue : t -> task -> unit
  val dequeue : t -> task option
  val is_empty : t -> bool
end

type t

val create : string -> int -> t
val startup : t -> unit
val shutdown : t -> unit
val submit : t -> task -> unit