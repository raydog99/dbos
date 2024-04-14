module CounterMetric : sig
  type t

  val make : unit -> t

  val increment : t -> unit
  val increment_by : t -> int64 -> unit

  val decrement : t -> unit
  val decrement_by : t -> int64 -> unit

  val reset : t -> unit
  val get_counter : t -> int64

  val equals : t -> t -> bool
  val not_equals : t -> t -> bool

  val aggregate : t -> abstractMetric -> unit
  val get_info : t -> string
end