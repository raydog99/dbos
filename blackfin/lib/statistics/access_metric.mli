module AccessMetric : sig
  type t

  val make : unit -> t

  val increment_reads : t -> unit
  val increment_updates : t -> unit
  val increment_inserts : t -> unit
  val increment_deletes : t -> unit

  val increment_reads_by : t -> int64 -> unit
  val increment_updates_by : t -> int64 -> unit
  val increment_inserts_by : t -> int64 -> unit
  val increment_deletes_by : t -> int64 -> unit

  val get_reads : t -> int64
  val get_updates : t -> int64
  val get_inserts : t -> int64
  val get_deletes : t -> int64

  val get_access_counter : t -> int -> CounterMetric.t

  val reset : t -> unit
  val get_info : t -> string
  val aggregate : t -> abstractMetric -> unit
end