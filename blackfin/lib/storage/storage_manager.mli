module StorageManager : sig
  type t

  val create : unit -> t
  val add_table : t -> string -> Table.t -> unit
  val drop_table : t -> string -> unit
  val get_table : t -> string -> Table.t option
  val has_table : t -> string -> bool
  val table_names : t -> string list
  val tables : t -> (string * Table.t) list
  val add_view : t -> string -> LQPView.t -> unit
  val drop_view : t -> string -> unit
  val get_view : t -> string -> LQPView.t option
  val has_view : t -> string -> bool
  val view_names : t -> string list
  val views : t -> (string * LQPView.t) list
  val add_prepared_plan : t -> string -> PreparedPlan.t -> unit
  val get_prepared_plan : t -> string -> PreparedPlan.t option
  val has_prepared_plan : t -> string -> bool
  val drop_prepared_plan : t -> string -> unit
  val prepared_plans : t -> (string * PreparedPlan.t) list
  val export_all_tables_as_csv : t -> string -> unit
end