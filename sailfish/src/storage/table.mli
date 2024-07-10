open Core

module type S = sig
  val invalid_oid : int

  val get_data_table :
    int -> int -> Schema.t -> string -> int -> bool ->
    bool -> bool -> SchemaType.layout_type -> DataTable.t

  val get_temp_table :
    Schema.t -> bool -> TempTable.t

  val drop_data_table :
    int -> int -> bool
end
