open Printf
open Schema

module AbstractTable = struct
  type t = {
    table_oid : oid_t;
    schema : catalog_schema;
    own_schema : bool;
    default_layout : layout option;
  }

  let create table_oid schema own_schema layout_type =
    match layout_type with
    | LayoutType.ROW | LayoutType.COLUMN ->
      Layout.create (Schema.getColumnCount schema) layout_type
    | _ ->
      failwith "Invalid layout type" in
  { table_oid; schema; own_schema; default_layout }
end