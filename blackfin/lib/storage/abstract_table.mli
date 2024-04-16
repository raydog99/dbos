module type AbstractTable = sig
  type t = {
    table_oid : oid_t;
    schema : catalog_schema;
    own_schema : bool;
    default_layout : layout option;
  }

  val create : oid_t -> catalog_schema -> bool -> layout_type -> t
end