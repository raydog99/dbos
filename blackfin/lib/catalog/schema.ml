module Schema : sig
  type column = {
    column_name : string;
    column_type : TypeId.t;
    fixed_length : int;
    variable_length : int;
    is_inlined : bool;
    is_not_null : bool;
    has_default : bool;
    default_value : TypeId.value option;
    column_offset : int;
  }

  type constraint_type =
    | PRIMARY
    | UNIQUE
    | FOREIGN

  type constraint_info = {
    constraint_oid : oid_t;
    constraint_type : constraint_type;
    column_ids : oid_t list;
  }
end = struct
  type column = {
    column_name : string;
    column_type : TypeId.t;
    fixed_length : int;
    variable_length : int;
    is_inlined : bool;
    is_not_null : bool;
    has_default : bool;
    default_value : TypeId.value option;
    column_offset : int;
  }

  type constraint_type =
    | PRIMARY
    | UNIQUE
    | FOREIGN

  type constraint_info = {
    constraint_oid : oid_t;
    constraint_type : constraint_type;
    column_ids : oid_t list;
  }

  type t = {
    length : int;
    columns : column list;
    column_count : int;
    uninlined_columns : oid_t list;
    uninlined_column_count : oid_t;
    tuple_is_inlined : bool;
    indexed_columns : oid_t list;
    constraints : (oid_t, constraint_info) Hashtbl.t;
    not_null_columns : oid_t list;
    primary_key_columns : oid_t list;
    unique_constraint_count : oid_t;
    fk_constraints : oid_t list;
    fk_sources : constraint_info list;
  }
end