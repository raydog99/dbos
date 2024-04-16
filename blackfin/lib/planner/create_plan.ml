open InternalTypes

module CreatePlan : CreatePlanSig = struct
  type primary_key_info = {
    primary_key_cols : string list;
    constraint_name : string;
  }

  type foreign_key_info = {
    foreign_key_sources : string list;
    foreign_key_sinks : string list;
    sink_table_name : string;
    constraint_name : string;
    upd_action : FKConstrActionType;
    del_action : FKConstrActionType;
  }

  type unique_info = {
    unique_cols : string list;
    constraint_name : string;
  }

  type check_info = {
    check_cols : string list;
    constraint_name : string;
    exp : ExpressionType * type_value;
  }

  type create_plan = {
    table_name : string;
    schema_name : string;
    database_name : string;
    table_schema : catalog_schema;
    create_type : CreateType;
    index_name : string;
    index_attrs : string list;
    index_type : IndexType;
    unique : bool;
    has_primary_key : bool;
    primary_key : primary_key_info;
    foreign_keys : foreign_key_info list;
    con_uniques : unique_info list;
    con_checks : check_info list;
  }

  let create_plan database_name create_type =
    { table_name = ""; schema_name = ""; database_name; table_schema = catalog_schema_dummy;
      create_type; index_name = ""; index_attrs = []; index_type = IndexType_Dummy;
      unique = false; has_primary_key = false;
      primary_key = { primary_key_cols = []; constraint_name = "" };
      foreign_keys = []; con_uniques = []; con_checks = [] }

  let copy_plan table_name schema_name database_name schema create_type =
    { table_name; schema_name; database_name; table_schema = schema; create_type;
      index_name = ""; index_attrs = []; index_type = IndexType_Dummy; unique = false;
      has_primary_key = false; primary_key = { primary_key_cols = []; constraint_name = "" };
      foreign_keys = []; con_uniques = []; con_checks = [] }
end