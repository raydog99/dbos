open InternalTypes

module type CreatePlan = sig
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

  type create_plan

  val create_plan : string -> CreateType -> create_plan
  val copy_plan : string -> string -> string -> catalog_schema -> CreateType -> create_plan
  val parse_plan : parser_create_statement -> create_plan

  val get_plan_node_type : create_plan -> PlanNodeType
  val get_info : create_plan -> string
  val copy : create_plan -> create_plan

  val get_index_name : create_plan -> string
  val get_table_name : create_plan -> string
  val get_schema_name : create_plan -> string
  val get_database_name : create_plan -> string
  val get_schema : create_plan -> catalog_schema
end