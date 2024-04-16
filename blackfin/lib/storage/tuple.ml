open Schema

module Tuple : Tuple = struct
  type tuple = {
    tuple_schema : Schema;
    tuple_data : char array;
    allocated : bool;
  }

  type schema = Schema

  let create schema =
    { tuple_schema = schema; tuple_data = [||]; allocated = false }

  let create_with_data schema data =
    { tuple_schema = schema; tuple_data = data; allocated = false }

  let copy tuple =
    { tuple = tuple with allocated = false }

  let move tuple data =
    tuple.tuple_data <- data

  let equal lhs rhs =
    if lhs.tuple_schema <> rhs.tuple_schema then false
    else equals_no_schema_check lhs rhs

  let not_equal lhs rhs =
    not (equal lhs rhs)

  let set_value_without_pool tuple column_id value =
    set_value tuple column_id value None

  let get_length tuple =
    catalog_Schema.get_length tuple.tuple_schema

  let is_null tuple column_id =
    let value = get_value tuple column_id in
    type_Value.is_null value

  let is_tuple_null tuple =
    tuple.tuple_data = [||]

  let get_type tuple column_id =
    catalog_Schema.get_type tuple.tuple_schema column_id

  let get_schema tuple =
    tuple.tuple_schema
end