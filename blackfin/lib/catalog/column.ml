module type ColumnType : sig
	val column_name : string;
    val column_type : TypeId.t;
    val fixed_length : int;
    val variable_length : int;
    val is_inlined : bool;
    val is_not_null : bool;
    val has_default : bool;
    val default_value : TypeId.value option;
    val column_offset : int;
end

module type C : sig
	val get_offset : column -> int
	val get_name : column -> string
	val get_length : column -> int
	val get_fixed_length : column -> int
	val get_variable_length : column -> int
	val get_type : column -> TypeId
	val is_inlined : unit -> bool
	val has_default : unit -> bool
end

module Make(Column: ColumnType) : C = struct
	let column_name = Column.column_name
	let column_type = Column.column_type
	let fixed_length = Column.fixed_length
	let variable_length = Column.variable_length
	let is_not_null = Column.is_not_null
	let has_default = Column.has_default
	let column_offset = Column.column_offset

	let get_offset column = column.column_offset
	let get_name column = column.column_name
	let get_length column =
		if column.is_inlined then column.fixed_length else column.variable_length
	let get_fixed_length column = column.fixed_length
	let get_variable_length column = column.variable_length
	let get_type column = column.column_type
	let is_not_null column = column.is_not_null
	let has_default column = column.has_default

	let hash column =
		let type_hash = TypeId.hash column.column_type in
    	let inline_hash = Hashtbl.hash column.is_inlined in
    	type_hash lxor inline_hash

    let equal column1 column2 =
    	column1.column_type = column2.column_type && column1.is_inlined = column2.is_inlined

    let get_info column =
    	Printf.sprintf "Name: %s, Type: %s, Length: %d, Offset: %d, Inlined: %b"
    	column.column_name
    	(TypeId.to_string column.column_type)
    	(get_length column)
    	column.column_offset
    	column.is_inlined
end