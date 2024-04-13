module Schema
module TileGroup

module Tile = struct
	type t = {
		database_id : int;
		table_id : int;
		table_group_id : int;
		tile_id : int;
		schema : Schema.t;
		tile_group : TileGroup.t;
		num_tuple_slots : int;
		column_count : int;
		data : bytes;
		column_count : int;
		tuple_length : int;
		tile_size : int;
		column_header : string;
		column_header_size : string;
	}

	let create ~tile_header ~schema ~tile_group ~tile_count 
		~tile ~database_id ~table_id ~tile_group_id ~tile_id ~schema =
		let tile_size = tile_count * tuple_length in
		let tuple_length = Schema.GetLength schema in
	    let data = Bytes.make tile_size '\000'
	    let column_count = Schema.GetColumnCount schema in
		{
			tile_header;
			schema;
			tile_size;
			tuple_length;
			column_count;
			tile_group;
			tile_count;
			tile;
			database_id;
			table_id;
			tile_group_id;
			tile_id;
			schema;
	    }

	let insert_tuple ~tuple_offset ~tuple = 
		assert(tuple_offset < get_allocated_tuple_count t);

		let location = tuple_offset * t.tuple_length in
		Bytes.blit tuple.tuple_data 0 t.data_location t.tuple_length

	let get_allocated_tuple_count () = t.num_tuple_slots

	let get_active_tuple_count () = 0

	let get_tuple_offset tuple_address = 
		if tuple_address < t.data || tuple_address >= t.data + t.tile_size then
			-1
		else
		let tuple_id = (tuple_address - t.data) / t.tuple_length in
		if tuple_id * t.tuple_length + t.data = tuple_address then
			tuple_id
	    else
	    	-1

	let get_column_offset name = 0

	let get_value tuple_offset column_id = 
		assert (tuple_offset < get_allocated_tuple_count);
		assert (column_id < get_column_count schema);
		let column_type = Schema.GetType column_id

	let set_value value tuple_offset column_id = 
		assert (tuple_offset < get_allocated_tuple_count t);
		assert (column_id < Schema.GetColumnCount t.schema);

		let tuple_location = GetTupleLocation tuple_offset t

	let get_tuple catalog tuple_location = 
		match t.tile_group_header with
		| Some tile_group_header ->
			TileGroupHeader.get_current_next_tuple_slot tile_group_header
		| None ->
			t.num_tuple_slots

	let get_schema () = t.schema

	let get_column_name column_index = 
		Schema.get_column column_index |> Column.get_name

	let get_column_count () = t.column_count

	let get_header () = t.tile_group_header

	let get_tile_group () = t.tile_group

	let get_tile_id () = t.tile_id
end