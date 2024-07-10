open Core
open Schema
open Tile_group_header
open Tile_group

module type TupleType = sig
  type t
  val get_data : t -> string
end

module type ValueType = sig
  type t
  val deserialize_from : string -> Type.t -> bool -> t
  val serialize_to : t -> string -> bool -> AbstractPool.t -> unit
  val cast_as : t -> Type.t -> t
end

module type AbstractPoolType = sig
  type t
end

module Make
    (Schema : SchemaType)
    (TileGroupHeader : TileGroupHeaderType)
    (TileGroup : TileGroupType)
    (Tuple : TupleType)
    (Value : ValueType)
    (AbstractPool : AbstractPoolType) = struct

  type t = {
    mutable database_id: int;
    mutable table_id: int;
    mutable tile_group_id: int;
    mutable tile_id: int;
    backend_type: BackendType.t;
    schema: Schema.t;
    mutable data: string;
    tile_group: TileGroup.t;
    pool: AbstractPool.t;
    num_tuple_slots: int;
    column_count: int;
    tuple_length: int;
    mutable uninlined_data_size: int;
    mutable column_header: string option;
    mutable column_header_size: int;
    tile_group_header: TileGroupHeader.t;
  }

  let invalid_oid = -1

  let create backend_type tile_header tuple_schema tile_group tuple_count =
    let column_count = Schema.get_column_count tuple_schema in
    let tuple_length = Schema.get_length tuple_schema in
    let tile_size = tuple_count * tuple_length in
    {
      database_id = invalid_oid;
      table_id = invalid_oid;
      tile_group_id = invalid_oid;
      tile_id = invalid_oid;
      backend_type;
      schema = tuple_schema;
      data = String.make tile_size '\000';
      tile_group;
      pool = AbstractPool.create ();
      num_tuple_slots = tuple_count;
      column_count;
      tuple_length;
      uninlined_data_size = 0;
      column_header = None;
      column_header_size = invalid_oid;
      tile_group_header = tile_header;
    }

  let get_allocated_tuple_count t = t.num_tuple_slots

  let get_tuple_location t tuple_offset =
    String.sub t.data (tuple_offset * t.tuple_length) t.tuple_length

  let insert_tuple t tuple_offset tuple =
    assert (tuple_offset < get_allocated_tuple_count t);
    let location = get_tuple_location t tuple_offset in
    String.blit ~src:(Tuple.get_data tuple) ~src_pos:0
                ~dst:t.data ~dst_pos:(tuple_offset * t.tuple_length)
                ~len:t.tuple_length

  let get_value t tuple_offset column_id =
    assert (tuple_offset < get_allocated_tuple_count t);
    assert (column_id < t.column_count);
    let column_type = Schema.get_type t.schema column_id in
    let tuple_location = get_tuple_location t tuple_offset in
    let field_location = String.sub tuple_location (Schema.get_offset t.schema column_id) 
                                    (t.tuple_length - Schema.get_offset t.schema column_id) in
    let is_inlined = Schema.is_inlined t.schema column_id in
    Value.deserialize_from field_location column_type is_inlined

  let get_value_fast t tuple_offset column_offset column_type is_inlined =
    assert (tuple_offset < get_allocated_tuple_count t);
    assert (column_offset < Schema.get_length t.schema);
    let tuple_location = get_tuple_location t tuple_offset in
    let field_location = String.sub tuple_location column_offset 
                                    (t.tuple_length - column_offset) in
    Value.deserialize_from field_location column_type is_inlined

  let set_value t value tuple_offset column_id =
    assert (tuple_offset < t.num_tuple_slots);
    assert (column_id < t.column_count);
    let tuple_location = get_tuple_location t tuple_offset in
    let field_location = String.sub tuple_location (Schema.get_offset t.schema column_id) 
                                    (t.tuple_length - Schema.get_offset t.schema column_id) in
    let is_inlined = Schema.is_inlined t.schema column_id in
    let col_type = Schema.get_type t.schema column_id in
    if Value.get_type_id value = col_type then
      Value.serialize_to value field_location is_inlined t.pool
    else
      let casted_value = Value.cast_as value col_type in
      Value.serialize_to casted_value field_location is_inlined t.pool

  let set_value_fast t value tuple_offset column_offset is_inlined column_length =
    assert (tuple_offset < t.num_tuple_slots);
    assert (column_offset < Schema.get_length t.schema);
    let tuple_location = get_tuple_location t tuple_offset in
    let field_location = String.sub tuple_location column_offset column_length in
    Value.serialize_to value field_location is_inlined t.pool

  let copy_tile t backend_type =
    let schema = t.schema in
    let tile_columns_inlined = Schema.is_inlined schema in
    let allocated_tuple_count = get_allocated_tuple_count t in

    let new_header = t.tile_group_header in
    let new_tile = TileFactory.get_tile
      backend_type invalid_oid invalid_oid invalid_oid invalid_oid
      new_header schema t.tile_group allocated_tuple_count
    in

    String.blit ~src:t.data ~src_pos:0
                ~dst:new_tile.data ~dst_pos:0
                ~len:(String.length t.data);

    if not tile_columns_inlined then
      let uninlined_col_cnt = Schema.get_uninlined_column_count schema in
      for col_itr = 0 to uninlined_col_cnt - 1 do
        let uninlined_col_offset = Schema.get_uninlined_column schema col_itr in
        for tuple_itr = 0 to allocated_tuple_count - 1 do
          let val_ = get_value new_tile tuple_itr uninlined_col_offset in
          set_value new_tile val_ tuple_itr uninlined_col_offset
        done
      done;
    new_tile

  let get_info t =
    let buf = Buffer.create 256 in
    Buffer.add_string buf (Printf.sprintf "TILE[#%d]\n" t.tile_id);
    Buffer.add_string buf (Printf.sprintf "Database[%d] // " t.database_id);
    Buffer.add_string buf (Printf.sprintf "Table[%d] // " t.table_id);
    Buffer.add_string buf (Printf.sprintf "TileGroup[%d]\n" t.tile_group_id);
    Buffer.add_string buf (String.make 80 '-' ^ "\n");

    let tuple_itr = TupleIterator.create t in
    let tuple = Tuple.create t.schema in

    let rec print_tuples count =
      match TupleIterator.next tuple_itr tuple with
      | true ->
          if count > 0 then Buffer.add_char buf '\n';
          Buffer.add_string buf (Printf.sprintf "%0*d: %s" 
            tuple_id_width count (Tuple.to_string tuple));
          print_tuples (count + 1)
      | false -> ()
    in
    print_tuples 0;

    Tuple.set_null tuple;
    let info = Buffer.contents buf in
    String.trim info

  let serialize_to t output num_tuples =
    let pos = SerializeOutput.position output in
    SerializeOutput.write_int output (-1);

    if not (serialize_header_to t output) then false
    else begin
      SerializeOutput.write_int output num_tuples;

      let written_count = ref 0 in
      let tuple_itr = TupleIterator.create t in
      let tuple = Tuple.create t.schema in

      let rec write_tuples () =
        if !written_count < num_tuples && TupleIterator.next tuple_itr tuple then begin
          Tuple.serialize_to tuple output;
          incr written_count;
          write_tuples ()
        end
      in
      write_tuples ();

      Tuple.set_null tuple;

      assert (!written_count = num_tuples);

      let sz = SerializeOutput.position output - pos - 4 in
      assert (sz > 0);
      SerializeOutput.write_int_at output pos sz;

      true
    end

  let serialize_header_to t output =
    match t.column_header with
    | Some header ->
        assert (t.column_header_size <> invalid_oid);
        SerializeOutput.write_bytes output header t.column_header_size;
        true
    | None ->
        assert (t.column_header_size = invalid_oid);

        let start = SerializeOutput.position output in
        SerializeOutput.write_int output (-1);

        SerializeOutput.write_byte output (-128);

        SerializeOutput.write_short output t.column_count;

        for column_itr = 0 to t.column_count - 1 do
          let type_ = Schema.get_type t.schema column_itr in
          SerializeOutput.write_byte output (Type.to_int type_)
        done;

        for column_itr = 0 to t.column_count - 1 do
          let name = Schema.get_column_name t.schema column_itr in
          let length = String.length name in
          assert (length >= 0);
          SerializeOutput.write_int output length;
          SerializeOutput.write_bytes output name length
        done;

        let position = SerializeOutput.position output in
        t.column_header_size <- position - start;

        let non_inclusive_header_size = t.column_header_size - 4 in
        SerializeOutput.write_int_at output start non_inclusive_header_size;

        t.column_header <- Some (SerializeOutput.get_data output start t.column_header_size);

        true

  let serialize_tuples_to t output tuples num_tuples =
    let pos = SerializeOutput.position output in
    SerializeOutput.write_int output (-1);

    assert (not (Tuple.is_null tuples.(0)));

    if not (serialize_header_to t output) then false
    else begin
      SerializeOutput.write_int output num_tuples;
      for tuple_itr = 0 to num_tuples - 1 do
        Tuple.serialize_to tuples.(tuple_itr) output
      done;

      SerializeOutput.write_int_at output pos 
        (SerializeOutput.position output - pos - 4);

      true
    end

  let deserialize_tuples_from t input pool =
    ignore (SerializeInput.read_int input);
    ignore (SerializeInput.read_byte input);

    let column_count = SerializeInput.read_short input in
    assert (column_count > 0);

    let types = Array.init column_count (fun _ -> 
      SerializeInput.read_enum_in_single_byte input
    ) in

    let names = Array.init column_count (fun _ ->
      SerializeInput.read_text_string input
    ) in

    if column_count <> Schema.get_column_count t.schema then
      let message = Printf.sprintf 
        "Column count mismatch. Expecting %d, but %d given\n\
         Expecting the following columns:\n%d\n\
         The following columns are given:\n%s"
        (Schema.get_column_count t.schema) column_count
        (Schema.get_column_count t.schema)
        (String.concat "\n" (Array.mapi (fun i name ->
          Printf.sprintf "column %d: %s, type = %d" i name (Type.to_int types.(i))
        ) names))
      in
      raise (Failure message)
    else
      deserialize_tuples_from_without_header t input pool

  let deserialize_tuples_from_without_header t input pool =
    let tuple_count = SerializeInput.read_int input in
    assert (tuple_count > 0);

    assert (tuple_count <= t.num_tuple_slots);
    let temp_tuple = Tuple.create t.schema in

    for tuple_itr = 0 to tuple_count - 1 do
      Tuple.move temp_tuple (get_tuple_location t tuple_itr);
      Tuple.deserialize_from temp_tuple input pool
    done
end