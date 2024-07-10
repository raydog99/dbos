open Core
open Schema

module type ValueType = sig
  type t
  val deserialize_from : string -> Type.t -> bool -> t
  val serialize_to : t -> string -> bool -> AbstractPool.t option -> unit
  val cast_as : t -> Type.t -> t
  val get_type_id : t -> Type.t
  val get_length : t -> int
  val is_null : t -> bool
  val compare_not_equals : t -> t -> CmpBool.t
  val compare_greater_than : t -> t -> CmpBool.t
  val compare_less_than : t -> t -> CmpBool.t
  val hash_combine : t -> int -> int
  val to_string : t -> string
end

module type AbstractPoolType = sig
  type t
end

module Make
    (Schema : SchemaType)
    (Value : ValueType)
    (AbstractPool : AbstractPoolType) = struct

  type t = {
    mutable tuple_schema: Schema.t;
    mutable tuple_data: string;
    mutable allocated: bool;
  }

  let create schema =
    let tuple_data = String.make (Schema.get_length schema 0) '\000' in
    { tuple_schema = schema; tuple_data; allocated = false }

  let get_value t column_id =
    assert (t.tuple_schema <> None);
    assert (t.tuple_data <> "");
    let column_type = Schema.get_type t.tuple_schema column_id in
    let data_ptr = String.sub t.tuple_data (Schema.get_offset t.tuple_schema column_id) 
                                           (Schema.get_length t.tuple_schema column_id) in
    let is_inlined = Schema.is_inlined t.tuple_schema column_id in
    Value.deserialize_from data_ptr column_type is_inlined

  let set_value t column_offset value data_pool =
    let type_ = Schema.get_type t.tuple_schema column_offset in
    let is_inlined = Schema.is_inlined t.tuple_schema column_offset in
    let value_location = String.sub t.tuple_data (Schema.get_offset t.tuple_schema column_offset) 
                                                 (Schema.get_length t.tuple_schema column_offset) in
    let column_length = Schema.get_length t.tuple_schema column_offset in

    if Type.equal type_ (Value.get_type_id value) then
      if (Type.equal type_ Type.Varchar || Type.equal type_ Type.Varbinary)
         && (column_length <> 0 && Value.get_length value <> Type.peloton_value_null)
         && Value.get_length value > column_length + 1 then
        raise (ValueOutOfRangeException (type_, column_length))
      else
        Value.serialize_to value value_location is_inlined data_pool
    else
      let casted_value = Value.cast_as value type_ in
      Value.serialize_to casted_value value_location is_inlined data_pool

  let set_from_tuple t tuple columns pool =
    List.iteri columns ~f:(fun this_col_itr col ->
      let fetched_value = tuple#get_value col in
      set_value t this_col_itr fetched_value pool
    )

  let copy t source pool =
    let is_inlined = Schema.is_inlined t.tuple_schema in
    let uninlineable_column_count = Schema.get_uninlined_column_count t.tuple_schema in

    if is_inlined then
      String.blit ~src:source ~src_pos:0 ~dst:t.tuple_data ~dst_pos:0 
                  ~len:(Schema.get_length t.tuple_schema 0)
    else begin
      String.blit ~src:source ~src_pos:0 ~dst:t.tuple_data ~dst_pos:0 
                  ~len:(Schema.get_length t.tuple_schema 0);

      for column_itr = 0 to uninlineable_column_count - 1 do
        let unlineable_column_id = Schema.get_uninlined_column t.tuple_schema column_itr in
        let value = get_value t unlineable_column_id in
        set_value t unlineable_column_id value pool
      done
    end

  let export_serialization_size t =
    let bytes = ref 0 in
    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      match Schema.get_type t.tuple_schema column_itr with
      | Type.Tinyint | Type.Smallint | Type.Integer | Type.Bigint 
      | Type.Timestamp | Type.Decimal ->
          bytes := !bytes + 8 
      | Type.Varchar | Type.Varbinary ->
          if not (Value.is_null (get_value t column_itr)) then
            bytes := !bytes + 4 + Value.get_length (get_value t column_itr)
      | _ ->
          raise (UnknownTypeException (Schema.get_type t.tuple_schema column_itr, 
                                       "Unknown ValueType found during Export serialization."))
    done;
    !bytes

  let get_uninlined_memory_size t =
    let bytes = ref 0 in
    let column_count = Schema.get_column_count t.tuple_schema in

    if not (Schema.is_inlined t.tuple_schema) then
      for column_itr = 0 to column_count - 1 do
        if (Type.equal (Schema.get_type t.tuple_schema column_itr) Type.Varchar ||
            Type.equal (Schema.get_type t.tuple_schema column_itr) Type.Varbinary) &&
           not (Schema.is_inlined t.tuple_schema column_itr) then
          if not (Value.is_null (get_value t column_itr)) then
            bytes := !bytes + 4 + Value.get_length (get_value t column_itr)
      done;
    !bytes

  let serialize_with_header_to t output =
    assert (t.tuple_schema <> None);
    assert (t.tuple_data <> "");

    let start = SerializeOutput.position output in
    SerializeOutput.write_int output 0;  

    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      let value = get_value t column_itr in
      Value.serialize_to value output
    done;

    let serialized_size = SerializeOutput.position output - start - 4 in
    SerializeOutput.write_int_at output start serialized_size

  let serialize_to t output =
    assert (t.tuple_schema <> None);
    let start = SerializeOutput.reserve_bytes output 4 in
    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      let value = get_value t column_itr in
      Value.serialize_to value output
    done;

    SerializeOutput.write_int_at output start (SerializeOutput.position output - start - 4)

  let serialize_to_export t output col_offset null_array =
    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      if Value.is_null (get_value t column_itr) then
        let byte = (col_offset + column_itr) lsr 3 in
        let bit = (col_offset + column_itr) mod 8 in
        let mask = 0x80 lsr bit in
        null_array.(byte) <- Char.chr ((Char.code null_array.(byte)) lor mask)
      else
        let val_ = get_value t column_itr in
        Value.serialize_to val_ output
    done

  let equals_no_schema_check t other =
    let column_count = Schema.get_column_count t.tuple_schema in

    let rec check_columns column_itr =
      if column_itr = column_count then true
      else
        let lhs = get_value t column_itr in
        let rhs = other#get_value column_itr in
        if Value.compare_not_equals lhs rhs = CmpBool.True then false
        else check_columns (column_itr + 1)
    in
    check_columns 0

  let equals_no_schema_check_columns t other columns =
    List.for_all columns ~f:(fun column_itr ->
      let lhs = get_value t column_itr in
      let rhs = other#get_value column_itr in
      Value.compare_not_equals lhs rhs <> CmpBool.True
    )

  let set_all_nulls t =
    assert (t.tuple_schema <> None);
    assert (t.tuple_data <> "");
    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      let value = Value.get_null_value_by_type (Schema.get_type t.tuple_schema column_itr) in
      set_value t column_itr value None
    done

  let set_all_zeros t =
    assert (t.tuple_schema <> None);
    assert (t.tuple_data <> "");
    let column_count = Schema.get_column_count t.tuple_schema in

    for column_itr = 0 to column_count - 1 do
      let value = Value.get_zero_value_by_type (Schema.get_type t.tuple_schema column_itr) in
      set_value t column_itr value None
    done

  let compare t other =
    let column_count = Schema.get_column_count t.tuple_schema in

    let rec compare_columns column_itr =
      if column_itr = column_count then 0
      else
        let lhs = get_value t column_itr in
        let rhs = other#get_value column_itr in
        if Value.compare_greater_than lhs rhs = CmpBool.True then 1
        else if Value.compare_less_than lhs rhs = CmpBool.True then -1
        else compare_columns (column_itr + 1)
    in
    compare_columns 0

  let compare_columns t other columns =
    let rec compare_cols = function
      | [] -> 0
      | column_itr :: rest ->
          let lhs = get_value t column_itr in
          let rhs = other#get_value column_itr in
          if Value.compare_greater_than lhs rhs = CmpBool.True then 1
          else if Value.compare_less_than lhs rhs = CmpBool.True then -1
          else compare_cols rest
    in
    compare_cols columns

  let hash_code t seed =
    let column_count = Schema.get_column_count t.tuple_schema in
    let rec hash_columns column_itr acc =
      if column_itr = column_count then acc
      else
        let value = get_value t column_itr in
        let new_acc = Value.hash_combine value acc in
        hash_columns (column_itr + 1) new_acc
    in
    hash_columns 0 seed

  let move_to_tuple t address =
    assert (t.tuple_schema <> None);
    t.tuple_data <- address

  let hash_code_no_seed t =
    hash_code t 0

  let get_data_ptr t column_id =
    assert (t.tuple_schema <> None);
    assert (t.tuple_data <> "");
    String.sub t.tuple_data (Schema.get_offset t.tuple_schema column_id) 
                            (Schema.get_length t.tuple_schema column_id)

  let get_info t =
    let column_count = Schema.get_column_count t.tuple_schema in
    let buf = Buffer.create 256 in
    Buffer.add_string buf "(";
    for column_itr = 0 to column_count - 1 do
      if column_itr > 0 then Buffer.add_string buf ", ";
      if Value.is_null (get_value t column_itr) then
        Buffer.add_string buf "<NULL>"
      else
        let val_ = get_value t column_itr in
        Buffer.add_string buf (Value.to_string val_)
    done;
    Buffer.add_string buf ")";
    Buffer.contents buf

end