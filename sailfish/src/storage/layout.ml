open Core

type layout_type =
  | Row
  | Column
  | Hybrid

type column_map_entry = int * int
type column_map_type = (int, column_map_entry) Hashtbl.t

type t = {
  mutable layout_oid: int;
  mutable num_columns: int;
  mutable layout_type: layout_type;
  mutable column_layout: column_map_type;
}

let row_store_layout_oid = 0
let column_store_layout_oid = 1 
let invalid_oid = -1 

let create_with_column_count num_columns layout_type =
  let layout_oid = 
    match layout_type with
    | Row -> row_store_layout_oid
    | Column -> column_store_layout_oid
    | Hybrid -> invalid_oid
  in
  { layout_oid;
    num_columns;
    layout_type;
    column_layout = Hashtbl.create (module Int) }

let create_with_column_map column_map =
  let num_columns = Hashtbl.length column_map in
  let layout_type, layout_oid =
    let row_layout = ref true in
    let column_layout = ref true in
    Hashtbl.iteri column_map ~f:(fun ~key:column_id ~data:(tile_id, _) ->
      if tile_id <> 0 then row_layout := false;
      if tile_id <> column_id then column_layout := false
    );
    if !row_layout then
      (Row, row_store_layout_oid)
    else if !column_layout then
      (Column, column_store_layout_oid)
    else
      (Hybrid, invalid_oid)
  in
  let t = { layout_oid; num_columns; layout_type; column_layout = column_map } in
  if layout_type <> Hybrid then
    Hashtbl.clear t.column_layout;
  t

let create_with_predefined_layout column_map num_columns layout_id =
  let layout_type =
    if layout_id = row_store_layout_oid then Row
    else if layout_id = column_store_layout_oid then Column
    else Hybrid
  in
  let t = { layout_oid = layout_id; num_columns; layout_type; column_layout = column_map } in
  if layout_type <> Hybrid then
    Hashtbl.clear t.column_layout;
  t

let locate_tile_and_column t column_id =
  assert (t.num_columns > column_id);
  match t.layout_type with
  | Row -> (0, column_id)
  | Column -> (column_id, 0)
  | Hybrid ->
    match Hashtbl.find t.column_layout column_id with
    | Some entry -> entry
    | None -> failwith "Column not found in layout"

let get_layout_difference t other =
  assert (t.num_columns = other.num_columns);
  if t.layout_oid <> other.layout_oid then
    let diff = ref 0 in
    for col_itr = 0 to t.num_columns - 1 do
      let (tile_id1, _) = locate_tile_and_column t col_itr in
      let (tile_id2, _) = locate_tile_and_column other col_itr in
      if tile_id1 <> tile_id2 then
        incr diff
    done;
    Float.of_int !diff /. Float.of_int t.num_columns
  else
    0.0

let get_tile_id_from_column_id t column_id =
  let (tile_id, _) = locate_tile_and_column t column_id in
  tile_id

let get_tile_map t =
  let tile_map = Hashtbl.create (module Int) in
  match t.layout_type with
  | Row ->
    let columns = List.init t.num_columns ~f:(fun i -> (i, i)) in
    Hashtbl.set tile_map ~key:0 ~data:columns
  | Column ->
    for column_id = 0 to t.num_columns - 1 do
      Hashtbl.set tile_map ~key:column_id ~data:[(column_id, 0)]
    done
  | Hybrid ->
    Hashtbl.iteri t.column_layout ~f:(fun ~key:column_id ~data:(tile_id, column_offset) ->
      Hashtbl.update tile_map tile_id ~f:(function
        | Some columns -> (column_id, column_offset) :: columns
        | None -> [(column_id, column_offset)]
      )
    );
  tile_map

let get_tile_column_offset t column_id =
  let (_, tile_column_id) = locate_tile_and_column t column_id in
  tile_column_id

let get_layout_schemas t (schema : Schema.t) : Schema.t list =
  let tile_schemas = Hashtbl.create (module Int) in
  match t.layout_type with
  | Row -> [schema]
  | Column ->
    List.init t.num_columns ~f:(fun col_id ->
      Schema.create [Schema.get_column schema col_id]
    )
  | Hybrid ->
    Hashtbl.iteri t.column_layout ~f:(fun ~key:column_id ~data:(tile_id, _) ->
      Hashtbl.update tile_schemas tile_id ~f:(function
        | Some cols -> Schema.get_column schema column_id :: cols
        | None -> [Schema.get_column schema column_id]
      )
    );
    Hashtbl.to_alist tile_schemas
    |> List.map ~f:(fun (_, columns) -> Schema.create (List.rev columns))

let get_layout_stats t =
  match t.layout_type with
  | Row -> Int.Map.singleton 0 t.num_columns
  | Column -> 
    List.init t.num_columns ~f:(fun i -> (i, 1))
    |> Int.Map.of_alist_exn
  | Hybrid ->
    Hashtbl.fold t.column_layout ~init:Int.Map.empty
      ~f:(fun ~key:_ ~data:(tile_id, _) acc ->
        Int.Map.update acc tile_id ~f:(function
          | Some count -> count + 1
          | None -> 1
        )
      )

let serialize_column_map t =
  match t.layout_type with
  | Row | Column -> ""
  | Hybrid ->
    Hashtbl.to_alist t.column_layout
    |> List.map ~f:(fun (col_id, (tile_id, tile_col_id)) ->
        sprintf "%d:%d:%d" col_id tile_id tile_col_id
      )
    |> String.concat ~sep:","

let deserialize_column_map num_columns column_map_str =
  let column_map = Hashtbl.create (module Int) in
  String.split column_map_str ~on:','
  |> List.iter ~f:(fun entry ->
      match String.split entry ~on:':' with
      | [col_id; tile_id; tile_col_id] ->
        let col_id = Int.of_string col_id in
        let tile_id = Int.of_string tile_id in
        let tile_col_id = Int.of_string tile_col_id in
        Hashtbl.set column_map ~key:col_id ~data:(tile_id, tile_col_id)
      | _ -> failwith "Invalid column map entry"
    );
  column_map

let get_column_map_info t =
  let tile_column_map = Hashtbl.create (module Int) in
  (match t.layout_type with
  | Row ->
    Hashtbl.set tile_column_map ~key:0 
      ~data:(List.init t.num_columns ~f:Fn.id)
  | Column ->
    List.init t.num_columns ~f:(fun col_id ->
      Hashtbl.set tile_column_map ~key:col_id ~data:[0]
    )
  | Hybrid ->
    Hashtbl.iteri t.column_layout ~f:(fun ~key:col_id ~data:(tile_id, _) ->
      Hashtbl.update tile_column_map tile_id ~f:(function
        | Some cols -> col_id :: cols
        | None -> [col_id]
      )
    ));
  Hashtbl.to_alist tile_column_map
  |> List.map ~f:(fun (tile_id, cols) ->
      sprintf "%d : %s" tile_id (String.concat ~sep:" " (List.map cols ~f:Int.to_string))
    )
  |> String.concat ~sep:" :: "

let layout_type_to_string = function
  | Row -> "Row"
  | Column -> "Column"
  | Hybrid -> "Hybrid"

let get_info t =
  sprintf 
    "** Layout[#%d] **\n\
     Number of columns[%d]\n\
     LayoutType[%s]\n\
     ColumnMap[ %s]"
    t.layout_oid
    t.num_columns
    (layout_type_to_string t.layout_type)
    (get_column_map_info t)

let equal t1 t2 =
  t1.layout_type = t2.layout_type &&
  t1.layout_oid = t2.layout_oid &&
  t1.num_columns = t2.num_columns &&
  (t1.layout_type <> Hybrid || Hashtbl.equal (=) t1.column_layout t2.column_layout)

let get_oid t = t.layout_oid
let get_column_count t = t.num_columns
let is_row_store t = t.layout_type = Row
let is_column_store t = t.layout_type = Column