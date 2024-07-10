open Core
open Layout
open Tile_group

type t = {
  table_oid: int;
  schema: Schema.t;
  own_schema: bool;
  default_layout: Layout.t;
  mutable tile_groups: TileGroup.t list;
}

let create table_oid schema ~own_schema layout_type =
  assert (layout_type = LayoutType.Row || layout_type = LayoutType.Column);
  let default_layout = Layout.create (Schema.get_column_count schema) layout_type in
  { table_oid; schema; own_schema; default_layout; tile_groups = [] }

let get_tile_group_with_layout t database_id tile_group_id layout num_tuples =
  let schemas = Layout.get_layout_schemas layout t.schema in
  TileGroup.create database_id t.table_oid tile_group_id t schemas layout num_tuples

let get_info t =
  let tile_group_count = List.length t.tile_groups in
  let tuple_count = List.fold t.tile_groups ~init:0 ~f:(fun acc tg ->
    acc + TileGroup.get_next_tuple_slot tg
  ) in
  let inner = String.concat ~sep:"\n" (List.map t.tile_groups ~f:TileGroup.get_info) in
  Printf.sprintf "Table '%s' [OID= %d, NumTuples=%d, NumTiles=%d]\n%s"
    (get_name t) t.table_oid tuple_count tile_group_count inner