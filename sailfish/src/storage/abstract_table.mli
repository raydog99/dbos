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

val create : 
  int -> 
  Schema.t -> 
  own_schema:bool -> 
  LayoutType.t -> 
  t

val get_tile_group_with_layout : 
  t -> 
  int -> 
  int -> 
  Layout.t -> 
  int -> 
  TileGroup.t

val get_info : 
  t -> 
  string
