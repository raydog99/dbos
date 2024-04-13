module TileGroup = struct
  type backend_type = 
    | InMemory
    | Disk

  type t = {
    database_id : int;
    table_id : int;
    tile_group_id : int;
    backend_type : backend_type;
    tiles : tile list;
    tile_group_header : tile_group_header;
    table : abstract_table;
    num_tuple_slots : int;
    tile_count : int;
    tile_group_layout : layout option;
  }

  let create_tile_group ~backend_type ~tile_group_header ~table ~schemas ~layout ~tuple_count ~database_id ~table_id ~tile_group_id =
    let tile_count = List.length schemas in
    {
      database_id;
      table_id;
      tile_group_id;
      backend_type;
      tile_group_header;
      table;
      num_tuple_slots = tuple_count;
      tile_count;
      tile_group_layout;
    }
end