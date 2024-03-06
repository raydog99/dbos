type physical_tile = {
    tuple_offsets : int list;
    attributes : (string * int list) list;
}

type tile_group = physical_tile list

let create_physical_tile tuple_offsets attributes =
    { tuple_offsets; attributes }

let create_tile_group tiles = tiles

type logical_tile = {
    columns : (int list * (string * (string * int list) list) list) list;
}

let create_logical_tile columns =
    { columns }