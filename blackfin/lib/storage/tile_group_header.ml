module TileGroupHeader = struct
  type t = {
    tile_group : tile_group option;
    num_tuple_slots : int;
    mutable next_tuple_slot : int;
    tile_header_lock : unit;
    tuple_headers : tuple_header array;
    mutable immutable : bool;
  }

  let create tuple_count =
    let tuple_headers = Array.make tuple_count {
      transaction_id = None;
      last_reader_commit_id = None;
      begin_commit_id = None;
      end_commit_id = None;
      next_item_pointer = None;
      prev_item_pointer = None;
      indirection = None;
    } in
    {
      tile_group = None;
      num_tuple_slots = tuple_count;
      next_tuple_slot = 0;
      tile_header_lock = ();
      tuple_headers;
      immutable = false;
    }

  let get_active_tuple_count t =
    let active_tuple_slots = ref 0 in
    for tuple_slot_id = 0 to t.num_tuple_slots - 1 do
      let tuple_txn_id = t.tuple_headers.(tuple_slot_id).transaction_id in
      match tuple_txn_id with
      | Some txn_id ->
          if txn_id = 1 then
            active_tuple_slots := !active_tuple_slots + 1
          else
            assert false
      | None -> ()
    done;
    !active_tuple_slots
end