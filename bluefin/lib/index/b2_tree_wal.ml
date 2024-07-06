open B2_tree_types

type wal_log_type =
  | WALInitPage
  | WALLogicalSplit

type wal_init_page = {
  typ: wal_log_type;
  dt_id: dtid;
}

type wal_logical_split = {
  typ: wal_log_type;
  right_pid: int64;
  parent_pid: int64;
  left_pid: int64;
}

let serialize_wal_entry entry =
  match entry with
  | WALInitPage { typ; dt_id } ->
      let buffer = Bytes.create 16 in
      Bytes.set_int32_le buffer 0 (Int32.of_int (match typ with WALInitPage -> 0 | WALLogicalSplit -> 1));
      Bytes.set_int64_le buffer 4 (Int64.of_int dt_id);
      buffer
  | WALLogicalSplit { typ; right_pid; parent_pid; left_pid } ->
      let buffer = Bytes.create 32 in
      Bytes.set_int32_le buffer 0 (Int32.of_int (match typ with WALInitPage -> 0 | WALLogicalSplit -> 1));
      Bytes.set_int64_le buffer 4 right_pid;
      Bytes.set_int64_le buffer 12 parent_pid;
      Bytes.set_int64_le buffer 20 left_pid;
      buffer

let deserialize_wal_entry bytes =
  let typ = match Int32.to_int (Bytes.get_int32_le bytes 0) with
    | 0 -> WALInitPage
    | 1 -> WALLogicalSplit
    | _ -> failwith "Invalid WAL entry type"
  in
  match typ with
  | WALInitPage ->
      WALInitPage { 
        typ = WALInitPage; 
        dt_id = Int64.to_int (Bytes.get_int64_le bytes 4) 
      }
  | WALLogicalSplit ->
      WALLogicalSplit {
        typ = WALLogicalSplit;
        right_pid = Bytes.get_int64_le bytes 4;
        parent_pid = Bytes.get_int64_le bytes 12;
        left_pid = Bytes.get_int64_le bytes 20;
      }

let wal_fd = ref None

let init_wal filename =
  wal_fd := Some (open_out_bin filename)

let close_wal () =
  match !wal_fd with
  | Some fd -> close_out fd; wal_fd := None
  | None -> ()

let write_wal_entry entry =
  match !wal_fd with
  | Some fd ->
      let serialized = serialize_wal_entry entry in
      output_bytes fd serialized;
      flush fd
  | None ->
      failwith "WAL not initialized"

let apply_wal_entry entry =
  match entry with
  | WALInitPage { dt_id; _ } ->
      let new_page = allocate_page () in
      new_page.header.keep_in_memory <- true;
      new_page.page.B2_tree_node.is_leaf <- false;
      release_page new_page
  | WALLogicalSplit { right_pid; parent_pid; left_pid; _ } ->
      let right_page = get_page right_pid in
      let parent_page = get_page parent_pid in
      let left_page = get_page left_pid in

      let sep_key, sep_len = 
        B2_tree_node.get_sep right_page.page (B2_tree_node.find_sep right_page.page) 
      in
      
      ignore (B2_tree_node.insert_do_not_copy_payload 
        parent_page.page sep_key sep_len (String.length (Int64.to_string left_pid)) (-1));
      Bytes.blit_string (Int64.to_string left_pid) 0 
        parent_page.page.data parent_page.page.data_offset 
        (String.length (Int64.to_string left_pid));
      
      B2_tree_node.set_fences left_page.page
        (B2_tree_node.get_lower_fence_key right_page.page) right_page.page.lower_fence.length
        sep_key sep_len;
      B2_tree_node.set_fences right_page.page
        sep_key sep_len
        (B2_tree_node.get_upper_fence_key right_page.page) right_page.page.upper_fence.length;
      
      release_page right_page;
      release_page parent_page;
      release_page left_page

let recover_from_wal filename =
  let ic = open_in_bin filename in
  try
    while true do
      let entry_bytes = Bytes.create 32 in
      really_input ic entry_bytes 0 32;
      let entry = deserialize_wal_entry entry_bytes in
      apply_wal_entry entry
    done
  with End_of_file ->
    close_in ic