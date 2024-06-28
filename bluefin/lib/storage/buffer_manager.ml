module BufferManager = struct
  type buffer_frame = {
    mutable data: bytes;
    mutable header: header;
    mutable is_dirty: bool;
    mutable gsn: int;
  }
  and header = {
    mutable pid: int;
    mutable dt_id: int;
    mutable keep_in_memory: bool;
    mutable latch: Mutex.t;
  }

  type page_guard = {
    frame: buffer_frame;
    mutable is_exclusive: bool;
  }

  let page_size = 4096

  let allocate_page () =
    { data = Bytes.create page_size;
      header = { pid = 0; dt_id = 0; keep_in_memory = false; latch = Mutex.create () };
      is_dirty = false;
      gsn = 0 }

  let set_keep_in_memory frame value =
    frame.header.keep_in_memory <- value

  let set_dt_id frame dt_id =
    frame.header.dt_id <- dt_id

  let init_as_leaf frame =
    Bytes.set frame.data 0 '\001'

  let set_upper frame upper =
    let pid = upper.header.pid in
    Bytes.set_int32_be frame.data 0 (Int32.of_int pid)

  let increment_gsn frame =
    frame.gsn <- frame.gsn + 1

  let mark_as_dirty frame =
    frame.is_dirty <- true

  let get_count guard =
    Bytes.get_uint16_be guard.frame.data 2

  let get_full_key_len guard pos =
    let offset = 4 + pos * 2 in
    Bytes.get_uint16_be guard.frame.data offset

  let find_sep guard =
    let count = get_count guard in
    let total_len = ref 0 in
    for i = 0 to count - 1 do
      total_len := !total_len + get_full_key_len guard i
    done;
    let avg_len = !total_len / count in
    let mut_total = ref 0 in
    let mut_i = ref 0 in
    while !mut_total < !total_len / 2 && !mut_i < count - 1 do
      mut_total := !mut_total + get_full_key_len guard !mut_i;
      incr mut_i
    done;
    { length = avg_len; slot = !mut_i; is_underflow = false }

  let get_sep guard sep_info =
    let offset = 4 + sep_info.slot * 2 in
    Bytes.sub guard.frame.data offset sep_info.length

  let is_meta_node guard =
    guard.frame.header.pid = 0

  let upgrade_to_exclusive guard =
    Mutex.lock guard.frame.header.latch;
    { guard with is_exclusive = true }

  let space_needed guard length =
    4 + length

  let has_enough_space guard space_needed =
    let free_space = page_size - (Bytes.get_uint16_be guard.frame.data 0) in
    free_space >= space_needed

  let request_space guard space_needed =
    let current_used = Bytes.get_uint16_be guard.frame.data 0 in
    Bytes.set_uint16_be guard.frame.data 0 (current_used + space_needed)

  let is_leaf guard =
    Bytes.get guard.frame.data 0 = '\001'

  let split source target new_left slot sep_key =
    let count = get_count source in
    let new_count = count - slot - 1 in
    Bytes.set_uint16_be source.frame.data 2 new_count;
    Bytes.set_uint16_be new_left.frame.data 2 slot;
    Bytes.blit source.frame.data (4 + (slot + 1) * 2) new_left.frame.data 4 (new_count * 2);
    Bytes.set_uint16_be target.frame.data 2 ((get_count target) + 1);
    Bytes.blit sep_key 0 target.frame.data (4 + (get_count target) * 2) (Bytes.length sep_key)

  let unlock guard =
    if guard.is_exclusive then
      Mutex.unlock guard.frame.header.latch

  let get_child_guard parent_guard swip =
    let pid = Bytes.get_int32_be parent_guard.frame.data (Int32.to_int swip) in
    let frame = allocate_page () in
    frame.header.pid <- Int32.to_int pid;
    { frame = frame; is_exclusive = false }
end