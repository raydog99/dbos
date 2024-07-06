open B2_tree_types

type slot = {
  mutable head: int;
  mutable key_len: int;
  mutable payload_len: int;
  mutable offset: int;
}

type fence_key = {
  mutable offset: int;
  mutable length: int;
}

type t = {
  mutable is_leaf: bool;
  mutable count: int;
  mutable data_offset: int;
  mutable space_used: int;
  mutable prefix_length: int;
  mutable lower_fence: fence_key;
  mutable upper_fence: fence_key;
  mutable slots: slot array;
  mutable data: bytes;
  mutable upper: swip_type;
}

let create is_leaf = 
  {
    is_leaf;
    count = 0;
    data_offset = 0;
    space_used = 0;
    prefix_length = 0;
    lower_fence = { offset = 0; length = 0 };
    upper_fence = { offset = 0; length = 0 };
    slots = Array.make 0 { head = 0; key_len = 0; payload_len = 0; offset = 0 };
    data = Bytes.create 0;
    upper = 0L;
  }

let get_key node slot_id =
  Bytes.sub_string node.data node.slots.(slot_id).offset node.slots.(slot_id).key_len

let get_payload node slot_id =
  let offset = node.slots.(slot_id).offset + node.slots.(slot_id).key_len in
  Bytes.sub_string node.data offset node.slots.(slot_id).payload_len

let get_full_key_len node slot_id =
  node.prefix_length + node.slots.(slot_id).key_len

let get_lower_fence_key node =
  Bytes.sub_string node.data node.lower_fence.offset node.lower_fence.length

let get_upper_fence_key node =
  Bytes.sub_string node.data node.upper_fence.offset node.upper_fence.length

let compare_keys a a_len b b_len =
  let len = min a_len b_len in
  let rec compare_aux i =
    if i = len then
      compare a_len b_len
    else
      let cmp = compare a.[i] b.[i] in
      if cmp = 0 then compare_aux (i + 1) else cmp
  in
  compare_aux 0

let lower_bound node key key_len =
  let rec binary_search left right =
    if left >= right then left
    else
      let mid = (left + right) / 2 in
      let cmp = compare_keys key key_len (get_key node mid) node.slots.(mid).key_len in
      if cmp <= 0 then binary_search left mid
      else binary_search (mid + 1) right
  in
  binary_search 0 node.count

let insert_do_not_copy_payload node key key_len payload_len pos =
  let slot_id = if pos = -1 then lower_bound node key key_len else pos in
  Array.blit node.slots slot_id node.slots (slot_id + 1) (node.count - slot_id);
  let key_offset = node.data_offset - key_len in
  let payload_offset = key_offset - payload_len in
  Bytes.blit_string key node.prefix_length node.data key_offset key_len;
  node.slots.(slot_id) <- {
    head = int_of_char key.[node.prefix_length];
    key_len;
    payload_len;
    offset = key_offset;
  };
  node.data_offset <- payload_offset;
  node.space_used <- node.space_used + key_len + payload_len;
  node.count <- node.count + 1;
  slot_id

let set_fences node lower_key lower_len upper_key upper_len =
  let insert_fence fence key key_len =
    if key <> "" then
      let offset = node.data_offset - key_len in
      Bytes.blit_string key 0 node.data offset key_len;
      node.data_offset <- offset;
      node.space_used <- node.space_used + key_len;
      fence.offset <- offset;
      fence.length <- key_len
  in
  insert_fence node.lower_fence lower_key lower_len;
  insert_fence node.upper_fence upper_key upper_len;
  node.prefix_length <- 
    let rec common_prefix i =
      if i < min lower_len upper_len && lower_key.[i] = upper_key.[i] then
        common_prefix (i + 1)
      else i
    in
    common_prefix 0

let find_sep node =
  if not node.is_leaf then
    { length = get_full_key_len node (node.count / 2); slot = node.count / 2; trunc = false }
  else
    let best_slot = (node.count - 1) / 2 in
    let common = 
      let key1 = get_key node best_slot in
      let key2 = get_key node (best_slot + 1) in
      let len = min (String.length key1) (String.length key2) in
      let rec common_prefix i =
        if i < len && key1.[i] = key2.[i] then common_prefix (i + 1) else i
      in
      common_prefix 0
    in
    if best_slot + 1 < node.count && 
       String.length (get_key node best_slot) > common && 
       String.length (get_key node (best_slot + 1)) > (common + 1) then
      { length = node.prefix_length + common + 1; slot = best_slot; trunc = true }
    else
      { length = get_full_key_len node best_slot; slot = best_slot; trunc = false }

let get_sep node info =
  let key = Bytes.create info.length in
  Bytes.blit_string (get_lower_fence_key node) 0 key 0 node.prefix_length;
  if info.trunc then
    Bytes.blit_string (get_key node (info.slot + 1)) 0 key node.prefix_length (info.length - node.prefix_length)
  else
    Bytes.blit_string (get_key node info.slot) 0 key node.prefix_length (info.length - node.prefix_length);
  Bytes.unsafe_to_string key

let split node parent left sep_slot sep_key sep_len =
  let right = create node.is_leaf in
  right.upper <- node.upper;
  
  if node.is_leaf then
    for i = sep_slot + 1 to node.count - 1 do
      let key = get_key node i in
      let payload = get_payload node i in
      ignore (insert_do_not_copy_payload right key (String.length key) (String.length payload) (-1));
      Bytes.blit_string payload 0 right.data right.data_offset (String.length payload)
    done
  else
    for i = sep_slot + 1 to node.count - 1 do
      let key = get_key node i in
      let child = Int64.to_string node.slots.(i).offset in
      ignore (insert_do_not_copy_payload right key (String.length key) (String.length child) (-1));
      Bytes.blit_string child 0 right.data right.data_offset (String.length child)
    done;

  node.count <- sep_slot + (if node.is_leaf then 1 else 0);
  set_fences right sep_key sep_len (get_upper_fence_key node) node.upper_fence.length;
  set_fences node (get_lower_fence_key node) node.lower_fence.length sep_key sep_len;
  
  let parent_slot = insert_do_not_copy_payload parent sep_key sep_len 8 (-1) in
  Bytes.blit_string (Int64.to_string left) 0 parent.data parent.data_offset 8;
  parent, right

let remove_slot node slot_id =
  node.space_used <- node.space_used - node.slots.(slot_id).key_len - node.slots.(slot_id).payload_len;
  Array.blit node.slots (slot_id + 1) node.slots slot_id (node.count - slot_id - 1);
  node.count <- node.count - 1

let remove node key key_len =
  let slot_id = lower_bound node key key_len in
  if slot_id < node.count && compare_keys key key_len (get_key node slot_id) node.slots.(slot_id).key_len = 0 then
    begin
      remove_slot node slot_id;
      true
    end
  else
    false