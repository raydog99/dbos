open B2_tree_types
open B2_tree_node

type t = {
  mutable dt_id: dtid;
  mutable config: Config.t;
  mutable meta_node: B2_tree_node.t;
  mutable height: int;
}

let create dt_id config =
  let tree = {
    dt_id;
    config;
    meta_node = B2_tree_node.create false;
    height = 1;
  } in
  let root = B2_tree_node.create true in
  tree.meta_node.upper <- Int64.of_int (Obj.magic root : int);
  tree

let rec find_leaf tree key key_len =
  let rec descend node depth =
    if node.is_leaf then node
    else
      let pos = B2_tree_node.lower_bound node key key_len in
      let child_ptr = 
        if pos = node.count then node.upper
        else Int64.of_string (B2_tree_node.get_payload node pos)
      in
      descend (Obj.magic (Int64.to_int child_ptr) : B2_tree_node.t) (depth + 1)
  in
  descend tree.meta_node 0

let find tree key =
  let leaf = find_leaf tree key (String.length key) in
  let pos = B2_tree_node.lower_bound leaf key (String.length key) in
  if pos < leaf.count && 
     B2_tree_node.compare_keys key (String.length key) 
       (B2_tree_node.get_key leaf pos) leaf.slots.(pos).key_len = 0 then
    Some (B2_tree_node.get_payload leaf pos)
  else
    None

let insert tree key value =
  let leaf = find_leaf tree key (String.length key) in
  let pos = B2_tree_node.lower_bound leaf key (String.length key) in
  if pos < leaf.count && 
     B2_tree_node.compare_keys key (String.length key) 
       (B2_tree_node.get_key leaf pos) leaf.slots.(pos).key_len = 0 then
    begin
      (* Update existing key *)
      let payload_offset = leaf.slots.(pos).offset + leaf.slots.(pos).key_len in
      Bytes.blit_string value 0 leaf.data payload_offset (String.length value);
      OK
    end
  else
    begin
      (* Insert new key-value pair *)
      let slot_id = B2_tree_node.insert_do_not_copy_payload leaf key (String.length key) (String.length value) (-1) in
      Bytes.blit_string value 0 leaf.data leaf.data_offset (String.length value);
      
      (* Check if we need to split *)
      if leaf.space_used > (Bytes.length leaf.data * 3) / 4 then
        begin
          let parent, right = B2_tree_node.split leaf tree.meta_node (Obj.magic leaf : int64) 
                                (leaf.count / 2) 
                                (B2_tree_node.get_key leaf (leaf.count / 2)) 
                                (B2_tree_node.get_full_key_len leaf (leaf.count / 2)) in
          tree.meta_node <- parent;
          tree.height <- tree.height + 1
        end;
      OK
    end

let remove tree key =
  let leaf = find_leaf tree key (String.length key) in
  if B2_tree_node.remove leaf key (String.length key) then OK else NOT_FOUND