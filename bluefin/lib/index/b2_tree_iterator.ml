open B2_tree_types
open B2_tree_node

type t = {
  mutable btree: B2_tree_generic.t;
  mutable leaf: B2_tree_node.t;
  mutable pos: int;
  mutable key_buffer: bytes;
}

let create btree =
  {
    btree;
    leaf = btree.B2_tree_generic.meta_node;
    pos = -1;
    key_buffer = Bytes.create 1024;
  }

let seek_exact iter key =
  let leaf = B2_tree_generic.find_leaf iter.btree key (String.length key) in
  let pos = B2_tree_node.lower_bound leaf key (String.length key) in
  if pos < leaf.count && 
     B2_tree_node.compare_keys key (String.length key) 
       (B2_tree_node.get_key leaf pos) leaf.slots.(pos).key_len = 0 then
    begin
      iter.leaf <- leaf;
      iter.pos <- pos;
      OK
    end
  else
    NOT_FOUND

let seek iter key =
  let leaf = B2_tree_generic.find_leaf iter.btree key (String.length key) in
  let pos = B2_tree_node.lower_bound leaf key (String.length key) in
  iter.leaf <- leaf;
  iter.pos <- pos;
  if pos < leaf.count then OK else NOT_FOUND

let next iter =
  if iter.pos + 1 < iter.leaf.count then
    begin
      iter.pos <- iter.pos + 1;
      OK
    end
  else
    begin
      NOT_FOUND
    end

let prev iter =
  if iter.pos > 0 then
    begin
      iter.pos <- iter.pos - 1;
      OK
    end
  else
    begin
      NOT_FOUND
    end

let key iter =
  if iter.pos >= 0 && iter.pos < iter.leaf.count then
    let full_key = B2_tree_node.get_key iter.leaf iter.pos in
    { data = full_key; length = String.length full_key }
  else
    { data = ""; length = 0 }

let value iter =
  if iter.pos >= 0 && iter.pos < iter.leaf.count then
    let payload = B2_tree_node.get_payload iter.leaf iter.pos in
    { data = payload; length = String.length payload }
  else
    { data = ""; length = 0 }

let is_valid iter =
  iter.pos >= 0 && iter.pos < iter.leaf.count

let reset iter =
  iter.leaf <- iter.btree.B2_tree_generic.meta_node;
  iter.pos <- -1

let key_in_current_boundaries iter key =
  let cmp_lower = B2_tree_node.compare_keys key (String.length key) 
    (B2_tree_node.get_lower_fence_key iter.leaf) iter.leaf.lower_fence.length in
  let cmp_upper = B2_tree_node.compare_keys key (String.length key)
    (B2_tree_node.get_upper_fence_key iter.leaf) iter.leaf.upper_fence.length in
  cmp_lower > 0 && cmp_upper <= 0