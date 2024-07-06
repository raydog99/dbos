open B2_tree_types

type t = B2_tree_generic.t

let create dt_id config = B2_tree_generic.create dt_id config

let find tree key = B2_tree_generic.find tree key

let insert tree key value = B2_tree_generic.insert tree key value

let remove tree key = B2_tree_generic.remove tree key

module Iterator = struct
  type t = B2_tree_iterator.t

  let create tree = B2_tree_iterator.create tree

  let seek_exact = B2_tree_iterator.seek_exact

  let seek = B2_tree_iterator.seek

  let next = B2_tree_iterator.next

  let prev = B2_tree_iterator.prev

  let key = B2_tree_iterator.key

  let value = B2_tree_iterator.value

  let is_valid = B2_tree_iterator.is_valid

  let reset = B2_tree_iterator.reset
end