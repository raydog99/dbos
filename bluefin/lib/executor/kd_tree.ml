type kdtree = 
  | Leaf of tuple
  | Node of {
      tuple: tuple;
      dim: int;
      left: kdtree;
      right: kdtree;
    }

let rec build_kdtree tuples depth =
  match tuples with
  | [] -> failwith "Empty tuple list"
  | [t] -> Leaf t
  | _ ->
      let dim = depth mod (Array.length (List.hd tuples).values) in
      let sorted_tuples = List.sort (compare_tuple dim) tuples in
      let median_index = List.length tuples / 2 in
      let median_tuple = List.nth sorted_tuples median_index in
      let left_tuples = List.take median_index sorted_tuples in
      let right_tuples = List.drop (median_index + 1) sorted_tuples in
      Node {
        tuple = median_tuple;
        dim;
        left = build_kdtree left_tuples (depth + 1);
        right = build_kdtree right_tuples (depth + 1);
      }

let rec kdtree_lookup tree range =
  match tree with
  | Leaf t ->
      if tuple_in_range t range then [t] else []
  | Node {tuple; dim; left; right} ->
      let results = if tuple_in_range tuple range then [tuple] else [] in
      if range_left_of_tuple range dim tuple then
        results @ kdtree_lookup left range
      else if range_right_of_tuple range dim tuple then
        results @ kdtree_lookup right range
      else
        results @ kdtree_lookup left range @ kdtree_lookup right range

let tuple_in_range tuple range =
  Array.for_all2 (fun value (min_val, max_val) ->
    value >= min_val && value <= max_val
  ) tuple.values (Array.combine range.min_values range.max_values)

let range_left_of_tuple range dim tuple =
  range.max_values.(dim) < tuple.values.(dim)

let range_right_of_tuple range dim tuple =
  range.min_values.(dim) > tuple.values.(dim)

let build_kdtrees global_ht tuple_pointers =
  let build_tree_for_group key group =
    let group_tuples = Array.sub tuple_pointers group.start (group.end_ - group.start + 1) in
    (key, build_kdtree (Array.to_list group_tuples) 0)
  in
  
  let trees = Array.fold_left (fun acc table ->
    Hashtbl.fold (fun key group acc ->
      (build_tree_for_group key group) :: acc
    ) table acc
  ) [] global_ht.tables in
  
  IntMap.of_list trees

let range_join_with_equality build_tuples probe_tuples =
  let global_ht = Parallel_hashtable.parallel_build_hashtable build_tuples in
  let tuple_pointers = Parallel_hashtable.build_tuple_pointer_array global_ht build_tuples in
  let kdtrees = build_kdtrees global_ht tuple_pointers in
  
  List.fold_left (fun acc probe_tuple ->
    let eq_key = string_of_int probe_tuple.Tuple.id in
    let range = create_range probe_tuple in
    match IntMap.find_opt eq_key kdtrees with
    | Some tree ->
        let matches = kdtree_lookup tree range in
        acc @ List.map (fun match_tuple -> (probe_tuple, match_tuple)) matches
    | None -> acc
  ) [] probe_tuples