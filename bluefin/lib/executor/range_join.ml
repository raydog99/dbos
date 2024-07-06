open Tuple

type range = {
  min_values: float array;
  max_values: float array;
}

let range_join build_tuples probe_tuples =
  let kdtree = build_kdtree build_tuples 0 in
  List.fold_left (fun acc probe_tuple ->
    let range = create_range probe_tuple in
    let matches = kdtree_lookup kdtree range in
    acc @ List.map (fun match_tuple -> (probe_tuple, match_tuple)) matches
  ) [] probe_tuples

let create_range probe_tuple =
	let padding = 0.1 in
	let min_values = Array.map (fun v -> v -. padding) probe_tuple.values in
	let max_values = Array.map (fun v -> v +. padding) probe_tuple.values in
	{ min_values; max_values }