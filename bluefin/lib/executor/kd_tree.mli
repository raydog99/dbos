open Tuple
open Range_join
open Parallel_hashtable

type kdtree

val build_kdtree : Tuple.tuple list -> int -> kdtree
val kdtree_lookup : kdtree -> Range_join.range -> Tuple.tuple list
val build_kdtrees : Parallel_hashtable.global_hashtable -> Tuple.tuple array -> (string, kdtree) Map.Make(String).t
val range_join_with_equality : Tuple.tuple array -> Tuple.tuple list -> (Tuple.tuple * Tuple.tuple) list