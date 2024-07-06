open Tuple

type range = {
  min_values: float array;
  max_values: float array;
}

val range_join : Tuple.tuple list -> Tuple.tuple list -> (Tuple.tuple * Tuple.tuple) list
val create_range : Tuple.tuple -> range