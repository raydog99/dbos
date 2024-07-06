type tuple = {
  id: int;
  values: float array;
}

val create_tuple : int -> float array -> tuple
val compare_tuple : int -> tuple -> tuple -> int