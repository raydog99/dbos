open Tuple

type equivalence_group = {
  mutable count: int;
  mutable start: int;
  mutable end_: int;
}

type local_hashtable
type global_hashtable

val parallel_build_hashtable : Tuple.tuple array -> global_hashtable
val build_tuple_pointer_array : global_hashtable -> Tuple.tuple array -> Tuple.tuple array