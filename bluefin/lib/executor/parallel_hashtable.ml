open Tuple

module IntMap = Map.Make(Int)

type equivalence_group = {
  mutable count: int;
  mutable start: int;
  mutable end_: int;
}

type local_hashtable = {
  tables: (string, int) Hashtbl.t array;
  mutable total_count: int;
}

type global_hashtable = {
  tables: (string, equivalence_group) Hashtbl.t array;
  mutable total_count: int;
}

let hash_prefix hash_value = hash_value lsr (Sys.int_size - 9)

let create_local_hashtable () =
  { tables = Array.init 512 (fun _ -> Hashtbl.create 16); total_count = 0 }

let create_global_hashtable () =
  { tables = Array.init 512 (fun _ -> Hashtbl.create 16); total_count = 0 }

let add_to_local_hashtable local_ht key =
  let hash_value = Hashtbl.hash key in
  let table_index = hash_prefix hash_value in
  let table = local_ht.tables.(table_index) in
  match Hashtbl.find_opt table key with
  | Some count -> Hashtbl.replace table key (count + 1)
  | None -> Hashtbl.add table key 1;
  local_ht.total_count <- local_ht.total_count + 1

let merge_into_global_hashtable global_ht local_ht =
  Array.iteri (fun i local_table ->
    Hashtbl.iter (fun key local_count ->
      let global_table = global_ht.tables.(i) in
      match Hashtbl.find_opt global_table key with
      | Some group -> group.count <- group.count + local_count
      | None -> Hashtbl.add global_table key { count = local_count; start = 0; end_ = 0 }
    ) local_table
  ) local_ht.tables;
  global_ht.total_count <- global_ht.total_count + local_ht.total_count

let parallel_build_hashtable tuples =
  let num_threads = 4 in
  let local_hashtables = Array.init num_threads (fun _ -> create_local_hashtable ()) in
  
  (* Parallel materialization and local counting *)
  Array.parallel_iteri (fun i tuple ->
    let thread_id = i mod num_threads in
    add_to_local_hashtable local_hashtables.(thread_id) tuple.Tuple.id
  ) tuples;

  (* Merge local hashtables into global hashtable *)
  let global_ht = create_global_hashtable () in
  Array.iter (merge_into_global_hashtable global_ht) local_hashtables;

  (* Assign slices to equivalence groups *)
  let current_position = ref 0 in
  Array.iter (fun table ->
    Hashtbl.iter (fun _ group ->
      group.start <- !current_position;
      group.end_ <- !current_position + group.count - 1;
      current_position := !current_position + group.count
    ) table
  ) global_ht.tables;

  global_ht

let build_tuple_pointer_array global_ht tuples =
  let array_size = global_ht.total_count in
  let tuple_pointers = Array.make array_size (Obj.magic 0 : Tuple.tuple) in
  
  let num_threads = 4 in
  let local_positions = Array.make_matrix 512 num_threads 0 in
  
  Array.parallel_iteri (fun i tuple ->
    let thread_id = i mod num_threads in
    let hash_value = Hashtbl.hash tuple.Tuple.id in
    let table_index = hash_prefix hash_value in
    let group = Hashtbl.find global_ht.tables.(table_index) (string_of_int tuple.Tuple.id) in
    let position = Atomic.fetch_and_add local_positions.(table_index).(thread_id) 1 in
    tuple_pointers.(group.start + position) <- tuple
  ) tuples;

  tuple_pointers