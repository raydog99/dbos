(* Core types and modules *)
module type STAGED = sig
  type 'a code
  val lift: 'a -> 'a code
  val run: 'a code -> 'a
  val (+): int code -> int code -> int code
  val (-): int code -> int code -> int code
  val ( * ): int code -> int code -> int code
  val (/): int code -> int code -> int code
  val (mod): int code -> int code -> int code
  val (<): int code -> int code -> bool code
  val (>): int code -> int code -> bool code
  val (<=): int code -> int code -> bool code
  val (>=): int code -> int code -> bool code
  val (=): 'a code -> 'a code -> bool code
  val (&&): bool code -> bool code -> bool code
  val (||): bool code -> bool code -> bool code
  val not: bool code -> bool code
  val if_then_else: bool code -> 'a code -> 'a code -> 'a code
end

module Staged : STAGED = struct
  type 'a code = 'a
  let lift x = x
  let run x = x
  let (+) = (+)
  let (-) = (-)
  let ( * ) = ( * )
  let (/) = (/)
  let (mod) = (mod)
  let (<) = (<)
  let (>) = (>)
  let (<=) = (<=)
  let (>=) = (>=)
  let (=) = (=)
  let (&&) = (&&)
  let (||) = (||)
  let not = not
  let if_then_else cond then_branch else_branch =
    if cond then then_branch else else_branch
end

(* Data types *)
type field = string * string
type record = { fields: field list }
type value = 
  | IntValue of int
  | FloatValue of float
  | StringValue of string
  | BoolValue of bool
  | DateValue of int * int * int  (* year, month, day *)

(* Operator types *)
type predicate = record -> bool
type key_fun = record -> record
type ord_fun = record -> record -> int
type agg_fun = record -> record -> record
type callback = record -> unit
type producer = callback -> unit
type consumer = record -> unit

type op =
  | Scan of string * producer
  | Select of op * predicate * producer * consumer
  | Project of op * (string list) * producer * consumer
  | HashJoin of op * op * key_fun * key_fun * producer * consumer
  | SortMergeJoin of op * op * key_fun * key_fun * producer * consumer
  | Sort of op * ord_fun * producer * consumer
  | Agg of op * key_fun * record * agg_fun * producer * consumer
  | Distinct of op * producer * consumer
  | Limit of op * int * producer * consumer

(* Query execution *)
let rec exec (op: op) (cb: callback) : unit =
  match op with
  | Scan (table, produce) ->
      produce cb
  | Select (child, pred, produce, consume) ->
      exec child (fun tuple ->
        if pred tuple then consume tuple
      );
      produce cb
  | Project (child, columns, produce, consume) ->
      exec child (fun tuple ->
        let projected = { fields = List.filter (fun (name, _) -> List.mem name columns) tuple.fields } in
        consume projected
      );
      produce cb
  | HashJoin (left, right, lkey, rkey, produce, consume) ->
      let hash_table = Hashtbl.create 1024 in
      exec left (fun l_tuple ->
        let key = lkey l_tuple in
        Hashtbl.add hash_table key l_tuple
      );
      exec right (fun r_tuple ->
        let key = rkey r_tuple in
        if Hashtbl.mem hash_table key then
          let l_tuples = Hashtbl.find_all hash_table key in
          List.iter (fun l_tuple ->
            consume { fields = l_tuple.fields @ r_tuple.fields }
          ) l_tuples
      );
      produce cb
  | SortMergeJoin (left, right, lkey, rkey, produce, consume) ->
      let left_data = ref [] in
      let right_data = ref [] in
      exec left (fun l_tuple -> left_data := l_tuple :: !left_data);
      exec right (fun r_tuple -> right_data := r_tuple :: !right_data);
      let sorted_left = List.sort (fun a b -> compare (lkey a) (lkey b)) !left_data in
      let sorted_right = List.sort (fun a b -> compare (rkey a) (rkey b)) !right_data in
      let rec merge l r =
        match l, r with
        | [], _ | _, [] -> ()
        | lh::lt, rh::rt ->
            let lk, rk = lkey lh, rkey rh in
            if lk = rk then
              let matching_right = List.take_while (fun x -> rkey x = rk) r in
              List.iter (fun r_tuple ->
                consume { fields = lh.fields @ r_tuple.fields }
              ) matching_right;
              merge l rt
            else if lk < rk then
              merge lt r
            else
              merge l rt
      in
      merge sorted_left sorted_right;
      produce cb
  | Sort (child, ord_fun, produce, consume) ->
      let data = ref [] in
      exec child (fun tuple -> data := tuple :: !data);
      let sorted_data = List.sort ord_fun !data in
      List.iter consume sorted_data;
      produce cb
  | Agg (child, group_key, init, agg_fun, produce, consume) ->
      let agg_table = Hashtbl.create 1024 in
      exec child (fun tuple ->
        let key = group_key tuple in
        let current = 
          try Hashtbl.find agg_table key
          with Not_found -> init
        in
        let new_agg = agg_fun current tuple in
        Hashtbl.replace agg_table key new_agg
      );
      Hashtbl.iter (fun _ value -> consume value) agg_table;
      produce cb
  | Distinct (child, produce, consume) ->
      let seen = Hashtbl.create 1024 in
      exec child (fun tuple ->
        let key = tuple in (* Use the entire tuple as the key *)
        if not (Hashtbl.mem seen key) then begin
          Hashtbl.add seen key ();
          consume tuple
        end
      );
      produce cb
  | Limit (child, n, produce, consume) ->
      let count = ref 0 in
      exec child (fun tuple ->
        if !count < n then begin
          consume tuple;
          incr count
        end
      );
      produce cb

(* Staged query execution *)
let rec staged_exec (op: op) (cb: record Staged.code -> unit Staged.code) : unit Staged.code =
  match op with
  | Scan (table, produce) ->
      Staged.(lift (
        produce (fun r -> run (cb (lift r)))
      ))
  | Select (child, pred, produce, consume) ->
      Staged.(
        run (staged_exec child (fun tuple ->
          lift (
            if run (lift pred) (run tuple) then
              run (lift consume) (run tuple)
            else ()
          )
        ));
        lift (produce (fun r -> run (cb (lift r))))
      )
  | Project (child, columns, produce, consume) ->
      Staged.(
        run (staged_exec child (fun tuple ->
          lift (
            let projected = { fields = List.filter (fun (name, _) -> List.mem name columns) (run tuple).fields } in
            run (lift consume) projected
          )
        ));
        lift (produce (fun r -> run (cb (lift r))))
      )
  | HashJoin (left, right, lkey, rkey, produce, consume) ->
      Staged.(lift (
        let hash_table = Hashtbl.create 1024 in
        run (staged_exec left (fun l_tuple ->
          lift (
            let key = run (lift lkey) (run l_tuple) in
            Hashtbl.add hash_table key (run l_tuple)
          )
        ));
        run (staged_exec right (fun r_tuple ->
          lift (
            let key = run (lift rkey) (run r_tuple) in
            if Hashtbl.mem hash_table key then
              let l_tuples = Hashtbl.find_all hash_table key in
              List.iter (fun l_tuple ->
                run (lift consume) { fields = l_tuple.fields @ (run r_tuple).fields }
              ) l_tuples
          )
        ));
        produce (fun r -> run (cb (lift r)))
      ))
  | SortMergeJoin (left, right, lkey, rkey, produce, consume) ->
      Staged.(lift (
        let left_data = ref [] in
        let right_data = ref [] in
        run (staged_exec left (fun l_tuple -> lift (left_data := run l_tuple :: !left_data)));
        run (staged_exec right (fun r_tuple -> lift (right_data := run r_tuple :: !right_data)));
        let sorted_left = List.sort (fun a b -> compare (run (lift lkey) a) (run (lift lkey) b)) !left_data in
        let sorted_right = List.sort (fun a b -> compare (run (lift rkey) a) (run (lift rkey) b)) !right_data in
        let rec merge l r =
          match l, r with
          | [], _ | _, [] -> ()
          | lh::lt, rh::rt ->
              let lk, rk = run (lift lkey) lh, run (lift rkey) rh in
              if lk = rk then
                let matching_right = List.take_while (fun x -> run (lift rkey) x = rk) r in
                List.iter (fun r_tuple ->
                  run (lift consume) { fields = lh.fields @ r_tuple.fields }
                ) matching_right;
                merge l rt
              else if lk < rk then
                merge lt r
              else
                merge l rt
        in
        merge sorted_left sorted_right;
        produce (fun r -> run (cb (lift r)))
      ))
  | Sort (child, ord_fun, produce, consume) ->
      Staged.(lift (
        let data = ref [] in
        run (staged_exec child (fun tuple -> lift (data := run tuple :: !data)));
        let sorted_data = List.sort (fun a b -> run (lift ord_fun) a b) !data in
        List.iter (fun tuple -> run (lift consume) tuple) sorted_data;
        produce (fun r -> run (cb (lift r)))
      ))
  | Agg (child, group_key, init, agg_fun, produce, consume) ->
      Staged.(lift (
        let agg_table = Hashtbl.create 1024 in
        run (staged_exec child (fun tuple ->
          lift (
            let key = run (lift group_key) (run tuple) in
            let current = 
              try Hashtbl.find agg_table key
              with Not_found -> init
            in
            let new_agg = run (lift agg_fun) current (run tuple) in
            Hashtbl.replace agg_table key new_agg
          )
        ));
        Hashtbl.iter (fun _ value -> run (lift consume) value) agg_table;
        produce (fun r -> run (cb (lift r)))
      ))
  | Distinct (child, produce, consume) ->
      Staged.(lift (
        let seen = Hashtbl.create 1024 in
        run (staged_exec child (fun tuple ->
          lift (
            let key = run tuple in
            if not (Hashtbl.mem seen key) then begin
              Hashtbl.add seen key ();
              run (lift consume) (run tuple)
            end
          )
        ));
        produce (fun r -> run (cb (lift r)))
      ))
  | Limit (child, n, produce, consume) ->
      Staged.(lift (
        let count = ref 0 in
        run (staged_exec child (fun tuple ->
          lift (
            if !count < n then begin
              run (lift consume) (run tuple);
              incr count
            end
          )
        ));
        produce (fun r -> run (cb (lift r)))
      ))

(* Helper functions to create operators *)
let make_scan table =
  Scan (table, fun cb ->
    let data = read_table table in
    List.iter cb data
  )

let make_select child pred =
  Select (child, pred,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_project child columns =
  Project (child, columns,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_hash_join left right lkey rkey =
  HashJoin (left, right, lkey, rkey,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_sort_merge_join left right lkey rkey =
  SortMergeJoin (left, right, lkey, rkey,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_sort child ord_fun =
  Sort (child, ord_fun,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_agg child group_key init agg_fun =
  Agg (child, group_key, init, agg_fun,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_distinct child =
  Distinct (child,
    (fun cb -> ()),
    (fun r -> cb r)
  )

let make_limit child n =
  Limit (child, n,
    (fun cb -> ()),
    (fun r -> cb r)
  )

(* Query optimization *)
type statistics = {
  row_count: int;
  distinct_values: (string * int) list;
  min_max: (string * (value * value)) list;
}

let estimate_selectivity (pred: predicate) (stats: statistics) : float =
  0.1

let estimate_cost (op: op) (stats: statistics) : float =
  match op with
  | Scan _ -> float_of_int stats.row_count
  | Select (child, pred, _, _) ->
      let child_cost = estimate_cost child stats in
      let selectivity = estimate_selectivity pred stats in
      child_cost *. selectivity
  | HashJoin (left, right, _, _, _, _) ->
      let left_cost = estimate_cost left stats in
      let right_cost = estimate_cost right stats in
      left_cost +. right_cost +. (float_of_int stats.row_count *. 1.2)
  | _ -> float_of_int stats.row_count

let rec optimize (op: op) (stats: statistics) : op =
  match op with
  | Select (child, pred, produce, consume) ->
      let optimized_child = optimize child stats in
      Select (optimized_child, pred, produce, consume)
  | HashJoin (left, right, lkey, rkey, produce, consume) ->
      let optimized_left = optimize left stats in
      let optimized_right = optimize right stats in
      if estimate_cost (HashJoin (optimized_left, optimized_right, lkey, rkey, produce, consume)) stats 
         estimate_cost (SortMergeJoin (optimized_left, optimized_right, lkey, rkey, produce, consume)) stats
      then HashJoin (optimized_left, optimized_right, lkey, rkey, produce, consume)
      else SortMergeJoin (optimized_left, optimized_right, lkey, rkey, produce, consume)
  | Project (child, columns, produce, consume) ->
      let optimized_child = optimize child stats in
      Project (optimized_child, columns, produce, consume)
  | Sort (child, ord_fun, produce, consume) ->
      let optimized_child = optimize child stats in
      Sort (optimized_child, ord_fun, produce, consume)
  | Agg (child, group_key, init, agg_fun, produce, consume) ->
      let optimized_child = optimize child stats in
      Agg (optimized_child, group_key, init, agg_fun, produce, consume)
  | Distinct (child, produce, consume) ->
      let optimized_child = optimize child stats in
      Distinct (optimized_child, produce, consume)
  | Limit (child, n, produce, consume) ->
      let optimized_child = optimize child stats in
      Limit (optimized_child, n, produce, consume)
  | op -> op  (* For Scan, we don't optimize further *)

(* Indexing *)
type index = 
  | BTreeIndex of (record -> value) * (value -> record list)
  | HashIndex of (record -> value) * (value -> record list)

let create_btree_index key_extractor data =
  let tree = ref Map.empty in
  List.iter (fun record ->
    let key = key_extractor record in
    tree := Map.update key (function
      | None -> Some [record]
      | Some records -> Some (record :: records)
    ) !tree
  ) data;
  BTreeIndex (key_extractor, fun key -> 
    match Map.find_opt key !tree with
    | Some records -> records
    | None -> []
  )

let create_hash_index key_extractor data =
  let table = Hashtbl.create (List.length data) in
  List.iter (fun record ->
    let key = key_extractor record in
    Hashtbl.add table key record
  ) data;
  HashIndex (key_extractor, fun key -> 
    Hashtbl.find_all table key
  )

(* Index-aware scan operator *)
let make_index_scan table index =
  match index with
  | BTreeIndex (key_extractor, lookup) ->
      Scan (table, fun cb ->
        let (start_key, end_key) = get_key_range table in
        let rec scan_range current_key =
          if current_key <= end_key then
            let records = lookup current_key in
            List.iter cb records;
            scan_range (Map.next current_key)
        in
        scan_range start_key
      )
  | HashIndex (key_extractor, lookup) ->
      Scan (table, fun cb ->
        let keys = get_lookup_keys table in
        List.iter (fun key ->
          let records = lookup key in
          List.iter cb records
        ) keys
      )

(* Query planner *)
let generate_query_plan query stats indexes =
  let initial_plan = parse_query query in
  let optimized_plan = optimize initial_plan stats in
  
  let rec apply_indexes plan =
    match plan with
    | Scan (table, _) when Hashtbl.mem indexes table ->
        make_index_scan table (Hashtbl.find indexes table)
    | Select (child, pred, produce, consume) ->
        Select (apply_indexes child, pred, produce, consume)
    | Project (child, columns, produce, consume) ->
        Project (apply_indexes child, columns, produce, consume)
    | HashJoin (left, right, lkey, rkey, produce, consume) ->
        HashJoin (apply_indexes left, apply_indexes right, lkey, rkey, produce, consume)
    | SortMergeJoin (left, right, lkey, rkey, produce, consume) ->
        SortMergeJoin (apply_indexes left, apply_indexes right, lkey, rkey, produce, consume)
    | Sort (child, ord_fun, produce, consume) ->
        Sort (apply_indexes child, ord_fun, produce, consume)
    | Agg (child, group_key, init, agg_fun, produce, consume) ->
        Agg (apply_indexes child, group_key, init, agg_fun, produce, consume)
    | Distinct (child, produce, consume) ->
        Distinct (apply_indexes child, produce, consume)
    | Limit (child, n, produce, consume) ->
        Limit (apply_indexes child, n, produce, consume)
  in
  
  apply_indexes optimized_plan

(* Parallel execution *)
type parallel_op =
  | ParallelScan of string * int * (int -> producer)
  | ParallelSelect of parallel_op * predicate * (int -> producer) * (int -> consumer)
  | ParallelHashJoin of parallel_op * parallel_op * key_fun * key_fun * (int -> producer) * (int -> consumer)
  | ParallelAgg of parallel_op * key_fun * record * agg_fun * (int -> producer) * (int -> consumer)

let rec parallel_exec (op: parallel_op) (num_threads: int) (cb: callback) : unit =
  match op with
  | ParallelScan (table, num_partitions, produce) ->
      let results = Array.make num_threads [] in
      let workers = Array.init num_threads (fun thread_id ->
        Thread.create (fun () ->
          produce thread_id (fun r -> 
            results.(thread_id) <- r :: results.(thread_id)
          )
        ) ()
      ) in
      Array.iter Thread.join workers;
      Array.iter (List.iter cb) results
  | ParallelSelect (child, pred, produce, consume) ->
      parallel_exec child num_threads (fun tuple ->
        if pred tuple then
          for thread_id = 0 to num_threads - 1 do
            consume thread_id tuple
          done
      );
      for thread_id = 0 to num_threads - 1 do
        produce thread_id cb
      done
  | ParallelHashJoin (left, right, lkey, rkey, produce, consume) ->
      let hash_tables = Array.init num_threads (fun _ -> Hashtbl.create 1024) in
      parallel_exec left num_threads (fun l_tuple ->
        let key = lkey l_tuple in
        let thread_id = Hashtbl.hash key mod num_threads in
        Hashtbl.add hash_tables.(thread_id) key l_tuple
      );
      parallel_exec right num_threads (fun r_tuple ->
        let key = rkey r_tuple in
        let thread_id = Hashtbl.hash key mod num_threads in
        if Hashtbl.mem hash_tables.(thread_id) key then
          let l_tuples = Hashtbl.find_all hash_tables.(thread_id) key in
          List.iter (fun l_tuple ->
            consume thread_id { fields = l_tuple.fields @ r_tuple.fields }
          ) l_tuples
      );
      for thread_id = 0 to num_threads - 1 do
        produce thread_id cb
      done
  | ParallelAgg (child, group_key, init, agg_fun, produce, consume) ->
      let agg_tables = Array.init num_threads (fun _ -> Hashtbl.create 1024) in
      parallel_exec child num_threads (fun tuple ->
        let key = group_key tuple in
        let thread_id = Hashtbl.hash key mod num_threads in
        let current = 
          try Hashtbl.find agg_tables.(thread_id) key
          with Not_found -> init
        in
        let new_agg = agg_fun current tuple in
        Hashtbl.replace agg_tables.(thread_id) key new_agg
      );
      for thread_id = 0 to num_threads - 1 do
        Hashtbl.iter (fun _ value -> consume thread_id value) agg_tables.(thread_id);
        produce thread_id cb
      done

(* Main query execution function *)
let execute_query query stats indexes num_threads =
  let plan = generate_query_plan query stats indexes in
  let parallel_plan = parallelize_plan plan num_threads in
  parallel_exec parallel_plan num_threads (fun r ->
    print_endline (string_of_record r)
  )

(* Helper function to parallelize a plan *)
let rec parallelize_plan plan num_threads =
  match plan with
  | Scan (table, _) ->
      ParallelScan (table, num_threads, fun thread_id cb ->
        let data = read_table_partition table thread_id num_threads in
        List.iter cb data
      )
  | Select (child, pred, _, _) ->
      let parallel_child = parallelize_plan child num_threads in
      ParallelSelect (parallel_child, pred,
        (fun thread_id cb -> ()),
        (fun thread_id r -> cb r)
      )
  | HashJoin (left, right, lkey, rkey, _, _) ->
      let parallel_left = parallelize_plan left num_threads in
      let parallel_right = parallelize_plan right num_threads in
      ParallelHashJoin (parallel_left, parallel_right, lkey, rkey,
        (fun thread_id cb -> ()),
        (fun thread_id r -> cb r)
      )
  | Agg (child, group_key, init, agg_fun, _, _) ->
      let parallel_child = parallelize_plan child num_threads in
      ParallelAgg (parallel_child, group_key, init, agg_fun,
        (fun thread_id cb -> ()),
        (fun thread_id r -> cb r)
      )
  | _ -> failwith "Unsupported operator for parallelization"