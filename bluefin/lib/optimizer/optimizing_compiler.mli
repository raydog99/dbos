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

module Staged : STAGED

(* Data types *)
type field = string * string
type record = { fields: field list }
type value = 
  | IntValue of int
  | FloatValue of float
  | StringValue of string
  | BoolValue of bool
  | DateValue of int * int * int

(* Operator types *)
type predicate = record -> bool
type key_fun = record -> record
type ord_fun = record -> record -> int
type agg_fun = record -> record -> record
type callback = record -> unit
type producer = callback -> unit
type consumer = record -> unit

type op

(* Query execution *)
val exec : op -> callback -> unit
val staged_exec : op -> (record Staged.code -> unit Staged.code) -> unit Staged.code

(* Helper functions to create operators *)
val make_scan : string -> op
val make_select : op -> predicate -> op
val make_project : op -> string list -> op
val make_hash_join : op -> op -> key_fun -> key_fun -> op
val make_sort_merge_join : op -> op -> key_fun -> key_fun -> op
val make_sort : op -> ord_fun -> op
val make_agg : op -> key_fun -> record -> agg_fun -> op
val make_distinct : op -> op
val make_limit : op -> int -> op

(* Query optimization *)
type statistics = {
  row_count: int;
  distinct_values: (string * int) list;
  min_max: (string * (value * value)) list;
}

val optimize : op -> statistics -> op

(* Indexing *)
type index

val create_btree_index : (record -> value) -> record list -> index
val create_hash_index : (record -> value) -> record list -> index

(* Query planner *)
val generate_query_plan : string -> statistics -> (string, index) Hashtbl.t -> op

(* Parallel execution *)
type parallel_op

val parallel_exec : parallel_op -> int -> callback -> unit

(* Main query execution function *)
val execute_query : string -> statistics -> (string, index) Hashtbl.t -> int -> unit

(* Helper function to parallelize a plan *)
val parallelize_plan : op -> int -> parallel_op