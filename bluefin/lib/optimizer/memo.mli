open Core

module RelNodeTyp : sig
  type t = 
    | Logical
    | Physical

  val is_logical : t -> bool
  val extract_group : t -> group_id option
end

module Value : sig
  type t = 
    | Int of int
    | String of string

  val to_string : t -> string
end

type group_id = int
type expr_id = int

module RelMemoNode : sig
  type 'a t = {
    typ: 'a;
    children: group_id list;
    data: Value.t option;
  }

  val to_string : 'a t -> string
end

module Winner : sig
  type t = {
    impossible: bool;
    expr_id: expr_id;
    cost: float list;
  }
end

module GroupInfo : sig
  type t = {
    winner: Winner.t option;
  }
end

module Group : sig
  type t = {
    group_exprs: expr_id list;
    info: GroupInfo.t;
    properties: Obj.t array;
  }
end

module ReducedGroupId : sig
  type t = int

  val as_group_id : t -> group_id
  val to_string : t -> string
end

module Memo : sig
  type 'a t

  val create : (Obj.t -> Obj.t) array -> 'a t
  val next_id : 'a t -> int
  val get_reduced_group_id : 'a t -> group_id -> group_id
  val merge_group : 'a t -> group_id -> group_id -> group_id
  val infer_properties : 'a t -> 'a RelMemoNode.t -> Obj.t array
  val add_expr_to_group : 'a t -> expr_id -> group_id -> 'a RelMemoNode.t -> unit
  val get_group_info : 'a t -> group_id -> GroupInfo.t
  val update_group_info : 'a t -> group_id -> GroupInfo.t -> unit
  val get_all_group_bindings : 'a t -> group_id -> bool -> bool -> int -> group_id list
  val get_all_expr_bindings : 'a t -> expr_id -> bool -> bool -> int -> expr_id list
  val get_all_exprs_in_group : 'a t -> group_id -> expr_id list
  val get_all_group_ids : 'a t -> group_id list
  val get_group : 'a t -> group_id -> Group.t
  val get_best_group_binding : 'a t -> group_id -> 'a -> ('a, Error.t) result
  val clear_winner : 'a t -> unit
  val compute_plan_space : 'a t -> int
end