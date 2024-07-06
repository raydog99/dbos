module RelNodeTyp : sig
  type t =
    | Logical
    | Physical

  val is_logical : t -> bool
end

module Value : sig
  type t =
    | Int of int
    | String of string
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
    cost: float;
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

module Memo : sig
  type 'a t

  val create : unit -> 'a t
  val next_id : 'a t -> int
  val get_reduced_group_id : 'a t -> group_id -> group_id
  val merge_group : 'a t -> group_id -> group_id -> group_id
  val add_expr_to_group : 'a t -> expr_id -> group_id -> 'a RelMemoNode.t -> unit
  val get_group_info : 'a t -> group_id -> GroupInfo.t
  val update_group_info : 'a t -> group_id -> GroupInfo.t -> unit
end