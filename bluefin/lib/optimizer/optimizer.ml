open Core

module RelNodeTyp = struct
  type t = 
    | Logical
    | Physical

  let is_logical = function
    | Logical -> true
    | _ -> false
end

module Value = struct
  type t = 
    | Int of int
    | String of string
end

type group_id = int
type expr_id = int

module RelMemoNode = struct
  type 'a t = {
    typ: 'a;
    children: group_id list;
    data: Value.t option;
  }

  let to_string node =
    let children_str = String.concat ~sep:" " (List.map node.children ~f:Int.to_string) in
    let data_str = Option.value_map node.data ~default:"" ~f:(fun v -> 
      match v with
      | Value.Int i -> Int.to_string i
      | Value.String s -> s
    ) in
    Printf.sprintf "(%s %s %s)" (Sexp.to_string (RelNodeTyp.sexp_of_t node.typ)) data_str children_str
end

module Winner = struct
  type t = {
    impossible: bool;
    expr_id: expr_id;
    cost: float;
  }
end

module GroupInfo = struct
  type t = {
    winner: Winner.t option;
  }
end

module Group = struct
  type t = {
    group_exprs: expr_id list;
    info: GroupInfo.t;
    properties: Obj.t array; (* Using Obj.t to simulate Any type *)
  }
end

module Memo = struct
  type 'a t = {
    expr_id_to_group_id: (expr_id, group_id) Hashtbl.t;
    expr_id_to_expr_node: (expr_id, 'a RelMemoNode.t) Hashtbl.t;
    expr_node_to_expr_id: ('a RelMemoNode.t, expr_id) Hashtbl.t;
    groups: (group_id, Group.t) Hashtbl.t;
    mutable group_expr_counter: int;
    merged_groups: (group_id, group_id) Hashtbl.t;
  }

  let create () = {
    expr_id_to_group_id = Hashtbl.create (module Int);
    expr_id_to_expr_node = Hashtbl.create (module Int);
    expr_node_to_expr_id = Hashtbl.create (module Poly);
    groups = Hashtbl.create (module Int);
    group_expr_counter = 0;
    merged_groups = Hashtbl.create (module Int);
  }

  let next_id t =
    let id = t.group_expr_counter in
    t.group_expr_counter <- id + 1;
    id

  let get_reduced_group_id t group_id =
    let rec find_root gid =
      match Hashtbl.find t.merged_groups gid with
      | Some next_gid -> find_root next_gid
      | None -> gid
    in
    find_root group_id

  let merge_group t group_a group_b =
    let group_a = get_reduced_group_id t group_a in
    let group_b = get_reduced_group_id t group_b in
    if group_a = group_b then group_b
    else begin
      Hashtbl.set t.merged_groups ~key:group_a ~data:group_b;
      group_b
    end

  let add_expr_to_group t expr_id group_id memo_node =
    match Hashtbl.find t.groups group_id with
    | Some group ->
        Hashtbl.set t.groups ~key:group_id 
          ~data:{group with Group.group_exprs = expr_id :: group.group_exprs}
    | None ->
        let new_group = {
          Group.group_exprs = [expr_id];
          info = {GroupInfo.winner = None};
          properties = [||];
        } in
        Hashtbl.set t.groups ~key:group_id ~data:new_group

  let get_group_info t group_id =
    let group_id = get_reduced_group_id t group_id in
    match Hashtbl.find t.groups group_id with
    | Some group -> group.Group.info
    | None -> failwith "Group not found"

  let update_group_info t group_id group_info =
    let group_id = get_reduced_group_id t group_id in
    match Hashtbl.find t.groups group_id with
    | Some group ->
        Hashtbl.set t.groups ~key:group_id ~data:{group with Group.info = group_info}
    | None -> failwith "Group not found"

end