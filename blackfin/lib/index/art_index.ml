type 'a node =
  | Node4 of int * (int * 'a) list
  | Node16 of int * (int * 'a) array
  | Node48 of int * (int * 'a option) array * 'a array
  | Node256 of 'a array

type 'a t = 'a node ref

let empty () = ref (Node4 (0, []))

let find_child node byte =
  match !node with
  | Node4 (_, children) ->
     let rec loop = function
       | [] -> None
       | (k, v)::tl -> if k = byte then Some v else loop tl
     in loop children
  | Node16 (_, children) ->
     if byte >= Array.length children then None
     else Some children.(byte)
  | Node48 (_, child_indices, children) ->
     let idx = child_indices.(byte) in
     if idx = -1 then None else Some (children.(idx))
  | Node256 children -> Some children.(byte)

let rec insert tree key value =
  match !tree with
  | Node4 (count, children) ->
     let children = List.sort (fun (k1, _) (k2, _) -> compare k1 k2) children in
     (match List.split_when (fun (k, _) -> k >= key) children with
      | [], [] -> tree := Node4 (count + 1, [(key, value)])
      | prefix, (k, child)::suffix when k = key ->
         tree := Node4 (count, prefix @ [(k, value)] @ suffix)
      | prefix, suffix ->
         let node = Node4 (1, [(key, value)]) in
         let new_node = Node16 (count + 2, Array.make 16 (Node4 (0, []))) in
         let rec insert_children = function
           | [], [] -> ()
           | (k, v)::tl, idx::rest ->
              let child = ref (Node4 (0, [(k, v)])) in
              new_node.Node16.1.(idx) <- (k, child);
              insert_children (tl, rest) child
           | _ -> failwith "Invalid state"
         in
         insert_children (prefix @ suffix, []);
         tree := new_node)
  | _ -> failwith "Not implemented"

let rec lookup tree key =
  match !tree with
  | Node4 (_, children) ->
     let rec loop = function
       | [] -> None
       | (k, v)::tl -> if k = key then Some v else loop tl
     in loop children
  | Node16 (_, children) ->
     if key >= Array.length children then None
     else lookup children.(key) key
  | Node48 (_, child_indices, children) ->
     let idx = child_indices.(key) in
     if idx = -1 then None else lookup children.(idx) key
  | Node256 children ->
     if key >= Array.length children then None
     else Some children.(key)