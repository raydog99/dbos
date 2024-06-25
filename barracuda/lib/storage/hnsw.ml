module HNSW = struct
  type vector = float array
  type id = int
  type distance = float

  type node = {
    id: id;
    vector: vector;
    mutable connections: (id * distance) list array;
  }

  type t = {
    nodes: (id, node) Hashtbl.t;
    entry_point: id option ref;
    max_level: int;
    ef_construction: int;
    m: int;
    m_max_0: int;
    m_max: int;
    distance_fn: vector -> vector -> distance;
  }

  let create ?(max_level=16) ?(ef_construction=200) ?(m=16) ?(m_max_0=64) ?(m_max=32) distance_fn =
    {
      nodes = Hashtbl.create 1000;
      entry_point = ref None;
      max_level;
      ef_construction;
      m;
      m_max_0;
      m_max;
      distance_fn;
    }

  let get_random_level t =
    let rec loop level =
      if level >= t.max_level - 1 || Random.float 1.0 > 1.0 /. float_of_int t.m then level
      else loop (level + 1)
    in
    loop 0

  let insert t id vector =
    let level = get_random_level t in
    let node = {
      id;
      vector;
      connections = Array.make (level + 1) [];
    } in
    Hashtbl.add t.nodes id node;

    match !(t.entry_point) with
    | None -> t.entry_point := Some id
    | Some ep ->
        let ep_node = Hashtbl.find t.nodes ep in
        let dist = t.distance_fn vector ep_node.vector in
        for l = min level (Array.length ep_node.connections - 1) downto 0 do
          node.connections.(l) <- (ep, dist) :: node.connections.(l);
          ep_node.connections.(l) <- (id, dist) :: ep_node.connections.(l)
        done;
        if level > Array.length ep_node.connections - 1 then
          t.entry_point := Some id

  let search t query k =
    match !(t.entry_point) with
    | None -> []
    | Some ep ->
        let ep_node = Hashtbl.find t.nodes ep in
        let dist = t.distance_fn query ep_node.vector in
        let candidates = ref [(ep, dist)] in
        for _ = 1 to k - 1 do
          match !candidates with
          | (id, _) :: _ ->
              let node = Hashtbl.find t.nodes id in
              List.iter (fun (neighbor_id, _) ->
                let neighbor = Hashtbl.find t.nodes neighbor_id in
                let neighbor_dist = t.distance_fn query neighbor.vector in
                candidates := (neighbor_id, neighbor_dist) :: !candidates
              ) node.connections.(0);
              candidates := List.sort (fun (_, d1) (_, d2) -> compare d1 d2) !candidates
          | [] -> ()
        done;
        List.take k !candidates
end