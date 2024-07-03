module VersionChain = struct
  type t = Version.t option ref

  let create () = ref None

  let add_version chain version =
    match !chain with
    | None -> chain := Some version
    | Some current_version ->
        chain := Some (Version.create 
          (Version.get_data version)
          (Version.get_timestamp version)
          (Version.get_txn_id version)
          (Some current_version))

  let get_latest_version chain = !chain

  let find_version chain timestamp txn_id =
    let rec find version =
      match version with
      | None -> None
      | Some v ->
          if Version.get_timestamp v <= timestamp || Version.get_txn_id v = txn_id then
            Some v
          else
            find (Version.get_prev_version v)
    in
    find !chain
end