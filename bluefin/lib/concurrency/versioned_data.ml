module VersionedData = struct
  type t = (string, VersionChain.t) Hashtbl.t

  let create () = Hashtbl.create 100

  let add_or_update t key version =
    match Hashtbl.find_opt t key with
    | Some chain -> VersionChain.add_version chain version
    | None ->
        let chain = VersionChain.create () in
        VersionChain.add_version chain version;
        Hashtbl.add t key chain

  let get_version t key timestamp txn_id =
    match Hashtbl.find_opt t key with
    | Some chain -> VersionChain.find_version chain timestamp txn_id
    | None -> None
end