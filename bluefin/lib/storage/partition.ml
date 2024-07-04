open Lwt.Infix
module BufferFrame = Buffer_frame

type pid = int64

module IOFrame = struct
  type state = Reading | Ready | ToDelete | Undefined

  type t = {
    mutable state : state;
    mutable bf : BufferFrame.t option;
    mutable readers_counter : int64;
    mutex : Lwt_mutex.t;
  }

  let create () = {
    state = Undefined;
    bf = None;
    readers_counter = 0L;
    mutex = Lwt_mutex.create ();
  }

  let lock t = Lwt_mutex.lock t.mutex

  let unlock t = Lwt_mutex.unlock t.mutex
end

module HashTable = struct
  type entry = {
    mutable key : int64;
    mutable next : entry option;
    mutable value : IOFrame.t;
  }

  type t = {
    mutable entries : entry option array;
    mask : int64;
  }

  type handler = entry option ref

  let create size_in_bits =
    let size = 1 lsl size_in_bits in
    {
      entries = Array.make size None;
      mask = Int64.pred (Int64.shift_left 1L size_in_bits);
    }

  let hash_key t k = Int64.logand k t.mask

  let insert t key =
    let frame = IOFrame.create () in
    let rec insert_entry idx =
      match t.entries.(idx) with
      | None ->
          let new_entry = { key; next = None; value = frame } in
          t.entries.(idx) <- Some new_entry;
          frame
      | Some entry ->
          if entry.key = key then
            entry.value
          else
            match entry.next with
            | None ->
                let new_entry = { key; next = None; value = frame } in
                entry.next <- Some new_entry;
                frame
            | Some _ -> insert_entry (idx + 1)
    in
    insert_entry (Int64.to_int (hash_key t key))

  let lookup t key =
    let rec lookup_entry idx =
      match t.entries.(idx) with
      | None -> None
      | Some entry ->
          if entry.key = key then
            Some (ref (Some entry))
          else
            match entry.next with
            | None -> None
            | Some _ -> lookup_entry (idx + 1)
    in
    lookup_entry (Int64.to_int (hash_key t key))

  let remove t (handler : handler) =
    match !handler with
    | None -> ()
    | Some entry ->
        let idx = Int64.to_int (hash_key t entry.key) in
        if t.entries.(idx) == Some entry then
          t.entries.(idx) <- entry.next
        else
          let rec remove_from_list prev =
            match prev.next with
            | None -> ()
            | Some next ->
                if next == entry then
                  prev.next <- next.next
                else
                  remove_from_list next
          in
          match t.entries.(idx) with
          | None -> ()
          | Some first -> remove_from_list first;
        handler := None

  let remove_by_key t key =
    match lookup t key with
    | None -> ()
    | Some handler -> remove t handler

  let has t key =
    match lookup t key with
    | None -> false
    | Some _ -> true
end

module Partition = struct
  type t = {
    ht_mutex : Lwt_mutex.t;
    io_ht : HashTable.t;
    free_bfs_limit : int64;
    dram_free_list : FreeList.t;
    pid_distance : int64;
    pids_mutex : Lwt_mutex.t;
    mutable freed_pids : pid list;
    mutable next_pid : pid;
  }

  let create first_pid pid_distance free_bfs_limit =
    {
      ht_mutex = Lwt_mutex.create ();
      io_ht = HashTable.create 16;
      free_bfs_limit;
      dram_free_list = FreeList.create ();
      pid_distance;
      pids_mutex = Lwt_mutex.create ();
      freed_pids = [];
      next_pid = first_pid;
    }

  let next_pid t =
    Lwt_mutex.with_lock t.pids_mutex (fun () ->
      match t.freed_pids with
      | pid :: rest ->
          t.freed_pids <- rest;
          Lwt.return pid
      | [] ->
          let pid = t.next_pid in
          t.next_pid <- Int64.add t.next_pid t.pid_distance;
          Lwt.return pid
    )

  let free_page t pid =
    Lwt_mutex.with_lock t.pids_mutex (fun () ->
      t.freed_pids <- pid :: t.freed_pids;
      Lwt.return_unit
    )

  let allocated_pages t =
    Int64.div t.next_pid t.pid_distance

  let freed_pages t =
    Lwt_mutex.with_lock t.pids_mutex (fun () ->
      Lwt.return (Int64.of_int (List.length t.freed_pids))
    )

  let batch_push t head tail count =
    FreeList.batch_push t.dram_free_list head tail count
end