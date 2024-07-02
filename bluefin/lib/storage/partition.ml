module BufferFrame = struct
  type t = { mutable data : string }
end

module FreeList = struct
  type t = { mutable free : BufferFrame.t list }
end

type pid = int64
type u64 = int64
type u8 = int
type s64 = int64

module Config = struct
  let flags_ssd_gib = ref 0
  let page_size = 4096
end

module IOFrame = struct
  type state = Reading | Ready | ToDelete | Undefined

  type t = {
    mutable state : state;
    mutable bf : BufferFrame.t option;
    readers_counter : s64 Atomic.t;
  }

  let create () = 
    { state = Undefined; bf = None; readers_counter = Atomic.make 0L }
end

module HashTable = struct
  type entry = {
    key : u64;
    mutable next : entry option;
    value : IOFrame.t;
  }

  type handler = {
    mutable holder : entry option ref;
  }

  type t = {
    mask : u64;
    entries : entry option array;
  }

  let create size_in_bits =
    let size = 1 lsl (Int64.to_int size_in_bits) in
    { mask = Int64.pred (Int64.shift_left 1L (Int64.to_int size_in_bits));
      entries = Array.make size None }

  let hash_key t k = Int64.logand k t.mask

  let insert t key =
    let hash = hash_key t key in
    let new_entry = { key; next = t.entries.(Int64.to_int hash); value = IOFrame.create () } in
    t.entries.(Int64.to_int hash) <- Some new_entry;
    new_entry.value

  let lookup t key =
    let hash = hash_key t key in
    let rec find_entry entry =
      match entry with
      | Some e when e.key = key -> Some (ref (Some e))
      | Some e -> find_entry e.next
      | None -> None
    in
    { holder = match find_entry t.entries.(Int64.to_int hash) with
               | Some r -> r
               | None -> ref None }

  let remove t handler =
    match !(handler.holder) with
    | Some entry ->
        let hash = hash_key t entry.key in
        let rec remove_entry prev curr =
          match curr with
          | Some e when e.key = entry.key ->
              (match prev with
               | Some p -> p.next <- e.next
               | None -> t.entries.(Int64.to_int hash) <- e.next)
          | Some e -> remove_entry (Some e) e.next
          | None -> ()
        in
        remove_entry None t.entries.(Int64.to_int hash);
        handler.holder := None
    | None -> ()

  let has t key =
    match lookup t key with
    | { holder = { contents = Some _ } } -> true
    | _ -> false
end

module Partition = struct
  type t = {
    io_ht : HashTable.t;
    free_bfs_limit : u64;
    dram_free_list : FreeList.t;
    pid_distance : u64;
    mutable freed_pids : pid list;
    mutable next_pid : u64;
  }

  let create first_pid pid_distance free_bfs_limit =
    { io_ht = HashTable.create 16L;
      free_bfs_limit;
      dram_free_list = { FreeList.free = [] };
      pid_distance;
      freed_pids = [];
      next_pid = first_pid }

  let next_pid t =
    match t.freed_pids with
    | pid :: rest ->
        t.freed_pids <- rest;
        pid
    | [] ->
        let pid = t.next_pid in
        t.next_pid <- Int64.add t.next_pid t.pid_distance;
        assert (Int64.div (Int64.mul pid (Int64.of_int Config.page_size)) 1073741824L <= Int64.of_int !(Config.flags_ssd_gib));
        pid

  let free_page t pid =
    t.freed_pids <- pid :: t.freed_pids

  let allocated_pages t =
    Int64.div t.next_pid t.pid_distance

  let freed_pages t =
    Int64.of_int (List.length t.freed_pids)
end