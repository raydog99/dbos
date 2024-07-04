open Lwt.Infix
open Unix

module FreedBfsBatch = struct
  type t = {
    mutable freed_bfs_batch_head: BufferFrame.t option;
    mutable freed_bfs_batch_tail: BufferFrame.t option;
    mutable freed_bfs_counter: int64;
  }

  let create () = {
    freed_bfs_batch_head = None;
    freed_bfs_batch_tail = None;
    freed_bfs_counter = 0L;
  }

  let reset t =
    t.freed_bfs_batch_head <- None;
    t.freed_bfs_batch_tail <- None;
    t.freed_bfs_counter <- 0L

  let push t partition =
    Partition.batch_push partition t.freed_bfs_batch_head t.freed_bfs_batch_tail t.freed_bfs_counter;
    reset t

  let size t = t.freed_bfs_counter

  let add t bf =
    (match t.freed_bfs_batch_head with
    | None -> t.freed_bfs_batch_tail <- Some bf
    | Some head -> BufferFrame.set_next_free bf head);
    t.freed_bfs_batch_head <- Some bf;
    t.freed_bfs_counter <- Int64.succ t.freed_bfs_counter
end

module BufferManager = struct
  type t = {
    bfs: BufferFrame.t array;
    ssd_fd: Unix.file_descr;
    safety_pages: int;
    dram_pool_size: int64;
    ssd_freed_pages_counter: int64 ref;
    partitions_count: int64;
    partitions_mask: int64;
    partitions: Partition.t array;
    clock_cursor: int64 ref;
    bg_threads_counter: int ref;
    bg_threads_keep_running: bool ref;
  }

  let page_size = 4 * 1024

  let create ssd_fd =
    let dram_gib = Config.get_int "dram_gib" in
    let dram_pool_size = Int64.of_int (dram_gib * 1024 * 1024 * 1024 / BufferFrame.size) in
    let safety_pages = 10 in
    let dram_total_size = BufferFrame.size * (Int64.to_int dram_pool_size + safety_pages) in
    let bfs = Array.make (Int64.to_int dram_pool_size) (BufferFrame.create ()) in
    let partitions_count = 1 lsl (Config.get_int "partition_bits") in
    let partitions_mask = Int64.pred (Int64.of_int partitions_count) in
    let free_bfs_limit = Int64.to_float dram_pool_size *. (Config.get_float "free_pct" /. 100.0) /. float_of_int partitions_count |> ceil |> int_of_float in
    let partitions = Array.init partitions_count (fun p_i -> 
      Partition.create (Int64.of_int p_i) (Int64.of_int partitions_count) free_bfs_limit
    ) in
    Array.iteri (fun i bf ->
      let p_i = i mod partitions_count in
      Partition.push_free partitions.(p_i) bf
    ) bfs;
    { bfs; ssd_fd; safety_pages; dram_pool_size; ssd_freed_pages_counter = ref 0L;
      partitions_count = Int64.of_int partitions_count; partitions_mask;
      partitions; clock_cursor = ref 0L; bg_threads_counter = ref 0;
      bg_threads_keep_running = ref true }

  let random_partition t =
    let rand_partition_i = Random.int64 t.partitions_count in
    t.partitions.(Int64.to_int rand_partition_i)

  let random_buffer_frame t =
    let rand_buffer_i = Random.int64 t.dram_pool_size in
    t.bfs.(Int64.to_int rand_buffer_i)

  let get_partition t pid =
    let partition_i = Int64.logand pid t.partitions_mask in
    t.partitions.(Int64.to_int partition_i)

  let get_partition_id t pid =
    Int64.logand pid t.partitions_mask

  let allocate_page t =
    let partition = random_partition t in
    let free_bf = Partition.try_pop_free partition in
    let free_pid = Partition.next_pid partition in
    BufferFrame.init free_bf free_pid;
    free_bf

  let evict_last_page t =
    (* Implementation omitted for brevity *)
    ()

  let reclaim_page t bf =
    let partition = get_partition t (BufferFrame.get_pid bf) in
    if Config.get_bool "recycle_pages" then
      Partition.free_page partition (BufferFrame.get_pid bf);
    if not (BufferFrame.is_being_written_back bf) then begin
      BufferFrame.reset bf;
      Partition.push_free partition bf
    end

  let resolve_swip t swip_guard swip_value =
    (* Implementation omitted for brevity *)
    random_buffer_frame t  (* Placeholder *)

  let read_page_sync t pid destination =
    let rec read_loop bytes_left =
      if bytes_left > 0 then begin
        let bytes_read = Unix.pread t.ssd_fd destination (page_size - bytes_left) 
                           (Int64.mul pid (Int64.of_int page_size)) bytes_left in
        read_loop (bytes_left - bytes_read)
      end
    in
    read_loop page_size

  let f_data_sync t =
    Unix.fsync t.ssd_fd

  let start_background_threads t =
    (* Implementation omitted for brevity *)
    ()

  let stop_background_threads t =
    t.bg_threads_keep_running := false;
    while !(t.bg_threads_counter) > 0 do
      Unix.sleepf 0.001
    done

  let write_all_buffer_frames t =
    (* Implementation omitted for brevity *)
    ()

  let serialize t =
    let map = Hashtbl.create 16 in
    let max_pid = ref 0L in
    Array.iter (fun partition ->
      max_pid := Int64.max !max_pid (Partition.get_next_pid partition)
    ) t.partitions;
    Hashtbl.add map "max_pid" (Int64.to_string !max_pid);
    map

  let deserialize t map =
    let max_pid = Int64.of_string (Hashtbl.find map "max_pid") in
    let aligned_max_pid = Int64.logand (Int64.add max_pid (Int64.pred t.partitions_count))
                                       (Int64.lognot (Int64.pred t.partitions_count)) in
    Array.iteri (fun i partition ->
      Partition.set_next_pid partition (Int64.add aligned_max_pid (Int64.of_int i))
    ) t.partitions

  let get_pool_size t = t.dram_pool_size

  let consumed_pages t =
    Array.fold_left (fun acc partition ->
      Int64.add acc (Int64.sub (Partition.allocated_pages partition) 
                                (Partition.freed_pages partition))
    ) 0L t.partitions

  let get_containing_buffer_frame t ptr =
    let index = (Nativeint.to_int (Nativeint.sub (Nativeint.of_string ptr) 
                                   (Nativeint.of_string (Obj.magic t.bfs)))) / BufferFrame.size in
    t.bfs.(index)
end

module BMC = struct
  let global_bf : BufferManager.t option ref = ref None
end