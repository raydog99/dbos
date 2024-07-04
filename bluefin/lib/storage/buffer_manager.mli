module BufferFrame = Buffer_frame

module FreedBfsBatch : sig
  type t

  val create : unit -> t
  val reset : t -> unit
  val push : t -> Partition.t -> unit
  val size : t -> int64
  val add : t -> BufferFrame.t -> unit
end

module BufferManager : sig
  type t

  val create : Unix.file_descr -> t
  val random_partition : t -> Partition.t
  val random_buffer_frame : t -> BufferFrame.t
  val get_partition : t -> Pid.t -> Partition.t
  val get_partition_id : t -> Pid.t -> int64
  val allocate_page : t -> BufferFrame.t
  val resolve_swip : t -> Swip.guard -> Swip.t -> BufferFrame.t
  val evict_last_page : t -> unit
  val reclaim_page : t -> BufferFrame.t -> unit
  val read_page_sync : t -> Pid.t -> bytes -> unit
  val read_page_async : t -> Pid.t -> bytes -> (unit -> unit) -> unit Lwt.t
  val f_data_sync : t -> unit
  val start_background_threads : t -> unit
  val stop_background_threads : t -> unit
  val write_all_buffer_frames : t -> unit
  val serialize : t -> (string, string) Hashtbl.t
  val deserialize : t -> (string, string) Hashtbl.t -> unit
  val get_pool_size : t -> int64
  val consumed_pages : t -> int64
  val get_containing_buffer_frame : t -> bytes -> BufferFrame.t
end

module BMC : sig
  val global_bf : BufferManager.t option ref
end