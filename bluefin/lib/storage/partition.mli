open Lwt.Infix
module BufferFrame = Buffer_frame

type pid = int64

module IOFrame : sig
  type state = Reading | Ready | ToDelete | Undefined

  type t = {
    mutable state : state;
    mutable bf : BufferFrame.t option;
    mutable readers_counter : int64;
  }

  val create : unit -> t
  val lock : t -> unit Lwt.t
  val unlock : t -> unit
end

module HashTable : sig
  type t
  type handler

  val create : int -> t
  val insert : t -> int64 -> IOFrame.t
  val lookup : t -> int64 -> handler option
  val remove : t -> handler -> unit
  val remove_by_key : t -> int64 -> unit
  val has : t -> int64 -> bool
end

module Partition : sig
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

  val create : pid -> int64 -> int64 -> t

  val next_pid : t -> pid Lwt.t

  val free_page : t -> pid -> unit Lwt.t

  val allocated_pages : t -> int64

  val freed_pages : t -> int64 Lwt.t

  val batch_push : t -> BufferFrame.t option -> BufferFrame.t option -> int64 -> unit
end