open Lwt.Infix
module BufferFrame = Buffer_frame

type dt_type = int
type dt_id = int64

module ParentSwipHandler : sig
  type t = {
    swip : BufferFrame.t Swip.t;
    parent_guard : Guard.t;
    parent_bf : BufferFrame.t option;
    pos : int64;
    is_bf_updated : bool;
  }

  val get_parent_read_page_guard : t -> 'a -> 'a HybridPageGuard.t
end

type space_check_result = Nothing | PickAnotherBf | RestartSameBf

module DTMeta : sig
  type t = {
    iterate_children : 'a -> BufferFrame.t -> (BufferFrame.t Swip.t -> bool) -> unit;
    find_parent : 'a -> BufferFrame.t -> ParentSwipHandler.t;
    check_space_utilization : 'a -> BufferFrame.t -> space_check_result;
    checkpoint : 'a -> BufferFrame.t -> bytes -> unit;
    undo : 'a -> bytes -> int64 -> unit;
    todo : 'a -> bytes -> int64 -> int64 -> bool -> unit;
    unlock : 'a -> bytes -> unit;
    serialize : 'a -> (string, string) Hashtbl.t;
    deserialize : 'a -> (string, string) Hashtbl.t -> unit;
  }
end

module DTRegistry : sig
  type t

  val create : unit -> t

  val register_datastructure_type : t -> dt_type -> DTMeta.t -> unit

  val register_datastructure_instance : t -> dt_type -> 'a -> string -> dt_id

  val register_datastructure_instance_with_id : t -> dt_type -> 'a -> string -> dt_id -> unit

  val iterate_children_swips : t -> dt_id -> BufferFrame.t -> (BufferFrame.t Swip.t -> bool) -> unit

  val find_parent : t -> dt_id -> BufferFrame.t -> ParentSwipHandler.t

  val check_space_utilization : t -> dt_id -> BufferFrame.t -> space_check_result

  val checkpoint : t -> dt_id -> BufferFrame.t -> bytes -> unit

  val undo : t -> dt_id -> bytes -> int64 -> unit

  val todo : t -> dt_id -> bytes -> int64 -> int64 -> bool -> unit

  val unlock : t -> dt_id -> bytes -> unit

  val serialize : t -> dt_id -> (string, string) Hashtbl.t

  val deserialize : t -> dt_id -> (string, string) Hashtbl.t -> unit

  val global_dt_registry : t
end