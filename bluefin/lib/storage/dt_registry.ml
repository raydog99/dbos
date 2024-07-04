open Lwt.Infix
module BufferFrame = Buffer_frame

module ParentSwipHandler = struct
  type t = {
    swip : BufferFrame.t Swip.t;
    parent_guard : Guard.t;
    parent_bf : BufferFrame.t option;
    pos : int64;
    is_bf_updated : bool;
  }

  let get_parent_read_page_guard t a =
    HybridPageGuard.create (Guard.move t.parent_guard) t.parent_bf a
end

type space_check_result = Nothing | PickAnotherBf | RestartSameBf

module DTMeta = struct
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

module DTRegistry = struct
  type dt_type = int
  type dt_id = int64

  type t = {
    mutable instances_counter : int64;
    dt_types_ht : (dt_type, DTMeta.t) Hashtbl.t;
    dt_instances_ht : (dt_id, dt_type * 'a * string) Hashtbl.t;
    mutex : Mutex.t;
  }

  let create () = {
    instances_counter = 0L;
    dt_types_ht = Hashtbl.create 16;
    dt_instances_ht = Hashtbl.create 16;
    mutex = Mutex.create ();
  }

  let iterate_children_swips t dtid bf callback =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dtid in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.iterate_children root_obj bf callback

  let find_parent t dtid bf =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dtid in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.find_parent root_obj bf

  let check_space_utilization t dtid bf =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dtid in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.check_space_utilization root_obj bf

  let checkpoint t dtid bf dest =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dtid in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.checkpoint root_obj bf dest

  let register_datastructure_type t type_ dt_meta =
    Mutex.lock t.mutex;
    Hashtbl.replace t.dt_types_ht type_ dt_meta;
    Mutex.unlock t.mutex

  let register_datastructure_instance_with_id t type_ root_object name dt_id =
    Mutex.lock t.mutex;
    Hashtbl.add t.dt_instances_ht dt_id (type_, root_object, name);
    if dt_id >= t.instances_counter then
      t.instances_counter <- Int64.succ dt_id;
    Mutex.unlock t.mutex

  let register_datastructure_instance t type_ root_object name =
    Mutex.lock t.mutex;
    let new_instance_id = t.instances_counter in
    t.instances_counter <- Int64.succ t.instances_counter;
    Hashtbl.add t.dt_instances_ht new_instance_id (type_, root_object, name);
    Mutex.unlock t.mutex;
    new_instance_id

  let undo t dt_id wal_entry tts =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dt_id in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.undo root_obj wal_entry tts

  let todo t dt_id entry version_worker_id version_tx_id called_before =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dt_id in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.todo root_obj entry version_worker_id version_tx_id called_before

  let unlock t dt_id entry =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dt_id in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.unlock root_obj entry

  let serialize t dt_id =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dt_id in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.serialize root_obj

  let deserialize t dt_id map =
    let (dt_type, root_obj, _) = Hashtbl.find t.dt_instances_ht dt_id in
    let dt_meta = Hashtbl.find t.dt_types_ht dt_type in
    dt_meta.deserialize root_obj map

  let global_dt_registry = create ()
end