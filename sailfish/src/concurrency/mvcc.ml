open Types
open Latches

let global_lock = HybridLock.create ()

let create_version_vector size = Array.make size None

let rec get_visible_version transaction_id start_time = function
  | None -> None
  | Some v ->
      if v.pred = None || v.timestamp = transaction_id || v.timestamp < start_time then
        Some v.balance
      else
        get_visible_version transaction_id start_time v.pred

let read_version transaction vector index =
  HybridLock.read_with_fallback global_lock (fun () ->
    get_visible_version transaction.id transaction.start_time vector.(index)
  )

let update_version transaction vector index new_balance =
  HybridLock.lock global_lock;
  let current_version = vector.(index) in
  let new_version = {
    balance = new_balance;
    timestamp = transaction.id;
    pred = current_version;
  } in
  vector.(index) <- Some new_version;
  transaction.undo_buffer.deltas <- (transaction.id, new_version) :: transaction.undo_buffer.deltas;
  HybridLock.unlock global_lock

let create_versioned_positions size =
  { positions = Array.make (size / 1024 + 1) (-1, -1) }

let update_versioned_positions vp index =
  let array_index = index / 1024 in
  let first, last = vp.positions.(array_index) in
  if first = -1 || index < first then
    vp.positions.(array_index) <- (index, max last index)
  else if index > last then
    vp.positions.(array_index) <- (first, index)

let get_scan_ranges vp =
  Array.mapi (fun i (first, last) ->
    if first = -1 then
      { start = i * 1024; end_ = (i + 1) * 1024 - 1 }
    else
      { start = first; end_ = last }
  ) vp.positions

let scan_records transaction vector vp f =
  let ranges = get_scan_ranges vp in
  Array.iter (fun range ->
    for i = range.start to range.end_ do
      match read_version transaction vector i with
      | Some balance -> f i balance
      | None -> ()
    done
  ) ranges