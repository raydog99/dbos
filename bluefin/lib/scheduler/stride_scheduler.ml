open Types

let max_slots = 128

let create_scheduler_state num_workers =
  {
    global_slot_array = Array.make max_slots None;
    worker_threads = Array.init num_workers (fun _ -> {
      current_task = None;
      active_slots = Array.make max_slots 0;
      priorities = Array.make max_slots 1;
      pass_values = Array.make max_slots 0.0;
      global_pass = 0.0;
      change_mask = 0L;
      return_mask = 0L;
      current_slot = -1;
    });
    resource_groups = [||];
    global_state_array = Array.make num_workers (-1);
  }

let start_finalization scheduler slot =
  match scheduler.global_slot_array.(slot) with
  | Some (task_set, _) ->
      scheduler.global_slot_array.(slot) <- Some (task_set, false);
      let finalization_count = ref 0 in
      Array.iteri (fun i state ->
        if state = slot then
          begin
            scheduler.global_state_array.(i) <- finalization_marker;
            incr finalization_count
          end
      ) scheduler.global_state_array;
      task_set.finalization_counter <- !finalization_count
  | None -> ()

let finalize_task_set scheduler task_set =
  match task_set.finalization_function with
  | Some f -> f ()
  | None -> ()

let calculate_stride priority =
  1.0 /. float_of_int priority

let add_resource_group scheduler priority =
  let rg = { task_sets = []; priority } in
  let index = Array.length scheduler.resource_groups in
  scheduler.resource_groups <- Array.append scheduler.resource_groups [|rg|];
  index


let update_bitmasks scheduler slot is_new_resource_group =
  Array.iter (fun worker ->
    if is_new_resource_group then
      worker.change_mask <- Int64.logor worker.change_mask (Int64.shift_left 1L slot)
    else
      worker.return_mask <- Int64.logor worker.return_mask (Int64.shift_left 1L slot)
  ) scheduler.worker_threads

let add_task_set scheduler rg_index task_set =
  let rg = scheduler.resource_groups.(rg_index) in
  rg.task_sets <- task_set :: rg.task_sets;
  if task_set.is_active then
    let slot = Array.length scheduler.global_slot_array in
    scheduler.global_slot_array.(slot) <- Some (task_set, true);
    update_bitmasks scheduler slot (List.length rg.task_sets = 1)

let sync_worker_state scheduler worker_id =
  let worker = scheduler.worker_threads.(worker_id) in
  let process_mask mask is_change =
    let rec loop mask index =
      if mask = 0L then ()
      else if Int64.logand mask 1L <> 0L then
        begin
          worker.active_slots.(index) <- 1;
          if is_change then
            begin
              let rg = scheduler.resource_groups.(index) in
              worker.priorities.(index) <- rg.priority;
              worker.pass_values.(index) <- worker.global_pass;
            end
          else
            worker.pass_values.(index) <- worker.global_pass;
          loop (Int64.shift_right_logical mask 1) (index + 1)
        end
      else
        loop (Int64.shift_right_logical mask 1) (index + 1)
    in
    loop mask 0
  in
  let change_mask = Int64.atomic_exchange worker.change_mask 0L in
  let return_mask = Int64.atomic_exchange worker.return_mask 0L in
  process_mask change_mask true;
  process_mask return_mask false

let update_pass scheduler worker_id slot execution_time =
  let worker = scheduler.worker_threads.(worker_id) in
  let stride = calculate_stride worker.priorities.(slot) in
  worker.pass_values.(slot) <- worker.pass_values.(slot) +. stride *. execution_time;
  worker.global_pass <- worker.global_pass +. stride *. execution_time

let pick_task scheduler worker_id =
  sync_worker_state scheduler worker_id;
  let worker = scheduler.worker_threads.(worker_id) in
  let min_pass = ref Float.infinity in
  let selected_slot = ref (-1) in
  for slot = 0 to max_slots - 1 do
    if worker.active_slots.(slot) = 1 && worker.pass_values.(slot) < !min_pass then begin
      min_pass := worker.pass_values.(slot);
      selected_slot := slot
    end
  done;
  if !selected_slot <> -1 then
    begin
      worker.current_slot <- !selected_slot;
      scheduler.global_state_array.(worker_id) <- !selected_slot;
      match scheduler.global_slot_array.(!selected_slot) with
      | Some (task_set, is_valid) ->
          if is_valid then
            (match task_set.tasks with
             | task :: rest ->
                 task_set.tasks <- rest;
                 Some task
             | [] ->
                 start_finalization scheduler !selected_slot;
                 None)
          else
            (worker.active_slots.(!selected_slot) <- 0; None)
      | None -> None
    end
  else
    None

let finish_task scheduler worker_id =
  let worker = scheduler.worker_threads.(worker_id) in
  if scheduler.global_state_array.(worker_id) = finalization_marker then
    match scheduler.global_slot_array.(worker.current_slot) with
    | Some (task_set, _) ->
        task_set.finalization_counter <- task_set.finalization_counter - 1;
        if task_set.finalization_counter = 0 then
          finalize_task_set scheduler task_set
    | None -> ()