open Types
open Stride_scheduler

let num_cores = 8

let scheduler_state = create_scheduler_state num_cores

let execute_task task =
  0.001

let worker_loop worker_id =
  while true do
    match pick_task scheduler_state worker_id with
    | Some task ->
        let execution_time = execute_task task in
        update_pass scheduler_state worker_id scheduler_state.worker_threads.(worker_id).current_slot execution_time;
        finish_task scheduler_state worker_id
    | None ->
        Thread.yield ()
  done

let start_workers () =
  for i = 0 to num_cores - 1 do
    ignore (Thread.create worker_loop i)
  done

let add_query priority task_sets =
  let rg_index = add_resource_group scheduler_state priority in
  List.iter (fun ts -> add_task_set scheduler_state rg_index ts) task_sets

let set_finalization_function task_set f =
  task_set.finalization_function <- Some f