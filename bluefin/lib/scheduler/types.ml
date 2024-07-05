type slot = int

type morsel = {
  start_index: int;
  size: int;
  numa_node: int;
}

type pipeline_job = {
  pipeline_id: int;
  morsel: morsel;
}

type task_set = {
  mutable tasks: task list;
  mutable is_active: bool;
  mutable finalization_counter: int;
  mutable finalization_function: (unit -> unit) option;
}

type task = {
  mutable morsels: morsel list;
}

type worker_thread = {
  mutable current_task: task option;
  mutable active_slots: int array;
  mutable priorities: int array;
  mutable pass_values: float array;
  mutable global_pass: float;
  mutable change_mask: int64;
  mutable return_mask: int64;
  mutable current_slot: int;
  numa_node: int;
}

type resource_group = {
  mutable task_sets: task_set list;
  priority: int;
}

type scheduler_state = {
  mutable global_slot_array: (task_set * bool) option array;
  worker_threads: worker_thread array;
  mutable resource_groups: resource_group array;
  global_state_array: int array;
}

let finalization_marker = -1
let morsel_size = 100000