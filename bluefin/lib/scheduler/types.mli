type morsel = {
  start_index: int;
  size: int;
  numa_node: int;
}

type pipeline_job = {
  pipeline_id: int;
  morsel: morsel;
}

type task_set
type task
type resource_group
type worker_thread
type scheduler_state

val finalization_marker : int
val morsel_size : int