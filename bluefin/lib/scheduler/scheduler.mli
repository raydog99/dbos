val num_cores : int
val scheduler_state : Types.scheduler_state
val execute_task : Types.task -> float
val worker_loop : int -> unit
val start_workers : unit -> unit
val add_query : int -> Types.task_set list -> unit
val set_finalization_function : Types.task_set -> (unit -> unit) -> unit