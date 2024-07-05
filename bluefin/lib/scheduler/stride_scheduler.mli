open Types

val create_scheduler_state : int -> scheduler_state
val add_resource_group : scheduler_state -> int -> int
val add_task_set : scheduler_state -> int -> task_set -> unit
val pick_task : scheduler_state -> int -> task option
val update_pass : scheduler_state -> int -> int -> float -> unit
val start_finalization : scheduler_state -> int -> unit
val finalize_task_set : scheduler_state -> task_set -> unit
val finish_task : scheduler_state -> int -> unit
val add_pipeline_job : scheduler_state -> pipeline_job -> unit