module ExecutorContext : sig
  type t

  type thread_states = {
    mutable num_threads : int;
    mutable state_size : int;
    mutable states : char option;
  }

  val create : TransactionContext.t -> t

  val get_transaction : t -> TransactionContext.t

  val get_param_values : t -> type_.codegen.QueryParameters.t -> type_.type_.value list

  val get_storage_manager : t -> StorageManager.t

  val get_params : t -> type_.codegen.QueryParameters.t

  val get_pool : t -> type_.type_.ephemeralPool

  val get_thread_states : t -> thread_states

  module ThreadStates : sig
    val reset : thread_states -> int -> unit

    val allocate : thread_states -> int -> unit

    val access_thread_state : thread_states -> int -> char option
  end
end