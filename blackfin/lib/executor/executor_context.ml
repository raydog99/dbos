open TransactionContext
open StorageManager

module ExecutorContext = struct
  type t = {
    transaction : TransactionContext;
    storage_manager : StorageManager;
  }

  type ThreadStates = {
    mutable num_threads : int;
    mutable state_size : int;
    mutable states : char option;
  }

  let create transaction = 
    let storage_manager = StorageManager.create () in
    {
      transaction;
      storage_manager;
      thread_states = {num_threads = 0; state_size = 0; states = None };
    }

  let get_transaction ctx = ctx.transaction

  let get_param_values ctx = Codegen.get_parameter_values ctx.parameters

  let get_storage_manager ctx = ctx.storage_manager

  let get_params ctx = ctx.parameters

  let get_pool ctx = ctx.thread_states.pool

  let get_thread_states ctx = ctx.thread_states

  module ThreadStates = struct
    let reset ts state_size =
      match ts.states with
      | Some states ->
          Type.free ts.pool states;
          ts.states <- None;
          ts.num_threads <- 0;
          let pad = state_size land (lnot 63) in
          ts.state_size <- state_size + (if pad != 0 then 64 - pad else 0)
      | None -> ()

    let allocate ts num_threads =
      assert (ts.state_size > 0);
      assert (ts.states = None);
      ts.num_threads <- num_threads;
      let alloc_size = ts.num_threads * ts.state_size in
      ts.states <- Some (Type.allocate ts.pool alloc_size);
      Peloton_util.memset ts.pool ts.states 0 alloc_size

    let access_thread_state ts thread_id =
      assert (ts.state_size > 0);
      assert (ts.states != None);
      assert (thread_id < ts.num_threads);
      match ts.states with
      | Some states ->
          let offset = thread_id * ts.state_size in
          CharVec.add_offset states offset
      | None -> failwith "Invalid thread states"
  end
end