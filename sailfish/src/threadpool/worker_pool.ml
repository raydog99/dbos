open Latches
open Thread
open Mutex
open Condition

type task = unit -> unit

module TaskQueue = struct
  type t = {
    queue: task Queue.t;
    mutex: Mutex.t;
    condition: Condition.t;
  }

  let create () = {
    queue = Queue.create ();
    mutex = Mutex.create ();
    condition = Condition.create ();
  }

  let enqueue t task =
    Mutex.lock t.mutex;
    Queue.add task t.queue;
    Condition.signal t.condition;
    Mutex.unlock t.mutex

  let dequeue t =
    Mutex.lock t.mutex;
    let task = 
      if Queue.is_empty t.queue then begin
        Condition.wait t.condition t.mutex;
        if Queue.is_empty t.queue then None
        else Some (Queue.take t.queue)
      end else Some (Queue.take t.queue)
    in
    Mutex.unlock t.mutex;
    task

  let is_empty t =
    Mutex.lock t.mutex;
    let empty = Queue.is_empty t.queue in
    Mutex.unlock t.mutex;
    empty
end

type t = {
  pool_name: string;
  num_workers: int;
  task_queue: TaskQueue.t;
  mutable is_running: bool;
  workers: Thread.t list ref;
}

let create pool_name num_workers =
  {
    pool_name;
    num_workers;
    task_queue = TaskQueue.create ();
    is_running = false;
    workers = ref [];
  }

let worker_func pool_name is_running task_queue =
  let rec loop pause_time =
    if !is_running || not (TaskQueue.is_empty task_queue) then
      match TaskQueue.dequeue task_queue with
      | Some task ->
          task ();
          loop 0.001
      | None ->
          Thread.delay pause_time;
          loop (min (pause_time *. 2.) 1.)
  in
  loop 0.001

let startup t =
  if not t.is_running then begin
    t.is_running <- true;
    for i = 0 to t.num_workers - 1 do
      let name = t.pool_name ^ "-worker-" ^ string_of_int i in
      let worker = Thread.create (worker_func name (ref t.is_running) t.task_queue) () in
      t.workers := worker :: !(t.workers)
    done
  end

let shutdown t =
  if t.is_running then begin
    t.is_running <- false;
    List.iter Thread.join !(t.workers);
    t.workers := []
  end

let submit t task = TaskQueue.enqueue t.task_queue task