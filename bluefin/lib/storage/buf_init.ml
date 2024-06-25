module type Buffer = sig
  type t

  val create : int -> t
  val reset : t -> unit
  val len : t -> int
  val sub : t -> int -> int -> t
end

module Make (B : Buffer) : sig
  type t
  type buffer = B.t
  val alloc : t -> int -> B.t
  val release : t -> buffer -> unit
  val make : unit -> t
end = struct
  module M = Map.Make (struct
    type t = int
    let compare = compare
  end)

  type buffer = B.t
  type t = { mutable available : buffer list M.t }

  let make () = { available = M.empty }

  let pop m i f =
    match M.find_opt i m with
    | Some [] -> raise @@ Invalid_argument "Empty list in map"
    | Some [b] ->
        M.remove i m, b
    | Some (b :: bs) ->
        let m' = M.remove i m |> M.add i bs in
        m', b
    | None -> m, f ()

  let push m v =
    let i = B.len v in
    match M.find_opt i m with
    | Some bs -> M.add i (v :: bs) m
    | None -> M.add i [v] m

  let alloc t size =
    let default () = B.create size in
    let i =
      match M.find_first_opt (fun i -> i >= size) t.available with
      | Some (i, _) -> if i < 2 * size then i else size
      | None -> size
    in
    let avail', b = pop t.available i default in
    t.available <- avail';
    B.reset b;
    b

  let release t buf = t.available <- push t.available buf
end

module BufferPoolManager = struct
  type buffer_tag = { mutable id : int }
  type buffer_state = int
  type buffer_desc = {
    mutable tag : buffer_tag;
    mutable state : buffer_state;
    mutable wait_backend_pgprocno : int;
    mutable buf_id : int;
    mutable free_next : int;
  }
  type buffer_desc_padded = buffer_desc

  let invalid_proc_number = -1
  let n_buffers = 1024
  let pg_io_align_size = 8192
  let blcksz = 8192

  let buffer_descriptors = Array.make n_buffers {
    tag = { id = 0 };
    state = 0;
    wait_backend_pgprocno = invalid_proc_number;
    buf_id = 0;
    free_next = 0;
  }
  let buffer_blocks = Bytes.create (n_buffers * blcksz)
  let buffer_io_cv_array = Array.make n_buffers (Condition.make ())
  let ckpt_buffer_ids = Array.make n_buffers 0

  let init_buffer_pool () =
    Array.iteri (fun i buf ->
      buf.tag <- { id = 0 };
      buf.state <- 0;
      buf.wait_backend_pgprocno <- invalid_proc_number;
      buf.buf_id <- i;
      buf.free_next <- i + 1;
    ) buffer_descriptors;
    buffer_descriptors.(n_buffers - 1).free_next <- -1;
    Strategy.initialize ();
    WritebackContext.init ()
  
  let buffer_shmem_size () =
    let size = ref 0 in
    size := !size + (n_buffers * (Obj.sizeof buffer_descriptors.(0)));
    size := !size + pg_io_align_size;
    size := !size + (n_buffers * blcksz);
    size := !size + Strategy.shmem_size ();
    size := !size + (n_buffers * (Obj.sizeof (Condition.make ())));
    size := !size + pg_io_align_size;
    size := !size + (n_buffers * (Obj.sizeof 0));
    !size
end

module Strategy = struct
  let initialize () = ()
  let shmem_size () = 1024
end

module WritebackContext = struct
  let init () = ()
end