open Lwt.Infix
open Unix

module AsyncWriteBuffer = struct
  type write_command = {
    mutable bf: BufferFrame.t;
    mutable pid: int64;
  }

  type t = {
    fd: Unix.file_descr;
    page_size: int;
    batch_max_size: int;
    mutable pending_requests: int;
    write_buffer: BufferFrame.Page.t array;
    write_buffer_commands: write_command array;
    mutable aio_context: Lwt_unix.io_context;
  }

  let create fd page_size batch_max_size =
    let write_buffer = Array.make batch_max_size (BufferFrame.Page.create ()) in
    let write_buffer_commands = Array.make batch_max_size { bf = BufferFrame.create (); pid = 0L } in
    let aio_context = Lwt_unix.init_io_context batch_max_size in
    { fd; page_size; batch_max_size; pending_requests = 0; 
      write_buffer; write_buffer_commands; aio_context }

  let is_full t =
    t.pending_requests >= t.batch_max_size - 2

  let add t bf pid =
    assert (not (is_full t));
    assert (t.pending_requests <= t.batch_max_size);
    WorkerCounters.increment_dt_page_writes bf.page.dt_id;
    let slot = t.pending_requests in
    t.pending_requests <- t.pending_requests + 1;
    t.write_buffer_commands.(slot) <- { bf; pid };
    bf.page.magic_debugging_number <- pid;
    BufferFrame.Page.copy bf.page t.write_buffer.(slot);
    let write_buffer_slot_ptr = Bigarray.Array1.sub t.write_buffer.(slot) 0 t.page_size in
    Lwt_unix.prepare_write t.aio_context t.fd (Int64.mul pid (Int64.of_int t.page_size)) write_buffer_slot_ptr

  let submit t =
    if t.pending_requests > 0 then
      Lwt_unix.submit t.aio_context
      >|= fun submitted ->
      assert (submitted = t.pending_requests);
      t.pending_requests
    else
      Lwt.return 0

  let poll_events_sync t =
    if t.pending_requests > 0 then
      Lwt_unix.get_events t.aio_context t.pending_requests
      >|= fun events ->
      assert (List.length events = t.pending_requests);
      t.pending_requests <- 0;
      List.length events
    else
      Lwt.return 0

  let get_written_bfs t callback n_events =
    let process_event i event =
      let slot = i in
      assert (Lwt_unix.Event.result event = t.page_size);
      let written_lsn = t.write_buffer.(slot).plsn in
      callback t.write_buffer_commands.(slot).bf written_lsn t.write_buffer_commands.(slot).pid
    in
    Lwt_list.iteri_p process_event (Lwt_unix.get_events t.aio_context n_events)

  let debug_info t =
    Printf.sprintf "AsyncWriteBuffer: pending_requests=%d, batch_max_size=%d"
      t.pending_requests t.batch_max_size
end