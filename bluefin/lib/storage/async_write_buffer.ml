module BufferFrame = struct
  type t = { mutable data : string }
  module Page = struct
    type t = { mutable content : string }
  end
end

type pid = int64
type u64 = int64

module AsyncWriteBuffer = struct
  type write_command = {
    mutable bf : BufferFrame.t;
    mutable pid : pid;
  }

  type t = {
    mutable aio_context : unit;
    fd : Unix.file_descr;
    page_size : u64;
    batch_max_size : u64;
    mutable pending_requests : u64;
    mutable write_buffer : BufferFrame.Page.t array;
    mutable write_buffer_commands : write_command array;
    mutable iocbs : unit;
    mutable iocbs_ptr : unit;
    mutable events : unit;
  }

  let create fd page_size batch_max_size =
    {
      aio_context = ();
      fd;
      page_size;
      batch_max_size;
      pending_requests = 0L;
      write_buffer = Array.make (Int64.to_int batch_max_size) { BufferFrame.Page.content = "" };
      write_buffer_commands = Array.make (Int64.to_int batch_max_size) { bf = { BufferFrame.data = "" }; pid = 0L };
      iocbs = ();
      iocbs_ptr = ();
      events = ();
    }

  let full t = t.pending_requests >= t.batch_max_size

  let add t bf pid =
    if not (full t) then begin
      t.write_buffer_commands.(Int64.to_int t.pending_requests) <- { bf; pid };
      t.pending_requests <- Int64.succ t.pending_requests
    end

  let submit t =
    0L

  let poll_events_sync t =
    0L

  let get_written_bfs t callback n_events =
    ()
end