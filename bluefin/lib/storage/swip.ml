type pid = int64
type u64 = int64

module BufferFrame = struct
  type t = { mutable data : string }
end

module Swip = struct
  type 'a t = {
    mutable value : u64;
  }

  let evicted_bit = Int64.shift_left 1L 63
  let evicted_mask = Int64.lognot (Int64.shift_left 1L 63)
  let cool_bit = Int64.shift_left 1L 62
  let cool_mask = Int64.lognot (Int64.shift_left 1L 62)
  let hot_mask = Int64.lognot (Int64.shift_left 3L 62)

  let create_from_bf bf =
    { value = Int64.of_int (Obj.magic bf : int) }

  let create_from_pid pid =
    { value = pid }

  let is_hot t =
    Int64.logand t.value (Int64.logor evicted_bit cool_bit) = 0L

  let is_cool t =
    Int64.logand t.value cool_bit <> 0L

  let is_evicted t =
    Int64.logand t.value evicted_bit <> 0L

  let as_page_id t =
    Int64.logand t.value evicted_mask

  let as_buffer_frame t =
    Obj.magic (Int64.to_int t.value) : BufferFrame.t

  let as_buffer_frame_masked t =
    Obj.magic (Int64.to_int (Int64.logand t.value hot_mask)) : BufferFrame.t

  let raw t =
    t.value

  let warm t bf =
    t.value <- Int64.of_int (Obj.magic bf : int)

  let warm_cool t =
    assert (is_cool t);
    t.value <- Int64.logand t.value (Int64.lognot cool_bit)

  let cool t =
    t.value <- Int64.logor t.value cool_bit

  let evict t pid =
    t.value <- Int64.logor pid evicted_bit

  let cast (t : 'a t) : 'b t =
    Obj.magic t
end