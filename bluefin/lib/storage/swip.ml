module BufferFrame = Buffer_frame

type pid = int64

let evicted_bit = Int64.shift_left 1L 63
let evicted_mask = Int64.lognot evicted_bit
let cool_bit = Int64.shift_left 1L 62
let cool_mask = Int64.lognot cool_bit
let hot_mask = Int64.lognot (Int64.logor evicted_bit cool_bit)

type 'a t = {
  mutable value: int64;
}

let create () = { value = 0L }

let of_buffer_frame bf =
  { value = Int64.of_nativeint (Nativeint.of_int (Obj.magic bf)) }

let of_swip other = { value = other.value }

let equal a b = a.value = b.value

let is_hot t = Int64.logand t.value (Int64.logor evicted_bit cool_bit) = 0L
let is_cool t = Int64.logand t.value cool_bit <> 0L
let is_evicted t = Int64.logand t.value evicted_bit <> 0L

let as_page_id t = Int64.logand t.value evicted_mask

let as_buffer_frame t =
  Obj.magic (Int64.to_nativeint t.value)

let as_buffer_frame_masked t =
  Obj.magic (Int64.to_nativeint (Int64.logand t.value hot_mask))

let raw t = t.value

let warm t bf =
  t.value <- Int64.of_nativeint (Nativeint.of_int (Obj.magic bf))

let warm_in_place t =
  assert (is_cool t);
  t.value <- Int64.logand t.value (Int64.lognot cool_bit)

let cool t =
  t.value <- Int64.logor t.value cool_bit

let evict t pid =
  t.value <- Int64.logor pid evicted_bit

let cast t = t