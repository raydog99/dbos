module BufferFrame = Buffer_frame

type pid = int64
type 'a t

val create : unit -> 'a t
val of_buffer_frame : BufferFrame.t -> 'a t
val of_swip : 'b t -> 'a t

val equal : 'a t -> 'a t -> bool

val is_hot : 'a t -> bool
val is_cool : 'a t -> bool
val is_evicted : 'a t -> bool

val as_page_id : 'a t -> pid
val as_buffer_frame : 'a t -> BufferFrame.t
val as_buffer_frame_masked : 'a t -> BufferFrame.t
val raw : 'a t -> int64

val warm : 'a t -> BufferFrame.t -> unit
val warm_in_place : 'a t -> unit
val cool : 'a t -> unit
val evict : 'a t -> pid -> unit

val cast : 'a t -> 'b t