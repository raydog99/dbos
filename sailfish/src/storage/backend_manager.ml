open Core

type backend_type =
  | MM
  | NVM
  | SSD
  | HDD

let peloton_data_file_size = ref 0

type t = {
  mutable data_file_address: char option;
  mutable data_file_len: int;
  mutable data_file_offset: int;
  mutable allocation_count: int;
  data_file_spinlock: Mutex.t;
}

let create () = {
  data_file_address = None;
  data_file_len = 0;
  data_file_offset = 0;
  allocation_count = 0;
  data_file_spinlock = Mutex.create ();
}

let instance = lazy (create ())

let get_instance () =
  Lazy.force instance

let allocate t backend_type size =
  t.allocation_count <- t.allocation_count + 1;
  match backend_type with
  | BackendType.MM | BackendType.NVM ->
      Stdlib.Gc.allocated_bytes () |> int_of_float |> Bytes.create
  | BackendType.SSD | BackendType.HDD ->
      Mutex.lock t.data_file_spinlock;
      let result =
        if t.data_file_offset < t.data_file_len then
          let cache_data_file_offset = t.data_file_offset in
          t.data_file_offset <- t.data_file_offset + size;
          Mutex.unlock t.data_file_spinlock;
          match t.data_file_address with
          | Some addr -> 
              Bytes.sub (Bytes.of_string addr) cache_data_file_offset size
          | None -> 
              failwith "Data file not initialized"
        else
          failwith (Printf.sprintf "No more memory available: offset: %d, length: %d" 
                      t.data_file_offset t.data_file_len)
      in
      result
  | BackendType.INVALID ->
      failwith "Invalid backend type"

let release _t _backend_type _address =
  ()

let sync _t _backend_type _address _length =
  ()