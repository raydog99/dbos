type dtid = int

module Config = struct
  type t = {
    use_bulk_insert: bool;
    enable_wal: bool;
  }
end

type op_result = 
  | OK
  | NOT_FOUND
  | DUPLICATE
  | NOT_ENOUGH_SPACE
  | OTHER

type slice = {
  data: string;
  length: int;
}

type mutable_slice = {
  mutable data: string;
  mutable length: int;
}

type swip_type = int64

type separator_info = {
  length: int;
  slot: int;
  trunc: bool;
}