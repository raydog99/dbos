type transaction_id = int64
type timestamp = int64
type balance = int

type account = {
  owner: string;
  mutable balance: balance;
}

type version_delta = {
  balance: balance;
  mutable timestamp: timestamp;
  pred: version_delta option;
}

type undo_buffer = {
  mutable deltas: (transaction_id * version_delta) list;
}

type version_vector = version_delta option array

type transaction_status = Active | Committed | Aborted

type transaction = {
  id: transaction_id;
  start_time: timestamp;
  mutable commit_time: timestamp option;
  mutable status: transaction_status;
  undo_buffer: undo_buffer;
}

type recently_committed = {
  mutable transactions: transaction list;
}

type attribute = Balance | Owner

type predicate = 
  | Equals of attribute * int
  | Between of attribute * int * int
  | And of predicate * predicate
  | Or of predicate * predicate

type predicate_log = {
  mutable predicates: predicate list;
  mutable accessed_attributes: attribute list;
}

type versioned_positions = {
  mutable positions: (int * int) array;
}

type scan_range = {
  start: int;
  end_: int;
}