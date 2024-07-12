open Types
open Latches

let create_predicate_log () = { predicates = []; accessed_attributes = [] }

let log_predicate log pred =
  log.predicates <- pred :: log.predicates

let log_accessed_attribute log attr =
  if not (List.mem attr log.accessed_attributes) then
    log.accessed_attributes <- attr :: log.accessed_attributes

let rec evaluate_predicate pred account =
  match pred with
  | Equals (attr, value) -> 
      (match attr with
       | Balance -> account.balance = value
       | Owner -> int_of_string account.owner = value)
  | Between (attr, low, high) ->
      (match attr with
       | Balance -> account.balance >= low && account.balance <= high
       | Owner -> let owner_int = int_of_string account.owner in owner_int >= low && owner_int <= high)
  | And (p1, p2) -> evaluate_predicate p1 account && evaluate_predicate p2 account
  | Or (p1, p2) -> evaluate_predicate p1 account || evaluate_predicate p2 account

let validate_transaction transaction predicate_log recently_committed =
  HybridLock.lock global_lock;
  let result = 
    let validate_undo_buffer undo_buffer =
      List.exists (fun (_, version) ->
        let account = { owner = ""; balance = version.balance } in
        List.exists (fun pred -> evaluate_predicate pred account) predicate_log.predicates &&
        List.exists (fun attr -> List.mem attr predicate_log.accessed_attributes) [Balance; Owner]
      ) undo_buffer.deltas
    in
    
    not (List.exists (fun t -> 
      t.start_time < transaction.start_time && 
      (match t.commit_time with
       | Some ct -> ct > transaction.start_time
       | None -> false) &&
      validate_undo_buffer t.undo_buffer
    ) recently_committed.transactions)
  in
  HybridLock.unlock global_lock;
  result