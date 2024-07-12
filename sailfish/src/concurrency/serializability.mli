open Types

val create_predicate_log : unit -> predicate_log
val log_predicate : predicate_log -> predicate -> unit
val log_accessed_attribute : predicate_log -> attribute -> unit
val validate_transaction : transaction -> predicate_log -> recently_committed -> bool