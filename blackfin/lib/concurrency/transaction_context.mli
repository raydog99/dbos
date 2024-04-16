open InternalTypes

module type TransactionContext : sig
	type t = {
		txn_id : int;
		thread_id : int;
		read_id : int;		(* Tuple versions transaction can access *)
		commit_id : int;
		timestamp : Int64.t;
		isolation_level : IsolationLevel
	}

	val create : int -> IsolationLevel.t -> int -> ?commit_id.int -> unit -> t
	val get_thread_id : unit -> int
	val get_transaction_id : unit -> int
	val get_read_id : unit -> int
	val get_commit_id : unit -> int
	val get_timestamp : unit -> int
	val get_transaction_level : unit -> int

	val set_commit_id : int -> unit
	val set_timestamp : Int64.t -> unit
end