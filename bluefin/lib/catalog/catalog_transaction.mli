open Core
open Catalog
open DatabaseInstance
open ClientContext
open Transaction

type t = {
  db : DatabaseInstance.t option;
  context : ClientContext.t option;
  transaction : Transaction.t option;
  transaction_id : int64;
  start_time : int64;
}

val create_from_catalog : Catalog.t -> ClientContext.t -> t
val create_from_db : DatabaseInstance.t -> int64 -> int64 -> t

val has_context : t -> bool
val get_context : t -> ClientContext.t

val get_system_catalog_transaction : ClientContext.t -> t
val get_system_transaction : DatabaseInstance.t -> t