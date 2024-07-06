open Core
open Catalog
open CatalogEntry

module CatalogEntryMap : sig
  type t

  val create : unit -> t
  val add_entry : t -> CatalogEntry.t -> unit
  val update_entry : t -> CatalogEntry.t -> unit
  val drop_entry : t -> CatalogEntry.t -> unit
  val entries : t -> CatalogEntry.t String.Map.t
  val get_entry : t -> string -> CatalogEntry.t option
end

module CatalogSet : sig
  type t

  module EntryLookup : sig
    type failure_reason =
      | SUCCESS
      | DELETED
      | NOT_PRESENT

    type t = {
      result : CatalogEntry.t option;
      reason : failure_reason;
    }
  end

  val create : Catalog.t -> ?defaults:DefaultGenerator.t -> unit -> t

  val create_entry : t -> CatalogTransaction.t -> string -> CatalogEntry.t -> LogicalDependencyList.t -> bool
  val create_entry_context : t -> ClientContext.t -> string -> CatalogEntry.t -> LogicalDependencyList.t -> bool

  val alter_entry : t -> CatalogTransaction.t -> string -> AlterInfo.t -> bool

  val drop_entry : t -> CatalogTransaction.t -> string -> cascade:bool -> ?allow_drop_internal:bool -> unit -> bool
  val drop_entry_context : t -> ClientContext.t -> string -> cascade:bool -> ?allow_drop_internal:bool -> unit -> bool

  val get_catalog : t -> DuckCatalog.t

  val alter_ownership : t -> CatalogTransaction.t -> ChangeOwnershipInfo.t -> bool

  val cleanup_entry : t -> CatalogEntry.t -> unit

  val get_entry_detailed : t -> CatalogTransaction.t -> string -> EntryLookup.t
  val get_entry : t -> CatalogTransaction.t -> string -> CatalogEntry.t option
  val get_entry_context : t -> ClientContext.t -> string -> CatalogEntry.t option

  val similar_entry : t -> CatalogTransaction.t -> string -> SimilarCatalogEntry.t

  val undo : t -> CatalogEntry.t -> unit

  val scan : t -> (CatalogEntry.t -> unit) -> unit
  val scan_with_prefix : t -> CatalogTransaction.t -> (CatalogEntry.t -> unit) -> string -> unit
  val scan_transaction : t -> CatalogTransaction.t -> (CatalogEntry.t -> unit) -> unit
  val scan_context : t -> ClientContext.t -> (CatalogEntry.t -> unit) -> unit

  val get_entries : t -> CatalogTransaction.t -> (module CatalogEntryType with type t = 'a) -> 'a list

  val created_by_other_active_transaction : t -> CatalogTransaction.t -> int64 -> bool
  val committed_after_starting : t -> CatalogTransaction.t -> int64 -> bool
  val has_conflict : t -> CatalogTransaction.t -> int64 -> bool
  val use_timestamp : t -> CatalogTransaction.t -> int64 -> bool

  val update_timestamp : t -> CatalogEntry.t -> int64 -> unit

  val get_catalog_lock : t -> Mutex.t

  val verify : t -> Catalog.t -> unit
end