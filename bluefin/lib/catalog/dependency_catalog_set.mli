open Core
open CatalogSet
open CatalogEntryInfo
open CatalogTransaction
open MangledEntryName
open CatalogEntry

module DependencyCatalogSet : sig
  type t

  val create : CatalogSet.t -> CatalogEntryInfo.t -> t

  val create_entry : t -> CatalogTransaction.t -> MangledEntryName.t -> CatalogEntry.t -> bool

  val get_entry_detailed : t -> CatalogTransaction.t -> MangledEntryName.t -> CatalogSet.EntryLookup.t

  val get_entry : t -> CatalogTransaction.t -> MangledEntryName.t -> CatalogEntry.t option

  val scan : t -> CatalogTransaction.t -> (CatalogEntry.t -> unit) -> unit

  val drop_entry : t -> CatalogTransaction.t -> MangledEntryName.t -> cascade:bool -> ?allow_drop_internal:bool -> unit -> bool

  val set : t -> CatalogSet.t
  val info : t -> CatalogEntryInfo.t
  val mangled_name : t -> MangledEntryName.t
end

module DependencyManager : sig
  val mangle_name : CatalogEntryInfo.t -> MangledEntryName.t
end