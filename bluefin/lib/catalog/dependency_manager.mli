open Core
open CatalogEntry
open CatalogEntryInfo
open DependencySubjectFlags
open DependencyDependentFlags
open DependencyEntry
open DuckCatalog
open ClientContext
open CatalogTransaction
open LogicalDependencyList
open AlterInfo

module DependencySubject : sig
  type t = {
    entry : CatalogEntryInfo.t;
    flags : DependencySubjectFlags.t;
  }
end

module DependencyDependent : sig
  type t = {
    entry : CatalogEntryInfo.t;
    flags : DependencyDependentFlags.t;
  }
end

module DependencyInfo : sig
  type t = {
    dependent : DependencyDependent.t;
    subject : DependencySubject.t;
  }

  val from_subject : DependencyEntry.t -> t
  val from_dependent : DependencyEntry.t -> t
end

module MangledEntryName : sig
  type t

  val create : CatalogEntryInfo.t -> t
  val name : t -> string
  val equal : t -> t -> bool
end

module MangledDependencyName : sig
  type t

  val create : MangledEntryName.t -> MangledEntryName.t -> t
  val name : t -> string
end

module DependencyManager : sig
  type t

  val create : DuckCatalog.t -> t

  val scan : t -> ClientContext.t -> (CatalogEntry.t -> CatalogEntry.t -> DependencyDependentFlags.t -> unit) -> unit

  val add_ownership : t -> CatalogTransaction.t -> CatalogEntry.t -> CatalogEntry.t -> unit

  val get_schema : CatalogEntry.t -> string
  val mangle_name : CatalogEntryInfo.t -> MangledEntryName.t
  val mangle_name_entry : CatalogEntry.t -> MangledEntryName.t
  val get_lookup_properties : CatalogEntry.t -> CatalogEntryInfo.t

  val add_object : t -> CatalogTransaction.t -> CatalogEntry.t -> LogicalDependencyList.t -> unit
  val drop_object : t -> CatalogTransaction.t -> CatalogEntry.t -> cascade:bool -> unit
  val alter_object : t -> CatalogTransaction.t -> CatalogEntry.t -> CatalogEntry.t -> AlterInfo.t -> unit
end