open Core
open Catalog
open Client_context
open Catalog_entry
open Schema_catalog_entry
open Catalog_type
open On_entry_not_found
open Query_error_context
open Logical_type

module CatalogEntryRetriever : sig
  type t

  val create : ClientContext.t -> t
  val copy : t -> t

  val get_entry :
    t ->
    CatalogType.t ->
    catalog:string ->
    schema:string ->
    name:string ->
    ?on_entry_not_found:OnEntryNotFound.t ->
    ?error_context:QueryErrorContext.t ->
    unit ->
    CatalogEntry.t option

  val get_entry_from_catalog :
    t ->
    CatalogType.t ->
    catalog:Catalog.t ->
    schema:string ->
    name:string ->
    ?on_entry_not_found:OnEntryNotFound.t ->
    ?error_context:QueryErrorContext.t ->
    unit ->
    CatalogEntry.t option

  val get_type :
    t ->
    catalog:string ->
    schema:string ->
    name:string ->
    ?on_entry_not_found:OnEntryNotFound.t ->
    unit ->
    LogicalType.t

  val get_type_from_catalog :
    t ->
    Catalog.t ->
    schema:string ->
    name:string ->
    ?on_entry_not_found:OnEntryNotFound.t ->
    unit ->
    LogicalType.t

  val get_schema :
    t ->
    catalog:string ->
    name:string ->
    ?on_entry_not_found:OnEntryNotFound.t ->
    ?error_context:QueryErrorContext.t ->
    unit ->
    SchemaCatalogEntry.t option

  val set_callback : t -> (CatalogEntry.t -> unit) -> unit
  val get_callback : t -> (CatalogEntry.t -> unit) option
end