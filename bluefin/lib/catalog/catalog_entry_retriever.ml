open Core
open Catalog
open Client_context
open Catalog_entry
open Query_error_context
open Type_catalog_entry
open Catalog_type
open Logical_type

type t = {
  context: ClientContext.t;
  mutable callback: (CatalogEntry.t -> unit) option;
}

let create context =
  { context; callback = None }

let get_type_from_catalog catalog schema name on_entry_not_found =
  let error_context = QueryErrorContext.create () in
  match get_entry CatalogType.TYPE_ENTRY catalog schema name on_entry_not_found error_context with
  | None -> LogicalType.invalid
  | Some result ->
      let type_entry = CatalogEntry.cast result (module TypeCatalogEntry) in
      type_entry.user_type

let get_type catalog schema name on_entry_not_found =
  let error_context = QueryErrorContext.create () in
  match get_entry CatalogType.TYPE_ENTRY catalog schema name on_entry_not_found error_context with
  | None -> LogicalType.invalid
  | Some result ->
      let type_entry = CatalogEntry.cast result (module TypeCatalogEntry) in
      type_entry.user_type

let get_entry t type_ catalog schema name on_entry_not_found error_context =
  get_entry_internal t (fun () ->
    Catalog.get_entry t.context type_ catalog schema name on_entry_not_found error_context
  )

let get_schema t catalog name on_entry_not_found error_context =
  let result = Catalog.get_schema t.context catalog name on_entry_not_found error_context in
  match result with
  | None -> None
  | Some schema ->
      (match t.callback with
      | Some cb -> cb schema
      | None -> ());
      Some schema

let get_entry_from_catalog t type_ catalog schema name on_entry_not_found error_context =
  get_entry_internal t (fun () ->
    catalog.get_entry t.context type_ schema name on_entry_not_found error_context
  )

let get_entry_internal t f =
  let result = f () in
  match result with
  | None -> None
  | Some entry ->
      (match t.callback with
      | Some cb -> cb entry
      | None -> ());
      Some entry

let set_callback t cb =
  t.callback <- Some cb

let get_callback t =
  t.callback