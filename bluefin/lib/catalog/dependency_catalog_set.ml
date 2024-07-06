open Core
open Catalog_entry
open Catalog_set
open Catalog_entry_info
open Logical_dependency_list
open Dependency_entry
open Dependency_manager
open Mangled_entry_name
open Mangled_dependency_name

type t = {
  set : CatalogSet.t;
  info : CatalogEntryInfo.t;
  mangled_name : MangledEntryName.t;
}

let create set info =
  let mangled_name = DependencyManager.mangle_name info in
  { set; info; mangled_name }

let apply_prefix t name =
  MangledDependencyName.create t.mangled_name name

let create_entry t transaction name value =
  let new_name = apply_prefix t name in
  let empty_dependencies = LogicalDependencyList.empty in
  CatalogSet.create_entry t.set transaction new_name.name value empty_dependencies

let get_entry_detailed t transaction name =
  let new_name = apply_prefix t name in
  CatalogSet.get_entry_detailed t.set transaction new_name.name

let get_entry t transaction name =
  let new_name = apply_prefix t name in
  CatalogSet.get_entry t.set transaction new_name.name

let scan t transaction callback =
  CatalogSet.scan_with_prefix t.set transaction
    (fun entry ->
       let dep = CatalogEntry.cast entry (module DependencyEntry) in
       let from = DependencyEntry.source_mangled_name dep in
       if String.Caseless.equal from.name t.mangled_name.name then
         callback entry)
    t.mangled_name.name

let drop_entry t transaction name ~cascade ~allow_drop_internal =
  let new_name = apply_prefix t name in
  CatalogSet.drop_entry t.set transaction new_name.name ~cascade ~allow_drop_internal