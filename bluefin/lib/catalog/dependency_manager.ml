open Core

module DependencyManager = struct
  type t = {
    catalog : BluefinCatalog.t;
    subjects : CatalogSet.t;
    dependents : CatalogSet.t;
  }

  let create catalog =
    { catalog; subjects = CatalogSet.create catalog; dependents = CatalogSet.create catalog }

  let get_schema entry =
    match CatalogEntry.type_ entry with
    | SCHEMA_ENTRY -> CatalogEntry.name entry
    | _ -> CatalogEntry.parent_schema entry |> SchemaCatalogEntry.name

  let mangle_name info =
    let type_ = CatalogEntryInfo.type_ info in
    let schema = CatalogEntryInfo.schema info in
    let name = CatalogEntryInfo.name info in
    MangledEntryName.create (CatalogType.to_string type_ ^ "\000" ^ schema ^ "\000" ^ name)

  let mangle_name_entry entry =
    match CatalogEntry.type_ entry with
    | DEPENDENCY_ENTRY ->
        let dependency_entry = CatalogEntry.cast entry (module DependencyEntry) in
        DependencyEntry.entry_mangled_name dependency_entry
    | _ ->
        let type_ = CatalogEntry.type_ entry in
        let schema = get_schema entry in
        let name = CatalogEntry.name entry in
        let info = CatalogEntryInfo.create type_ schema name in
        mangle_name info

  let is_system_entry entry =
    CatalogEntry.internal entry ||
    match CatalogEntry.type_ entry with
    | DEPENDENCY_ENTRY | DATABASE_ENTRY | RENAMED_ENTRY -> true
    | _ -> false

  let dependents t = t.dependents

  let subjects t = t.subjects

  let scan_set_internal t transaction info scan_subjects callback =
    let other_entries = ref [] in
    let cb other =
      assert (CatalogEntry.type_ other = DEPENDENCY_ENTRY);
      let other_entry = CatalogEntry.cast other (module DependencyEntry) in
      other_entries := other_entry :: !other_entries;
      callback other_entry
    in
    if scan_subjects then
      DependencyCatalogSet.scan (DependencyCatalogSet.create t.subjects info) transaction cb
    else
      DependencyCatalogSet.scan (DependencyCatalogSet.create t.dependents info) transaction cb

  let scan_dependents t transaction info callback =
    scan_set_internal t transaction info false callback

  let scan_subjects t transaction info callback =
    scan_set_internal t transaction info true callback

  let remove_dependency t transaction info =
    let dependent = info.DependencyInfo.dependent in
    let subject = info.DependencyInfo.subject in
    let dependents = DependencyCatalogSet.create t.dependents subject.entry in
    let subjects = DependencyCatalogSet.create t.subjects dependent.entry in
    let dependent_mangled = MangledEntryName.create dependent.entry in
    let subject_mangled = MangledEntryName.create subject.entry in
    (match DependencyCatalogSet.get_entry dependents transaction dependent_mangled with
     | Some _ -> DependencyCatalogSet.drop_entry dependents transaction dependent_mangled ~cascade:false
     | None -> ());
    (match DependencyCatalogSet.get_entry subjects transaction subject_mangled with
     | Some _ -> DependencyCatalogSet.drop_entry subjects transaction subject_mangled ~cascade:false
     | None -> ())

  let create_subject t transaction info =
    let from = info.DependencyInfo.dependent.entry in
    let set = DependencyCatalogSet.create t.subjects from in
    let dep = DependencySubjectEntry.create t.catalog info in
    let entry_name = DependencyEntry.entry_mangled_name dep in
    DependencyCatalogSet.create_entry set transaction entry_name dep

  let create_dependent t transaction info =
    let from = info.DependencyInfo.subject.entry in
    let set = DependencyCatalogSet.create t.dependents from in
    let dep = DependencyDependentEntry.create t.catalog info in
    let entry_name = DependencyEntry.entry_mangled_name dep in
    DependencyCatalogSet.create_entry set transaction entry_name dep

  let create_dependency t transaction info =
    let subjects = DependencyCatalogSet.create t.subjects info.DependencyInfo.dependent.entry in
    let dependents = DependencyCatalogSet.create t.dependents info.DependencyInfo.subject.entry in
    let subject_mangled = mangle_name info.DependencyInfo.subject.entry in
    let dependent_mangled = mangle_name info.DependencyInfo.dependent.entry in
    let dependent_flags = info.DependencyInfo.dependent.flags in
    let subject_flags = info.DependencyInfo.subject.flags in
    (match DependencyCatalogSet.get_entry subjects transaction subject_mangled with
     | Some existing_subject ->
         let existing = CatalogEntry.cast existing_subject (module DependencyEntry) in
         let existing_flags = DependencyEntry.subject existing |> (fun x -> x.flags) in
         if existing_flags <> subject_flags then
           DependencySubjectFlags.apply subject_flags existing_flags;
         DependencyCatalogSet.drop_entry subjects transaction subject_mangled ~cascade:false ~allow_drop_internal:false
     | None -> ());
    (match DependencyCatalogSet.get_entry dependents transaction dependent_mangled with
     | Some existing_dependent ->
         let existing = CatalogEntry.cast existing_dependent (module DependencyEntry) in
         let existing_flags = DependencyEntry.dependent existing |> (fun x -> x.flags) in
         if existing_flags <> dependent_flags then
           DependencyDependentFlags.apply dependent_flags existing_flags;
         DependencyCatalogSet.drop_entry dependents transaction dependent_mangled ~cascade:false ~allow_drop_internal:false
     | None -> ());
    create_dependent t transaction info;
    create_subject t transaction info
end