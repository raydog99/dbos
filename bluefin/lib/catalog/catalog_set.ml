open Core

module CatalogEntryMap = struct
  type t = (string, CatalogEntry.t) Hashtbl.t

  let create () = Hashtbl.create (module String)

  let add_entry t entry =
    let name = CatalogEntry.name entry in
    if Hashtbl.mem t name then
      raise (Invalid_argument (Printf.sprintf "Entry with name \"%s\" already exists" name))
    else
      Hashtbl.add_exn t ~key:name ~data:entry

  let update_entry t catalog_entry =
    let name = CatalogEntry.name catalog_entry in
    match Hashtbl.find t name with
    | None -> raise (Invalid_argument (Printf.sprintf "Entry with name \"%s\" does not exist" name))
    | Some existing ->
        let _ = CatalogEntry.set_child catalog_entry (Some existing) in
        Hashtbl.set t ~key:name ~data:catalog_entry

  let entries t = t

  let drop_entry t entry =
    let name = CatalogEntry.name entry in
    match Hashtbl.find t name with
    | None ->
        raise (Invalid_argument (Printf.sprintf "Attempting to drop entry with name \"%s\" but no chain with that name exists" name))
    | Some chain ->
        if not (phys_equal chain entry) then
          raise (Invalid_argument "Entry to drop is not the top of the chain")
        else
          match CatalogEntry.take_child entry with
          | None -> Hashtbl.remove t name
          | Some child -> Hashtbl.set t ~key:name ~data:child

  let get_entry t name = Hashtbl.find t name
end

module CatalogSet = struct
  type t = {
    catalog : DuckCatalog.t;
    defaults : DefaultGenerator.t option;
    mutable map : CatalogEntryMap.t;
    catalog_lock : Mutex.t;
  }

  let create catalog defaults =
    { catalog; defaults; map = CatalogEntryMap.create (); catalog_lock = Mutex.create () }

  let is_dependency_entry entry =
    CatalogEntry.type_ entry = CatalogType.DEPENDENCY_ENTRY

  let start_chain t transaction name =
    match CatalogEntryMap.get_entry t.map name with
    | Some _ -> false
    | None ->
        (match t.defaults with
         | Some defaults ->
             (match DefaultGenerator.create_default_entry defaults transaction name with
              | Some _ -> false
              | None ->
                  let dummy_node = InCatalogEntry.create CatalogType.INVALID t.catalog name in
                  CatalogEntry.set_timestamp dummy_node 0L;
                  CatalogEntry.set_deleted dummy_node true;
                  CatalogEntry.set_set dummy_node (Some t);
                  CatalogEntryMap.add_entry t.map dummy_node;
                  true)
         | None ->
             let dummy_node = InCatalogEntry.create CatalogType.INVALID t.catalog name in
             CatalogEntry.set_timestamp dummy_node 0L;
             CatalogEntry.set_deleted dummy_node true;
             CatalogEntry.set_set dummy_node (Some t);
             CatalogEntryMap.add_entry t.map dummy_node;
             true)

  let verify_vacancy t transaction entry =
    if has_conflict t transaction (CatalogEntry.timestamp entry) then
      raise (TransactionException (Printf.sprintf "Catalog write-write conflict on create with \"%s\"" (CatalogEntry.name entry)))
    else
      CatalogEntry.deleted entry

  let check_catalog_entry_invariants t value name =
    if CatalogEntry.internal value && not (DuckCatalog.is_system_catalog t.catalog) && name <> DEFAULT_SCHEMA then
      raise (Invalid_argument (Printf.sprintf "Attempting to create internal entry \"%s\" in non-system catalog - internal entries can only be created in the system catalog" name))
    else if not (CatalogEntry.internal value) then
      if not (CatalogEntry.temporary value) && DuckCatalog.is_system_catalog t.catalog && not (is_dependency_entry value) then
        raise (Invalid_argument (Printf.sprintf "Attempting to create non-internal entry \"%s\" in system catalog - the system catalog can only contain internal entries" name))
      else if CatalogEntry.temporary value && not (DuckCatalog.is_temporary_catalog t.catalog) then
        raise (Invalid_argument (Printf.sprintf "Attempting to create temporary entry \"%s\" in non-temporary catalog" name))
      else if not (CatalogEntry.temporary value) && DuckCatalog.is_temporary_catalog t.catalog && name <> DEFAULT_SCHEMA then
        raise (Invalid_argument (Printf.sprintf "Cannot create non-temporary entry \"%s\" in temporary catalog" name))

  let create_committed_entry t entry =
    let name = CatalogEntry.name entry in
    match CatalogEntryMap.get_entry t.map name with
    | Some _ -> None
    | None ->
        CatalogEntry.set_set entry (Some t);
        CatalogEntry.set_timestamp entry 0L;
        CatalogEntryMap.add_entry t.map entry;
        Some entry

  let create_entry_internal t transaction name value =
    match CatalogEntryMap.get_entry t.map name with
    | None ->
        if not (start_chain t transaction name) then false
        else
          let value_ptr = value in
          CatalogEntryMap.update_entry t.map value;
          (match Transaction.transaction transaction with
           | Some dtransaction ->
               DuckTransaction.push_catalog_entry dtransaction (CatalogEntry.child value_ptr)
           | None -> ());
          true
    | Some entry_value ->
        if not (verify_vacancy t transaction entry_value) then false
        else
          let value_ptr = value in
          CatalogEntryMap.update_entry t.map value;
          (match Transaction.transaction transaction with
           | Some dtransaction ->
               DuckTransaction.push_catalog_entry dtransaction (CatalogEntry.child value_ptr)
           | None -> ());
          true

  let create_entry t transaction name value dependencies =
    check_catalog_entry_invariants t value name;
    CatalogEntry.set_timestamp value (Transaction.transaction_id transaction);
    CatalogEntry.set_set value (Some t);
    DependencyManager.add_object (DuckCatalog.get_dependency_manager t.catalog) transaction value dependencies;
    Mutex.lock (DuckCatalog.get_write_lock t.catalog);
    Mutex.lock t.catalog_lock;
    let result = create_entry_internal t transaction name value in
    Mutex.unlock t.catalog_lock;
    Mutex.unlock (DuckCatalog.get_write_lock t.catalog);
    result
end