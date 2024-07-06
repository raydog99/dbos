open Core

module BluefinCatalog = struct
  type t = {
    db : AttachedDatabase.t;
    dependency_manager : DependencyManager.t;
    schemas : CatalogSet.t;
  }

  let create db =
    let dependency_manager = DependencyManager.create db in
    let schemas = CatalogSet.create db (DefaultSchemaGenerator.create db) in
    { db; dependency_manager; schemas }

  let initialize t ~load_builtin =
    let data = CatalogTransaction.get_system_transaction (AttachedDatabase.get_database t.db) in
    let info = CreateSchemaInfo.create ~schema:DEFAULT_SCHEMA ~internal:true in
    let _ = create_schema t data info in

    if load_builtin then
      let builtin = BuiltinFunctions.create data t in
      BuiltinFunctions.initialize builtin;
    verify t

  let is_bluefin_catalog _ = true

  let create_schema_internal t transaction info =
    let dependencies = LogicalDependencyList.create () in
    let entry = BluefinSchemaEntry.create t info in
    if CatalogSet.create_entry t.schemas transaction info.schema entry dependencies then
      Some entry
    else
      None

  let create_schema t transaction info =
    assert (not (String.is_empty info.schema));
    match create_schema_internal t transaction info with
    | Some result -> Some result
    | None ->
        (match info.on_conflict with
         | OnCreateConflict.ERROR_ON_CONFLICT ->
             raise (CatalogException.entry_already_exists CatalogType.SCHEMA_ENTRY info.schema)
         | OnCreateConflict.REPLACE_ON_CONFLICT ->
             let drop_info = DropInfo.create ~type_:CatalogType.SCHEMA_ENTRY ~catalog:info.catalog ~name:info.schema in
             drop_schema t transaction drop_info;
             (match create_schema_internal t transaction info with
              | Some result -> Some result
              | None -> raise (InternalException "Failed to create schema entry in CREATE_OR_REPLACE"))
         | OnCreateConflict.IGNORE_ON_CONFLICT -> None
         | _ -> raise (InternalException "Unsupported OnCreateConflict for CreateSchema"))

  let drop_schema t transaction info =
    assert (not (String.is_empty info.name));
    modify_catalog t;
    if not (CatalogSet.drop_entry t.schemas transaction info.name ~cascade:info.cascade) then
      if info.if_not_found = OnEntryNotFound.THROW_EXCEPTION then
        raise (CatalogException.missing_entry CatalogType.SCHEMA_ENTRY info.name "")

  let drop_schema_context t context info =
    drop_schema t (get_catalog_transaction t context) info

  let scan_schemas t context callback =
    CatalogSet.scan t.schemas (get_catalog_transaction t context)
      (fun entry -> callback (CatalogEntry.cast entry (module SchemaCatalogEntry)))

  let scan_schemas_no_context t callback =
    CatalogSet.scan t.schemas
      (fun entry -> callback (CatalogEntry.cast entry (module SchemaCatalogEntry)))

  let get_schema t transaction schema_name if_not_found error_context =
    assert (not (String.is_empty schema_name));
    match CatalogSet.get_entry t.schemas transaction schema_name with
    | Some entry -> Some (CatalogEntry.cast entry (module SchemaCatalogEntry))
    | None ->
        if if_not_found = OnEntryNotFound.THROW_EXCEPTION then
          raise (CatalogException.create error_context (Printf.sprintf "Schema with name %s does not exist!" schema_name))
        else
          None

  let get_database_size t context =
    let transaction = BluefinTransactionManager.get t.db in
    let lock = TransactionManager.shared_checkpoint_lock transaction in
    AttachedDatabase.get_storage_manager t.db |> StorageManager.get_database_size

  let get_metadata_info t context =
    let transaction = BluefinTransactionManager.get t.db in
    let lock = TransactionManager.shared_checkpoint_lock transaction in
    AttachedDatabase.get_storage_manager t.db |> StorageManager.get_metadata_info

  let in_memory t =
    AttachedDatabase.get_storage_manager t.db |> StorageManager.in_memory

  let get_db_path t =
    AttachedDatabase.get_storage_manager t.db |> StorageManager.get_db_path

  let verify t =
    CatalogSet.verify t.schemas t
end