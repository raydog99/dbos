open Core

module rec Catalog : sig
  type t

  val create : AttachedDatabase.t -> t

  val get_system_catalog : ClientContext.t -> t
  val get_catalog : ClientContext.t -> string -> t
  val get_catalog_entry : ClientContext.t -> string -> t option

  val get_attached : t -> AttachedDatabase.t
  val get_database : t -> DatabaseInstance.t
  val get_name : t -> string
  val get_oid : t -> int

  val get_catalog_version : t -> int
  val modify_catalog : t -> int

  val is_system_catalog : t -> bool
  val is_temporary_catalog : t -> bool

  val get_catalog_transaction : t -> ClientContext.t -> CatalogTransaction.t

  val create_schema : t -> CatalogTransaction.t -> CreateSchemaInfo.t -> CatalogEntry.t option
  val create_table : t -> CatalogTransaction.t -> BoundCreateTableInfo.t -> CatalogEntry.t option
  val create_view : t -> CatalogTransaction.t -> CreateViewInfo.t -> CatalogEntry.t option
  val create_sequence : t -> CatalogTransaction.t -> CreateSequenceInfo.t -> CatalogEntry.t option
  val create_type : t -> CatalogTransaction.t -> CreateTypeInfo.t -> CatalogEntry.t option
  val create_table_function : t -> CatalogTransaction.t -> CreateTableFunctionInfo.t -> CatalogEntry.t option

  val drop_entry : t -> ClientContext.t -> DropInfo.t -> unit

  val get_schema : t -> ClientContext.t -> string -> SchemaCatalogEntry.t
  val get_schema_opt : t -> ClientContext.t -> string -> OnEntryNotFound.t -> SchemaCatalogEntry.t option

  val get_entry : t -> ClientContext.t -> CatalogType.t -> string -> string -> CatalogEntry.t
  val get_entry_opt : t -> ClientContext.t -> CatalogType.t -> string -> string -> OnEntryNotFound.t -> CatalogEntry.t option

  val alter : t -> CatalogTransaction.t -> AlterInfo.t -> unit

  val get_database_size : t -> ClientContext.t -> DatabaseSize.t
  val get_metadata_info : t -> ClientContext.t -> MetadataBlockInfo.t list

  val in_memory : t -> bool
  val get_db_path : t -> string

  val catalog_type_lookup_rule : t -> CatalogType.t -> CatalogLookupBehavior.t

  val get_schemas : t -> ClientContext.t -> SchemaCatalogEntry.t list
  val get_all_schemas : ClientContext.t -> SchemaCatalogEntry.t list

  val verify : t -> unit

  val unrecognized_configuration_error : ClientContext.t -> string -> exn
  val autoload_extension_by_config_name : ClientContext.t -> string -> unit
  val auto_load_extension_by_catalog_entry : DatabaseInstance.t -> CatalogType.t -> string -> bool
  val try_auto_load : ClientContext.t -> string -> bool
end