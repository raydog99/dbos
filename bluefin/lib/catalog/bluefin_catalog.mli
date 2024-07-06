open Core

module BluefinCatalog : sig
  type t

  val create : AttachedDatabase.t -> t

  val is_bluefin_catalog : t -> bool
  val initialize : t -> load_builtin:bool -> unit
  val get_catalog_type : t -> string
  val get_dependency_manager : t -> DependencyManager.t
  val get_write_lock : t -> Mutex.t

  val create_schema : t -> CatalogTransaction.t -> CreateSchemaInfo.t -> CatalogEntry.t option

  val scan_schemas : t -> ClientContext.t -> (SchemaCatalogEntry.t -> unit) -> unit
  val scan_schemas_no_context : t -> (SchemaCatalogEntry.t -> unit) -> unit

  val get_schema : 
    t -> 
    CatalogTransaction.t -> 
    string -> 
    OnEntryNotFound.t ->
    ?error_context:QueryErrorContext.t ->
    unit ->
    SchemaCatalogEntry.t option

  val plan_create_table_as : 
    t -> 
    ClientContext.t -> 
    LogicalCreateTable.t -> 
    PhysicalOperator.t -> 
    PhysicalOperator.t

  val plan_insert : 
    t -> 
    ClientContext.t -> 
    LogicalInsert.t -> 
    PhysicalOperator.t -> 
    PhysicalOperator.t

  val plan_delete : 
    t -> 
    ClientContext.t -> 
    LogicalDelete.t -> 
    PhysicalOperator.t -> 
    PhysicalOperator.t

  val plan_update : 
    t -> 
    ClientContext.t -> 
    LogicalUpdate.t -> 
    PhysicalOperator.t -> 
    PhysicalOperator.t

  val bind_create_index : 
    t -> 
    Binder.t -> 
    CreateStatement.t -> 
    TableCatalogEntry.t -> 
    LogicalOperator.t -> 
    LogicalOperator.t

  val get_database_size : t -> ClientContext.t -> DatabaseSize.t
  val get_metadata_info : t -> ClientContext.t -> MetadataBlockInfo.t list
  val in_memory : t -> bool
  val get_db_path : t -> string

  val drop_schema : t -> ClientContext.t -> DropInfo.t -> unit
  val verify : t -> unit
end