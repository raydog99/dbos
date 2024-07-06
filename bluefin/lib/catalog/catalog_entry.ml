type catalog_type =
  | TableCatalogEntry
  | SchemaCatalogEntry
  | ViewCatalogEntry
  | SequenceCatalogEntry
  | MacroCatalogEntry
  | IndexCatalogEntry
  | TableFunctionCatalogEntry
  | ScalarFunctionCatalogEntry
  | AggregateFunctionCatalogEntry
  | PragmaFunctionCatalogEntry
  | CopyFunctionCatalogEntry
  | CollationCatalogEntry
  | TypeCatalogEntry

type catalog_entry = {
  mutable oid : int64;
  mutable type_ : catalog_type;
  mutable set : catalog_set option;
  mutable name : string;
  mutable deleted : bool;
  mutable temporary : bool;
  mutable internal : bool;
  mutable parent : catalog_entry option;
  mutable child : catalog_entry option;
}

type create_info = {
  schema : string;
  name : string;
}

type alter_info = {
  type_ : string;
}

let create_catalog_entry type_ name oid =
  {
    oid;
    type_;
    set = None;
    name;
    deleted = false;
    temporary = false;
    internal = false;
    parent = None;
    child = None;
  }

let create_catalog_entry_with_catalog type_ (catalog : catalog) name =
  create_catalog_entry type_ name (Catalog.modify_catalog catalog)

let set_as_root _entry = ()

let alter_entry _context (info : alter_info) =
  match info.type_ with
  | _ -> raise (Internal_exception "Unsupported alter type for catalog entry!")

let alter_entry_with_transaction transaction (info : alter_info) =
  match transaction.context with
  | Some context -> alter_entry context info
  | None -> raise (Internal_exception "Cannot AlterEntry without client context")

let undo_alter _context (_info : alter_info) = ()

let copy _context =
  raise (Internal_exception "Unsupported copy type for catalog entry!")

let get_info entry =
  let create_info = {
    schema = "default"; (* Assume default schema *)
    name = entry.name;
  } in
  create_info

let to_sql entry =
  match entry.type_ with
  | TableCatalogEntry -> Printf.sprintf "CREATE TABLE %s (...)" entry.name
  | ViewCatalogEntry -> Printf.sprintf "CREATE VIEW %s AS ..." entry.name
  | _ -> raise (Internal_exception "Unsupported catalog type for ToSQL()")

let set_child entry child =
  entry.child <- Some child;
  child.parent <- Some entry

let take_child entry =
  let child = entry.child in
  (match child with
  | Some c -> c.parent <- None
  | None -> ());
  entry.child <- None;
  child

let has_child entry = entry.child <> None

let has_parent entry = entry.parent <> None

let child entry =
  match entry.child with
  | Some c -> c
  | None -> raise (Internal_exception "Catalog entry has no child")

let parent entry =
  match entry.parent with
  | Some p -> p
  | None -> raise (Internal_exception "Catalog entry has no parent")

let parent_catalog _entry =
  raise (Internal_exception "CatalogEntry::ParentCatalog called on catalog entry without catalog")

let parent_schema _entry =
  raise (Internal_exception "CatalogEntry::ParentSchema called on catalog entry without schema")

let serialize entry serializer =
  let info = get_info entry in
  Serializer.write_string serializer info.schema;
  Serializer.write_string serializer info.name;

let deserialize deserializer =
  let schema = Deserializer.read_string deserializer in
  let name = Deserializer.read_string deserializer in
  { schema; name }

let verify _entry _catalog = ()

type in_catalog_entry = {
  entry : catalog_entry;
  catalog : catalog;
}

let create_in_catalog_entry type_ catalog name =
  {
    entry = create_catalog_entry_with_catalog type_ catalog name;
    catalog;
  }

let verify_in_catalog in_entry catalog =
  assert (in_entry.catalog == catalog)

let is_root entry = entry.parent = None

let get_root entry =
  let rec find_root e =
    match e.parent with
    | None -> e
    | Some p -> find_root p
  in
  find_root entry

let get_qualified_name entry =
  let rec build_name e acc =
    match e.parent with
    | None -> String.concat "." (List.rev acc)
    | Some p -> build_name p (e.name :: acc)
  in
  build_name entry [entry.name]

let is_temporary entry = entry.temporary

let set_temporary entry temp = entry.temporary <- temp

let is_internal entry = entry.internal

let set_internal entry internal = entry.internal <- internal

let is_deleted entry = entry.deleted

let set_deleted entry deleted = entry.deleted <- deleted