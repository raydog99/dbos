open Core
open Catalog

module rec CatalogEntry : sig
  type t = {
    oid : int;
    type_ : CatalogType.t;
    set : CatalogSet.t option;
    name : string;
    deleted : bool;
    temporary : bool;
    internal : bool;
    timestamp : int;
    comment : Value.t;
    tags : (string, string) Hashtbl.t;
  }

  val create : CatalogType.t -> Catalog.t -> string -> t
  val create_with_oid : CatalogType.t -> string -> int -> t

  val alter_entry : t -> ClientContext.t -> AlterInfo.t -> t
  val alter_entry_transaction : t -> CatalogTransaction.t -> AlterInfo.t -> t
  val undo_alter : t -> ClientContext.t -> AlterInfo.t -> unit
  val copy : t -> ClientContext.t -> t
  val get_info : t -> CreateInfo.t
  val set_as_root : t -> unit
  val to_sql : t -> string
  val parent_catalog : t -> Catalog.t
  val parent_schema : t -> SchemaCatalogEntry.t
  val verify : t -> Catalog.t -> unit
  val serialize : t -> Serializer.t -> unit
  val deserialize : Deserializer.t -> CreateInfo.t

  val set_child : t -> t -> unit
  val take_child : t -> t option
  val has_child : t -> bool
  val has_parent : t -> bool
  val child : t -> t
  val parent : t -> t

  val cast : t -> (module CatalogEntryType with type t = 'a) -> 'a
end

module InCatalogEntry : sig
  type t = {
    base : CatalogEntry.t;
    catalog : Catalog.t;
  }

  val create : CatalogType.t -> Catalog.t -> string -> t

  val parent_catalog : t -> Catalog.t
  val verify : t -> Catalog.t -> unit
end