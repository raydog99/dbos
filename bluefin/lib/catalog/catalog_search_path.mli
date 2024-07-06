open Core
open ClientContext

module CatalogSearchEntry : sig
  type t = {
    catalog : string;
    schema : string;
  }

  val create : string -> string -> t
  val to_string : t -> string
  val list_to_string : t list -> string
  val parse : string -> t
  val parse_list : string -> t list
end

module CatalogSetPathType : sig
  type t =
    | SET_SCHEMA
    | SET_SCHEMAS
end

module CatalogSearchPath : sig
  type t

  val create : ClientContext.t -> t

  val set : t -> CatalogSearchEntry.t -> CatalogSetPathType.t -> unit
  val set_multiple : t -> CatalogSearchEntry.t list -> CatalogSetPathType.t -> unit
  val reset : t -> unit
  val get : t -> CatalogSearchEntry.t list
  val get_set_paths : t -> CatalogSearchEntry.t list
  val get_default : t -> CatalogSearchEntry.t
  val get_default_schema : t -> string -> string
  val get_default_catalog : t -> string -> string
  val get_schemas_for_catalog : t -> string -> string list
  val get_catalogs_for_schema : t -> string -> string list
  val schema_in_search_path : t -> ClientContext.t -> string -> string -> bool
end