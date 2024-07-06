open Core
open Schema_catalog_entry

module SimilarCatalogEntry : sig
  type t = {
    name : string;
    distance : int;
    schema : SchemaCatalogEntry.t option;
  }

  val create : string -> int -> SchemaCatalogEntry.t option -> t

  val found : t -> bool

  val get_qualified_name : t -> qualify_catalog:bool -> qualify_schema:bool -> string
end