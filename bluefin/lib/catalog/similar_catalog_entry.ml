open Core
open Catalog
open Schema_catalog_entry

module SimilarCatalogEntry = struct
  type t = {
    name : string;
    distance : int;
    schema : SchemaCatalogEntry.t option;
  }

  let create name distance schema =
    { name; distance; schema }

  let found t =
    not (String.is_empty t.name)

  let get_qualified_name t ~qualify_catalog ~qualify_schema =
    assert (found t);
    match t.schema with
    | None -> t.name
    | Some schema ->
        let parts = [] in
        let parts =
          if qualify_catalog then
            (SchemaCatalogEntry.catalog schema |> Catalog.get_name) :: parts
          else
            parts
        in
        let parts =
          if qualify_schema then
            (SchemaCatalogEntry.name schema) :: parts
          else
            parts
        in
        let parts = t.name :: parts in
        String.concat ~sep:"." (List.rev parts)
end