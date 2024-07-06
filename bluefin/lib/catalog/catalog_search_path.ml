open Core

module CatalogSearchEntry = struct
  type t = {
    catalog: string;
    schema: string;
  }

  let create catalog schema =
    { catalog; schema }

  let to_string t =
    if String.is_empty t.catalog then
      write_optionally_quoted t.schema
    else
      write_optionally_quoted t.catalog ^ "." ^ write_optionally_quoted t.schema

  let write_optionally_quoted input =
    if String.exists input ~f:(fun c -> c = '.' || c = ',') then
      "\"" ^ input ^ "\""
    else
      input

  let list_to_string entries =
    String.concat ~sep:"," (List.map entries ~f:to_string)

  let parse_internal input idx =
    let rec loop catalog schema entry idx =
      if idx >= String.length input then
        if String.is_empty schema then
          failwith "Unexpected end of entry - empty CatalogSearchEntry"
        else
          create catalog schema
      else
        match input.[idx] with
        | '"' -> loop_quoted catalog schema entry (idx + 1)
        | '.' -> handle_separator catalog schema entry idx
        | ',' -> create catalog (if String.is_empty schema then entry else schema)
        | c -> loop catalog schema (entry ^ String.make 1 c) (idx + 1)
    and loop_quoted catalog schema entry idx =
      if idx >= String.length input then
        failwith "Unterminated quote in qualified name!"
      else if input.[idx] = '"' && idx + 1 < String.length input && input.[idx + 1] = '"' then
        loop_quoted catalog schema (entry ^ "\"") (idx + 2)
      else if input.[idx] = '"' then
        loop catalog schema entry (idx + 1)
      else
        loop_quoted catalog schema (entry ^ String.make 1 input.[idx]) (idx + 1)
    and handle_separator catalog schema entry idx =
      if String.is_empty entry then
        failwith "Unexpected dot - empty CatalogSearchEntry"
      else if String.is_empty schema then
        loop catalog entry "" (idx + 1)
      else if String.is_empty catalog then
        loop schema entry "" (idx + 1)
      else
        failwith "Too many dots - expected [schema] or [catalog.schema] for CatalogSearchEntry"
    in
    loop "" "" "" idx

  let parse input =
    let result, pos = parse_internal input 0 in
    if pos < String.length input then
      failwith (Printf.sprintf "Failed to convert entry \"%s\" to CatalogSearchEntry - expected a single entry" input)
    else
      result

  let parse_list input =
    let rec loop acc idx =
      if idx >= String.length input then
        List.rev acc
      else
        let entry, new_idx = parse_internal input idx in
        loop (entry :: acc) new_idx
    in
    loop [] 0
end

module CatalogSetPathType = struct
  type t =
    | SET_SCHEMA
    | SET_SCHEMAS
end

module CatalogSearchPath = struct
  type t = {
    mutable paths: CatalogSearchEntry.t list;
    mutable set_paths: CatalogSearchEntry.t list;
    context: ClientContext.t;
  }

  let create context =
    let t = { paths = []; set_paths = []; context } in
    t.reset ();
    t

  let reset t =
    t.set_paths (set_paths [])

  let get_set_name = function
    | CatalogSetPathType.SET_SCHEMA -> "SET schema"
    | CatalogSetPathType.SET_SCHEMAS -> "SET search_path"

  let set t new_paths set_type =
    if set_type <> CatalogSetPathType.SET_SCHEMAS && List.length new_paths <> 1 then
      failwith (Printf.sprintf "%s can set only 1 schema. This has %d" (get_set_name set_type) (List.length new_paths));
    
    let validated_paths = List.map new_paths ~f:(fun path ->
      let schema_entry = Catalog.get_schema t.context path.catalog path.schema OnEntryNotFound.RETURN_NULL in
      match schema_entry with
      | Some _ ->
          if String.is_empty path.catalog then
            { path with catalog = (get_default t).catalog }
          else
            path
      | None ->
          if String.is_empty path.catalog then
            match Catalog.get_catalog_entry t.context path.schema with
            | Some catalog ->
                (match catalog.get_schema t.context DEFAULT_SCHEMA OnEntryNotFound.RETURN_NULL with
                | Some schema -> { catalog = path.schema; schema = schema.name }
                | None -> failwith (Printf.sprintf "%s: No catalog + schema named \"%s\" found." (get_set_name set_type) (CatalogSearchEntry.to_string path)))
            | None -> failwith (Printf.sprintf "%s: No catalog + schema named \"%s\" found." (get_set_name set_type) (CatalogSearchEntry.to_string path))
          else
            failwith (Printf.sprintf "%s: No catalog + schema named \"%s\" found." (get_set_name set_type) (CatalogSearchEntry.to_string path))
    ) in

    if set_type = CatalogSetPathType.SET_SCHEMA then
      match List.hd validated_paths with
      | Some path when path.catalog = TEMP_CATALOG || path.catalog = SYSTEM_CATALOG ->
          failwith (Printf.sprintf "%s cannot be set to internal schema \"%s\"" (get_set_name set_type) path.catalog)
      | _ -> ()
    ;

    t.set_paths <- validated_paths;
    set_paths t t.set_paths

  let set_single t new_value set_type =
    set t [new_value] set_type

  let get t = t.paths

  let get_default_schema t catalog =
    List.find_map t.paths ~f:(fun path ->
      if path.catalog <> TEMP_CATALOG && String.Caseless.equal path.catalog catalog then
        Some path.schema
      else
        None
    )
    |> Option.value ~default:DEFAULT_SCHEMA

  let get_default_catalog t schema =
    List.find_map t.paths ~f:(fun path ->
      if path.catalog <> TEMP_CATALOG && String.Caseless.equal path.schema schema then
        Some path.catalog
      else
        None
    )
    |> Option.value ~default:INVALID_CATALOG

  let get_catalogs_for_schema t schema =
    List.filter_map t.paths ~f:(fun path ->
      if String.Caseless.equal path.schema schema then
        Some path.catalog
      else
        None
    )

  let get_schemas_for_catalog t catalog =
    List.filter_map t.paths ~f:(fun path ->
      if String.Caseless.equal path.catalog catalog then
        Some path.schema
      else
        None
    )

  let get_default t =
    match List.nth t.paths 1 with
    | Some path -> path
    | None -> failwith "Invalid catalog search path"

  let set_paths t new_paths =
    t.paths <- 
      CatalogSearchEntry.create TEMP_CATALOG DEFAULT_SCHEMA ::
      new_paths @
      [CatalogSearchEntry.create INVALID_CATALOG DEFAULT_SCHEMA;
       CatalogSearchEntry.create SYSTEM_CATALOG DEFAULT_SCHEMA;
       CatalogSearchEntry.create SYSTEM_CATALOG "pg_catalog"]

  let schema_in_search_path t catalog_name schema_name =
    List.exists t.paths ~f:(fun path ->
      String.Caseless.equal path.schema schema_name &&
      (String.Caseless.equal path.catalog catalog_name ||
       (is_invalid_catalog path.catalog &&
        String.Caseless.equal catalog_name (DatabaseManager.get_default_database t.context)))
    )
end