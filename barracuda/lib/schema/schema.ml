module Schema = struct
  type class_name = string
  type property_name = string

  type class_and_property = {
    class_name: class_name;
    property_name: property_name;
  }

  module Models = struct
    type property = {
      name: string;
    }

    type class_ = {
      name: class_name;
      properties: property list;
    }

    type schema = {
      classes: class_ list;
    }
  end

  type t = {
    objects: Models.schema;
  }

  let empty () = {
    objects = { Models.classes = [] };
  }

  let semantic_schema_for schema = schema.objects

  let uppercase_class_name name =
    match String.length name with
    | 0 -> name
    | 1 -> String.uppercase_ascii name
    | _ -> 
        String.capitalize_ascii name

  let lowercase_all_property_names props =
    List.map (fun prop -> 
      { prop with Models.name = String.uncapitalize_ascii prop.Models.name }
    ) props

  let lowercase_first_letter name =
    match String.length name with
    | 0 -> name
    | 1 -> String.lowercase_ascii name
    | _ -> 
        (String.lowercase_ascii (String.sub name 0 1)) ^ (String.sub name 1 (String.length name - 1))

  let lowercase_first_letter_of_strings strings =
    List.map lowercase_first_letter strings
end