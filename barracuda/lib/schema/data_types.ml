type data_type =
  | CRef
  | Text
  | Int
  | Number
  | Boolean
  | Date
  | GeoCoordinates
  | PhoneNumber
  | Blob
  | TextArray
  | IntArray
  | NumberArray
  | BooleanArray
  | DateArray
  | UUID
  | UUIDArray
  | Object
  | ObjectArray
  | String  (* deprecated *)
  | StringArray  (* deprecated *)

let data_type_to_string = function
  | CRef -> "cref"
  | Text -> "text"
  | Int -> "int"
  | Number -> "number"
  | Boolean -> "boolean"
  | Date -> "date"
  | GeoCoordinates -> "geoCoordinates"
  | PhoneNumber -> "phoneNumber"
  | Blob -> "blob"
  | TextArray -> "text[]"
  | IntArray -> "int[]"
  | NumberArray -> "number[]"
  | BooleanArray -> "boolean[]"
  | DateArray -> "date[]"
  | UUID -> "uuid"
  | UUIDArray -> "uuid[]"
  | Object -> "object"
  | ObjectArray -> "object[]"
  | String -> "string"
  | StringArray -> "string[]"

let as_name dt =
  String.map (function '[' | ']' -> 'A' | c -> c) (data_type_to_string dt)

let primitive_data_types = [
  Text; Int; Number; Boolean; Date; GeoCoordinates; PhoneNumber; Blob;
  TextArray; IntArray; NumberArray; BooleanArray; DateArray; UUID; UUIDArray
]

let nested_data_types = [Object; ObjectArray]

let deprecated_primitive_data_types = [String; StringArray]

type property_kind = Primitive | Ref | Nested

type class_name = string

type property_data_type = {
  kind: property_kind;
  primitive_type: data_type option;
  classes: class_name list;
  nested_type: data_type option;
}

let is_property_length prop_name offset =
  let len = String.length prop_name in
  if len > 4 + offset &&
     String.sub prop_name offset 4 = "len(" &&
     prop_name.[len - 1] = ')' then
    Some (String.sub prop_name (offset + 4) (len - offset - 5))
  else
    None

let is_array_type = function
  | StringArray -> Some String
  | TextArray -> Some Text
  | NumberArray -> Some Number
  | IntArray -> Some Int
  | BooleanArray -> Some Boolean
  | DateArray -> Some Date
  | UUIDArray -> Some UUID
  | ObjectArray -> Some Object
  | _ -> None

let find_property_data_type get_class data_type relax_cross_ref_validation belonging_to_class =
  match data_type with
  | [] -> Error "dataType must have at least one element"
  | [dt] ->
      if List.mem dt (List.map data_type_to_string (primitive_data_types @ deprecated_primitive_data_types)) then
        Ok { kind = Primitive; primitive_type = Some (List.find (fun x -> data_type_to_string x = dt) (primitive_data_types @ deprecated_primitive_data_types)); classes = []; nested_type = None }
      else if List.mem dt (List.map data_type_to_string nested_data_types) then
        Ok { kind = Nested; primitive_type = None; classes = []; nested_type = Some (List.find (fun x -> data_type_to_string x = dt) nested_data_types) }
      else if dt = "" then
        Error "dataType cannot be an empty string"
      else if Char.lowercase_ascii dt.[0] = dt.[0] then
        Error (Printf.sprintf "unknown primitive data type '%s'" dt)
      else
        Ok { kind = Ref; primitive_type = None; classes = [dt]; nested_type = None }
  | _ ->
      let validate_class_name name =
        name
      in
      let classes = List.map validate_class_name data_type in
      if not relax_cross_ref_validation then
        List.iter (fun class_name ->
          if class_name <> belonging_to_class && get_class class_name = None then
            failwith "ErrRefToNonexistentClass"
        ) classes;
      Ok { kind = Ref; primitive_type = None; classes = classes; nested_type = None }

let as_primitive data_type =
  match data_type with
  | [dt] ->
      if List.mem dt (List.map data_type_to_string (primitive_data_types @ deprecated_primitive_data_types)) then
        Some (List.find (fun x -> data_type_to_string x = dt) (primitive_data_types @ deprecated_primitive_data_types))
      else if dt = "" then
        Some Text
      else
        None
  | _ -> None

let as_nested data_type =
  match data_type with
  | [dt] ->
      if List.mem dt (List.map data_type_to_string nested_data_types) then
        Some (List.find (fun x -> data_type_to_string x = dt) nested_data_types)
      else
        None
  | _ -> None

let is_nested dt =
  List.mem dt nested_data_types