open Angstrom

(* Utility parsers *)
let whitespace = take_while (function ' ' | '\t' | '\n' -> true | _ -> false)
let token s = string s <* whitespace
let keyword s = string_ci s <* whitespace
let lexeme p = p <* whitespace

let identifier =
  lexeme (take_while1 (function
    | 'a'..'z' | 'A'..'Z' | '0'..'9' | '_' -> true
    | _ -> false))

let comma = lexeme (char ',')
let dot = lexeme (char '.')
let parens p = lexeme (char '(') *> p <* lexeme (char ')')

(* Forward declaration for subquery *)
let subquery = ref (fun _ -> assert false)

(* Basic table reference *)
let table_name =
  lift2 (fun schema name -> `Table (schema, name))
    (option None (identifier <* dot >>| Option.some))
    identifier

(* Table alias *)
let alias =
  option None (keyword "AS" *> identifier >>| Option.some)

(* Column list for derived tables *)
let column_list =
  parens (sep_by1 comma identifier)

(* Derived table (subquery) *)
let derived_table =
  lift3 (fun subq alias cols -> `DerivedTable (subq, alias, cols))
    (parens !subquery)
    alias
    (option None (column_list >>| Option.some))

(* Table-valued function *)
let table_valued_function =
  lift3 (fun name args alias -> `TableValuedFunction (name, args, alias))
    identifier
    (parens (sep_by comma expression))
    alias

(* Join types *)
let join_type =
  choice [
    keyword "INNER" *> return `Inner;
    keyword "LEFT" *> option `Left (keyword "OUTER" *> return `LeftOuter);
    keyword "RIGHT" *> option `Right (keyword "OUTER" *> return `RightOuter);
    keyword "FULL" *> option `Full (keyword "OUTER" *> return `FullOuter);
    keyword "CROSS" *> return `Cross;
    return `Inner  (* Default to INNER if no type specified *)
  ]

(* Join condition *)
let join_condition =
  choice [
    keyword "ON" *> expression;
    keyword "USING" *> parens (sep_by1 comma identifier) >>| fun cols -> `Using cols
  ]

(* Join clause *)
let join_clause relation =
  lift4 (fun jtype _ right_rel cond -> `Join (jtype, relation, right_rel, cond))
    join_type
    (keyword "JOIN")
    relation
    join_condition

(* Main relation parser *)
let rec relation_impl () =
  fix (fun relation ->
    let base_relation =
      choice [
        table_name;
        derived_table;
        table_valued_function;
        parens relation
      ]
    in
    let joined_relation =
      base_relation >>= fun left ->
      many (join_clause relation) >>| fun joins ->
      List.fold_left (fun acc join -> join acc) left joins
    in
    joined_relation
  )

(* Update subquery reference *)
let () = subquery := relation_impl ()

(* FROM clause parser *)
let from_clause =
  keyword "FROM" *> sep_by1 comma (relation_impl ())

(* Helper function to parse relations *)
let parse_relations input =
  match parse_string ~consume:All from_clause input with
  | Ok result -> Printf.printf "Successfully parsed relations\n"; result
  | Error msg -> failwith ("Error parsing relations: " ^ msg)