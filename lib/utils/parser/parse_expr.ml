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

(* Literal parsers *)
let integer = lexeme (take_while1 (function '0'..'9' -> true | _ -> false))
let float_number =
  lexeme (
    lift2 (fun a b -> a ^ "." ^ b)
      (take_while1 (function '0'..'9' -> true | _ -> false))
      (char '.' *> take_while1 (function '0'..'9' -> true | _ -> false))
  )
let string_literal =
  lexeme (char '\'' *> take_till (fun c -> c = '\'') <* char '\'')
let bool_literal =
  (keyword "TRUE" >>| fun _ -> true) <|> (keyword "FALSE" >>| fun _ -> false)
let null_literal = keyword "NULL" >>| fun _ -> None

(* Operator parsers *)
let arithmetic_op =
  lexeme (choice [char '+'; char '-'; char '*'; char '/'; char '%'])
let comparison_op =
  lexeme (choice [
    string "=" ; string "<>" ; string "!=" ;
    string "<" ; string "<=" ; string ">" ; string ">=" ;
    string_ci "LIKE" ; string_ci "IN" ; string_ci "BETWEEN"
  ])
let logical_op =
  lexeme (choice [string_ci "AND"; string_ci "OR"; string_ci "NOT"])

(* Function call parser *)
let function_call =
  lift2 (fun name args -> `FunctionCall (name, args))
    identifier
    (parens (sep_by comma expression))

(* CASE expression parser *)
and case_expression =
  let when_then =
    lift2 (fun when_expr then_expr -> (when_expr, then_expr))
      (keyword "WHEN" *> expression)
      (keyword "THEN" *> expression)
  in
  lift3
    (fun when_thens else_expr -> `Case (when_thens, else_expr))
    (keyword "CASE" *> many1 when_then)
    (option None (keyword "ELSE" *> expression >>| Option.some))
    (keyword "END")

(* Main expression parser *)
and expression =
  fix (fun expr ->
    let factor =
      choice [
        (integer >>| fun i -> `Integer (int_of_string i));
        (float_number >>| fun f -> `Float (float_of_string f));
        (string_literal >>| fun s -> `String s);
        (bool_literal >>| fun b -> `Bool b);
        (null_literal >>| fun _ -> `Null);
        (lift2 (fun tbl col -> `Column (tbl, col))
          (identifier <* dot)
          identifier);
        (identifier >>| fun id -> `Column (None, id));
        function_call;
        case_expression;
        (parens expr);
      ] 
    in
    let term = chainl1 factor (arithmetic_op >>| fun op a b -> `BinaryOp (op, a, b)) in
    let comparison = chainl1 term (comparison_op >>| fun op a b -> `Comparison (op, a, b)) in
    let logical = chainl1 comparison (logical_op >>| fun op a b -> `LogicalOp (op, a, b)) in
    logical
  )