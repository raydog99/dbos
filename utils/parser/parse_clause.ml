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
let parens p = lexeme (char '(') *> p <* lexeme (char ')')

(* SELECT clause *)
let select_clause =
  keyword "SELECT" *>
  (keyword "DISTINCT" *> return true <|> return false) >>= fun distinct ->
  sep_by1 comma expression >>| fun exprs ->
  (distinct, exprs)

(* FROM clause *)
let table_reference =
  lift2 (fun name alias -> (name, Option.map (fun a -> String.trim a) alias))
    identifier
    (option None (keyword "AS" *> identifier >>| Option.some))

let from_clause =
  keyword "FROM" *>
  sep_by1 comma table_reference

(* WHERE clause *)
let comparison_operator =
  choice [
    string "=" ; string "<>" ; string "<" ; string "<=" ; string ">" ; string ">=" ;
    string_ci "LIKE" ; string_ci "IN" ; string_ci "BETWEEN"
  ] <* whitespace

let condition =
  lift3
    (fun left op right -> Printf.sprintf "%s %s %s" left op right)
    expression
    comparison_operator
    expression

let where_clause =
  keyword "WHERE" *>
  sep_by1 (keyword "AND") condition

(* GROUP BY clause *)
let group_by_clause =
  keyword "GROUP" *> keyword "BY" *>
  sep_by1 comma expression

(* HAVING clause *)
let having_clause =
  keyword "HAVING" *>
  sep_by1 (keyword "AND") condition

(* ORDER BY clause *)
let order_direction =
  option "ASC" (keyword "ASC" <|> keyword "DESC")

let order_by_item =
  lift2 (fun expr dir -> (expr, dir))
    expression
    order_direction

let order_by_clause =
  keyword "ORDER" *> keyword "BY" *>
  sep_by1 comma order_by_item

(* LIMIT clause *)
let limit_clause =
  keyword "LIMIT" *>
  lift2 (fun limit offset -> (int_of_string limit, Option.map int_of_string offset))
    (take_while1 (function '0'..'9' -> true | _ -> false))
    (option None (keyword "OFFSET" *> take_while1 (function '0'..'9' -> true | _ -> false) >>| Option.some))

(* Combine all clauses *)
let select_statement =
  lift6
    (fun (distinct, select_exprs) from_tables where_conds group_by_exprs having_conds order_by_items ->
      {
        distinct;
        select_exprs;
        from_tables;
        where_conds;
        group_by_exprs;
        having_conds;
        order_by_items;
      })
    select_clause
    from_clause
    (option [] where_clause)
    (option [] group_by_clause)
    (option [] having_clause)
    (option [] order_by_clause)