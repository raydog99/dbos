module Schema = struct
  type ('ty, 'en, 'sym, 'fk, 'att) schema = {
    type_side: 'ty type_side;
    ens: 'en list;
    atts: ('att * ('en * 'ty)) list;
    fks: ('fk * ('en * 'en)) list;
    eqs: (('string * 'en) * ('ty, 'en, 'sym, 'fk, 'att) term * ('ty, 'en, 'sym, 'fk, 'att) term) list;
  }

  type ('ty, 'en, 'sym, 'fk, 'att) term =
    | Var of string
    | Fk of 'fk * ('ty, 'en, 'sym, 'fk, 'att) term
    | Att of 'att * ('ty, 'en, 'sym, 'fk, 'att) term
    | Sym of 'sym * ('ty, 'en, 'sym, 'fk, 'att) term list

  let size schema =
    List.length schema.ens + List.length schema.atts + List.length schema.fks + List.length schema.eqs

  let kind () = "SCHEMA"

  let discretize schema ensX =
    { schema with ens = ensX; atts = []; fks = [] }

  let validate schema =
    List.iter (fun (att, (en, ty)) ->
      if not (List.exists ((=) ty) schema.type_side.tys) then
        failwith ("On attribute " ^ att ^ ", the target type " ^ ty ^ " is not declared.")
      else if not (List.exists ((=) en) schema.ens) then
        failwith ("On attribute " ^ att ^ ", the source entity " ^ en ^ " is not declared.")
    ) schema.atts;

    List.iter (fun (fk, (en1, en2)) ->
      if not (List.exists ((=) en2) schema.ens) then
        failwith ("On foreign key " ^ fk ^ ", the target entity " ^ en2 ^ " is not declared.")
      else if not (List.exists ((=) en1) schema.ens) then
        failwith ("On foreign key " ^ fk ^ ", the source entity " ^ en1 ^ " is not declared.")
    ) schema.fks

  let to_string schema =
    let ens_str = String.concat " " schema.ens in
    let atts_str = String.concat " " (List.map fst schema.atts) in
    let fks_str = String.concat " " (List.map fst schema.fks) in
    let eqs_str = String.concat " " (List.map (fun ((_, en), t1, t2) -> en ^ " : " ^ (string_of_term t1) ^ " = " ^ (string_of_term t2)) schema.eqs) in
    "Entities: " ^ ens_str ^ "\nAttributes: " ^ atts_str ^ "\nForeign Keys: " ^ fks_str ^ "\nEquations: " ^ eqs_str

  and string_of_term = function
    | Var v -> v
    | Fk (fk, t) -> fk ^ "(" ^ (string_of_term t) ^ ")"
    | Att (att, t) -> att ^ "(" ^ (string_of_term t) ^ ")"
    | Sym (sym, ts) -> sym ^ "(" ^ (String.concat ", " (List.map string_of_term ts)) ^ ")"

  let acyclic schema =
    let dag = Dag.create () in
    List.iter (fun (fk, (en1, en2)) ->
      if not (Dag.add_edge dag en1 en2) then
        Some ("Adding dependency on " ^ fk ^ " causes circularity " ^ (Dag.to_string dag))
      else
        None
    ) schema.fks

end