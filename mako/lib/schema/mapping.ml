module Mapping = struct
  type ('ty, 'en1, 'sym, 'fk1, 'att1, 'en2, 'fk2, 'att2) t = {
    ens: ('en1, 'en2) Hashtbl.t;
    atts: ('att1, string * 'en2 * ('ty, 'en2, 'sym, 'fk2, 'att2, unit, unit) Term.t) Hashtbl.t;
    fks: ('fk1, 'en2 * 'fk2 list) Hashtbl.t;
    src: ('ty, 'en1, 'sym, 'fk1, 'att1) Schema.t;
    dst: ('ty, 'en2, 'sym, 'fk2, 'att2) Schema.t;
  }

  let create ens atts fks src dst =
    let mapping = {
      ens = Hashtbl.create (List.length ens);
      atts = Hashtbl.create (List.length atts);
      fks = Hashtbl.create (List.length fks);
      src;
      dst;
    } in
    List.iter (fun (en1, en2) -> Hashtbl.add mapping.ens en1 en2) ens;
    List.iter (fun (att1, (v, en2, term)) -> Hashtbl.add mapping.atts att1 (v, en2, term)) atts;
    List.iter (fun (fk1, (en2, fk2_list)) -> Hashtbl.add mapping.fks fk1 (en2, fk2_list)) fks;
    mapping

  let validate mapping =
    true

  let trans_term mapping term =
    let rec aux = function
      | Term.Var v -> Term.Var v
      | Term.Obj (o, ty) -> Term.Obj (o, ty)
      | Term.Gen g -> Term.Gen g
      | Term.Sk s -> Term.Sk s
      | Term.Fk (fk1, arg) ->
          let en2, fk2_list = Hashtbl.find mapping.fks fk1 in
          List.fold_left (fun acc fk2 -> Term.Fk (fk2, acc)) (aux arg) fk2_list
      | Term.Att (att1, arg) ->
          let v, en2, term = Hashtbl.find mapping.atts att1 in
          Term.subst term v (aux arg)
      | Term.Sym (sym, args) ->
          Term.Sym (sym, List.map aux args)
    in
    aux term

  let compose m1 m2 =
    let ens0 = Hashtbl.create (Hashtbl.length m1.ens) in
    Hashtbl.iter (fun en1 en2 ->
      Hashtbl.add ens0 en1 (Hashtbl.find m2.ens en2)
    ) m1.ens;

    let fks0 = Hashtbl.create (Hashtbl.length m1.fks) in
    Hashtbl.iter (fun fk1 (en2, fk2_list) ->
      let en3 = Hashtbl.find m2.ens en2 in
      let fk3_list = List.concat_map (fun fk2 ->
        let _, fk3_list = Hashtbl.find m2.fks fk2 in
        fk3_list
      ) fk2_list in
      Hashtbl.add fks0 fk1 (en3, fk3_list)
    ) m1.fks;

    let atts0 = Hashtbl.create (Hashtbl.length m1.atts) in
    Hashtbl.iter (fun att1 (v, en2, term) ->
      let en3 = Hashtbl.find m2.ens en2 in
      let term' = trans_term m2 term in
      Hashtbl.add atts0 att1 (v, en3, term')
    ) m1.atts;

    create
      (Hashtbl.to_seq ens0 |> List.of_seq)
      (Hashtbl.to_seq atts0 |> List.of_seq)
      (Hashtbl.to_seq fks0 |> List.of_seq)
      m1.src
      m2.dst

  let id schema =
    let ens = Schema.ens schema |> List.map (fun en -> (en, en)) in
    let fks = Schema.fks schema |> List.map (fun (fk, (en1, en2)) -> (fk, (en2, [fk]))) in
    let atts = Schema.atts schema |> List.map (fun (att, (en, ty)) ->
      (att, ("v", en, Term.Att (att, Term.Var "v")))
    ) in
    create ens atts fks schema schema

  let to_string mapping =
    let buffer = Buffer.create 256 in
    Hashtbl.iter (fun en1 en2 ->
      Buffer.add_string buffer (Printf.sprintf "\n\nentity\n\t%s -> %s" (Schema.string_of_en en1) (Schema.string_of_en en2));
      let fks0 = Schema.fks_from mapping.src en1 |> List.map (fun fk1 ->
        let en2, fk2_list = Hashtbl.find mapping.fks fk1 in
        Printf.sprintf "%s -> %s" (Schema.string_of_fk fk1)
          (if fk2_list = [] then "identity " ^ (Schema.string_of_en en2)
           else String.concat "." (List.map Schema.string_of_fk fk2_list))
      ) in
      let atts0 = Schema.atts_from mapping.src en1 |> List.map (fun att1 ->
        let v, en2, term = Hashtbl.find mapping.atts att1 in
        Printf.sprintf "%s -> lambda %s:%s. %s"
          (Schema.string_of_att att1)
          v
          (Schema.string_of_en en2)
          (Term.to_string term)
      ) in
      if fks0 <> [] then
        Buffer.add_string buffer (Printf.sprintf "\nforeign_keys\n\t%s" (String.concat "\n\t" fks0));
      if atts0 <> [] then
        Buffer.add_string buffer (Printf.sprintf "\nattributes\n\t%s" (String.concat "\n\t" atts0));
    ) mapping.ens;
    Buffer.contents buffer
end