type tuple = {
  id: int;
  values: float array;
}

let create_tuple id values =
  {id; values}

let compare_tuple dim t1 t2 =
  compare t1.values.(dim) t2.values.(dim)