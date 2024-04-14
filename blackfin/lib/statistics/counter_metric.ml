module CounterMetric = struct
  type t = {
    mutable count : int64;
  }

  let make () = { count = 0L }

  let increment counter = counter.count <- Int64.add counter.count 1L
  let increment_by counter count = counter.count <- Int64.add counter.count count

  let decrement counter = counter.count <- Int64.sub counter.count 1L
  let decrement_by counter count = counter.count <- Int64.sub counter.count count

  let reset counter = counter.count <- 0L
  let get_counter counter = counter.count

  let equals counter1 counter2 = counter1.count = counter2.count
  let not_equals counter1 counter2 = not (equals counter1 counter2)

  let aggregate counter source =
    match source#getType with
    | Counter ->
        let source_counter = (source :> CounterMetric.t) in
        counter.count <- Int64.add counter.count source_counter.count
    | _ -> ()

  let get_info counter = Int64.to_string counter.count
end