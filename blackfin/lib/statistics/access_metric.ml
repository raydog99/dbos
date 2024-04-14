open CounterMetric

module AccessMetric = struct
  type t = {
    access_counters : CounterMetric.t array;
  }

  let make () =
    let counters = Array.init 4 (fun _ -> CounterMetric.make Counter) in
    { access_counters = counters }

  let increment_reads metric =
    metric.access_counters.(0) <- CounterMetric.increment metric.access_counters.(0)

  let increment_updates metric =
    metric.access_counters.(1) <- CounterMetric.increment metric.access_counters.(1)

  let increment_inserts metric =
    metric.access_counters.(2) <- CounterMetric.increment metric.access_counters.(2)

  let increment_deletes metric =
    metric.access_counters.(3) <- CounterMetric.increment metric.access_counters.(3)

  let increment_reads_by metric count =
    metric.access_counters.(0) <- CounterMetric.increment_by metric.access_counters.(0) count

  let increment_updates_by metric count =
    metric.access_counters.(1) <- CounterMetric.increment_by metric.access_counters.(1) count

  let increment_inserts_by metric count =
    metric.access_counters.(2) <- CounterMetric.increment_by metric.access_counters.(2) count

  let increment_deletes_by metric count =
    metric.access_counters.(3) <- CounterMetric.increment_by metric.access_counters.(3) count

  let get_reads metric = CounterMetric.get_counter metric.access_counters.(0)
  let get_updates metric = CounterMetric.get_counter metric.access_counters.(1)
  let get_inserts metric = CounterMetric.get_counter metric.access_counters.(2)
  let get_deletes metric = CounterMetric.get_counter metric.access_counters.(3)

  let get_access_counter metric index = metric.access_counters.(index)

  let reset metric =
    Array.iter CounterMetric.reset metric.access_counters

  let get_info metric =
    let reads = CounterMetric.get_info metric.access_counters.(0) in
    let updates = CounterMetric.get_info metric.access_counters.(1) in
    let inserts = CounterMetric.get_info metric.access_counters.(2) in
    let deletes = CounterMetric.get_info metric.access_counters.(3) in
    "[ reads=" ^ reads ^ ", updates=" ^ updates ^ ", inserts=" ^ inserts ^ ", deletes=" ^ deletes ^ " ]"
end