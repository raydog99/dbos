type MetricType = Index

module type IndexMetricParams : sig
  val metricType : MetricType
  val database_id : int
  val table_id : int
  val index_id : int
end

module IndexMetric = struct
  type t = {
    database_id : int;
    table_id : int;
    index_id : int;
    index_name: string;
  }
end