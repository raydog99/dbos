open CreatePlanner
open ExecutorContext
open Database
open Schema
open Index

module type CreateExecutor = sig
	val create_executor : CreatePlanner -> ExecutorContext -> CreateExecutor;
    val dexecute : CreatePlanner -> ExecutorContext -> unit;
    val create_database : CreatePlanner -> Database
    val create_schema : CreatePlanner -> Schema
    val create_table : CreatePlanner -> Table
    val create_index : CreatePlanner -> Index
end