open Core
open Database_instance
open Client_context
open Transaction

module CatalogTransaction = struct
  type t = {
    db : DatabaseInstance.t option;
    context : ClientContext.t option;
    transaction : Transaction.t option;
    transaction_id : Int64.t;
    start_time : Int64.t;
  }

  let create catalog context =
    let transaction = Transaction.get context catalog in
    let db = Some (DatabaseInstance.get_database context) in
    if not (Transaction.is_duck_transaction transaction) then
      { db; context = Some context; transaction = Some transaction; transaction_id = -1L; start_time = -1L }
    else
      let dtransaction = Transaction.cast_duck_transaction transaction in
      { db;
        context = Some context;
        transaction = Some transaction;
        transaction_id = BluefinTransaction.transaction_id dtransaction;
        start_time = BluefinTransaction.start_time dtransaction }

  let create_from_db db transaction_id_p start_time_p =
    { db = Some db;
      context = None;
      transaction = None;
      transaction_id = transaction_id_p;
      start_time = start_time_p }

  let get_context t =
    match t.context with
    | Some context -> context
    | None -> failwith "Attempting to get a context in a CatalogTransaction without a context"

  let get_system_catalog_transaction context =
    create (Catalog.get_system_catalog context) context

  let get_system_transaction db =
    create_from_db db 1L 1L

  let has_context t =
    Option.is_some t.context
end