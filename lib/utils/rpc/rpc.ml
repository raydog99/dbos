open Yojson.Safe

module type RPC_OPERATIONS = sig
  type request
  type response

  val yojson_of_request : request -> Yojson.Safe.t
  val request_of_yojson : Yojson.Safe.t -> (request, string) result

  val yojson_of_response : response -> Yojson.Safe.t
  val response_of_yojson : Yojson.Safe.t -> (response, string) result

  val handle_request : request -> response
end

module MakeRPC (Ops : RPC_OPERATIONS) = struct
  type rpc_request = Ops.request
  type rpc_response = Ops.response

  let yojson_of_rpc_request request =
    Ops.yojson_of_request request

  let rpc_request_of_yojson json =
    Ops.request_of_yojson json

  let yojson_of_rpc_response response =
    Ops.yojson_of_response response

  let rpc_response_of_yojson json =
    Ops.response_of_yojson json

  let handle_request request =
    Ops.handle_request request
end