open Lwt
open Unix

module Rpc = MakeRPC(Operations)

let handle_client client_sock =
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input client_sock in
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output client_sock in
  let rec process_request () =
    Lwt_io.read_line_opt ic >>= function
    | Some request_json ->
        (try
          let rpc_request = Yojson.Safe.from_string request_json |> Rpc.rpc_request_of_yojson in
          match rpc_request with
          | Ok request ->
              let response = Rpc.handle_request request in
              let response_json = Rpc.yojson_of_rpc_response response |> Yojson.Safe.to_string in
              Lwt_io.write_line oc response_json >>= fun () ->
              process_request ()
          | Error msg ->
              let error_response = Rpc.Error ("Invalid request format: " ^ msg) in
              let error_json = Rpc.yojson_of_rpc_response error_response |> Yojson.Safe.to_string in
              Lwt_io.write_line oc error_json >>= fun () ->
              process_request ()
        with
        | exn ->
            let error_response = Rpc.Error (Printexc.to_string exn) in
            let error_json = Rpc.yojson_of_rpc_response error_response |> Yojson.Safe.to_string in
            Lwt_io.write_line oc error_json >>= fun () ->
            process_request ()
        )
    | None -> Lwt.return ()
  in
  Lwt.catch process_request
    (fun exn ->
      Lwt_io.eprintf "Error handling client connection: %s\n%!" (Printexc.to_string exn)
    )
  >>= fun () ->
  Lwt_io.close ic >>= fun () ->
  Lwt_io.close oc >>= fun () ->
  Lwt.return ()

let rpc_server port =
  let sockaddr = ADDR_INET (inet_addr_any, port) in
  let server_sock = socket PF_INET SOCK_STREAM 0 in
  bind server_sock sockaddr;
  listen server_sock 5;
  let rec accept_clients () =
    let client_sock, _ = accept server_sock in
    Lwt.async (fun () -> handle_client client_sock);
    accept_clients ()
  in
  Lwt_main.run (accept_clients ())