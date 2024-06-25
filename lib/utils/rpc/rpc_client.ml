open Lwt
open Unix

module Rpc = Rpc.MakeRPC(Operations)

let rpc_client server_ip server_port request =
  Lwt_unix.getaddrinfo server_ip (string_of_int server_port) [Unix.AI_FAMILY Unix.PF_INET] >>= fun addresses ->
  let sockaddr = (List.hd addresses).Unix.ai_addr in
  Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 >>= fun client_sock ->
  Lwt_unix.connect client_sock sockaddr >>= fun () ->
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output client_sock in
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input client_sock in
  let request_json = Yojson.Safe.to_string (Rpc.yojson_of_rpc_request request) in
  Lwt_io.write_line oc request_json >>= fun () ->
  Lwt_io.read_line ic >>= fun response_json ->
  Lwt_io.close ic >>= fun () ->
  Lwt_io.close oc >>= fun () ->
  let response = Yojson.Safe.from_string response_json |> Rpc.rpc_response_of_yojson in
  Lwt.return response
