open Lwt.Infix
open Lwt_unix

let handle_connection client_sockaddr client_fd =
  let in_chan = Lwt_io.of_fd ~mode:Lwt_io.input client_fd in
  let out_chan = Lwt_io.of_fd ~mode:Lwt_io.output client_fd in
  Lwt_io.read_line in_chan >>= fun request ->
  Lwt_io.printf "Received request: %s\n" request >>= fun () ->
  Lwt_io.write_line out_chan "Hello from the server!" >>= fun () ->
  Lwt_io.close in_chan >>= fun () ->
  Lwt_io.close out_chan >>= fun () ->
  Lwt_unix.close client_fd

let start_server () =
  let server_addr = Unix.inet_addr_any in
  let server_port = 8080 in
  let server_sockaddr = Unix.ADDR_INET (server_addr, server_port) in

  let socket = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Lwt_unix.setsockopt socket Unix.SO_REUSEADDR true;
  Lwt_unix.bind socket server_sockaddr;
  Lwt_unix.listen socket 5;

  let rec accept_connections () =
    Lwt_unix.accept socket >>= fun (client_fd, client_sockaddr) ->
    Lwt.async (fun () -> handle_connection client_sockaddr client_fd);
    accept_connections ()
  in

  accept_connections ()