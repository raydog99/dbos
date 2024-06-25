open Yojson.Safe

module RaftOperations : RPC.RPC_OPERATIONS = struct
  type request =
    | AppendEntries of AppendEntries.t
    | RequestVote of RequestVote.t

  type response =
    | AppendEntriesResponse of AppendEntriesResponse.t
    | RequestVoteResponse of RequestVoteResponse.t

  let yojson_of_request = function
    | AppendEntries append_entries -> AppendEntries.yojson_of_t append_entries
    | RequestVote request_vote -> RequestVote.yojson_of_t request_vote

  let request_of_yojson = function
    | `Assoc [("append_entries", json)] ->
        AppendEntries (AppendEntries.t_of_yojson json)
    | `Assoc [("request_vote", json)] ->
        RequestVote (RequestVote.t_of_yojson json)
    | _ -> Error "Invalid request format"

  let yojson_of_response = function
    | AppendEntriesResponse append_entries_response ->
        AppendEntriesResponse.yojson_of_t append_entries_response
    | RequestVoteResponse request_vote_response ->
        RequestVoteResponse.yojson_of_t request_vote_response

  let response_of_yojson = function
    | `Assoc [("append_entries_response", json)] ->
        AppendEntriesResponse (AppendEntriesResponse.t_of_yojson json)
    | `Assoc [("request_vote_response", json)] ->
        RequestVoteResponse (RequestVoteResponse.t_of_yojson json)
    | _ -> Error "Invalid response format"

  let handle_request = function
    | AppendEntries append_entries ->
        let response = handle_append_entries append_entries in
        AppendEntriesResponse response
    | RequestVote request_vote ->
        let response = handle_request_vote request_vote in
        RequestVoteResponse response

  and handle_append_entries append_entries =
    let current_term = 1 in
    if append_entries.term < current_term then
      { AppendEntriesResponse.success = false; term = current_term }
    else (
      { AppendEntriesResponse.success = true; term = current_term }
    )
  
  and handle_request_vote request_vote =
    let current_term = 1 in
    if request_vote.term < current_term then
      { RequestVoteResponse.vote_granted = false; term = current_term }
    else (
      { RequestVoteResponse.vote_granted = true; term = current_term }
    )
end