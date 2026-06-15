//// tern/server — a wisp/mist HTTP API over a `StorageBackend`.
////
////   GET  /health          → liveness
////   POST /v1/events       → ingest a lineage event (one atomic transaction)
////   GET  /v1/graph?...     → temporal traversal, as JSON
////
//// SSE streaming joins in M4.5 (it needs the streaming backend method).

import gleam/dynamic/decode
import gleam/http.{Get, Post}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp
import mist
import simplifile
import tern/core/model.{Identity, Tenant}
import tern/core/storage.{
  type StorageBackend, type TimelineQuery, Both, Downstream, TimelineQuery,
  Upstream,
}
import tern/ingest
import tern/wire
import wisp.{type Request, type Response}
import wisp/wisp_mist

/// Start the HTTP server on `port`. Blocks (returns the mist actor result).
pub fn start(store: StorageBackend, port: Int) {
  let secret = wisp.random_string(64)
  wisp_mist.handler(handler(store), secret)
  |> mist.new
  |> mist.port(port)
  |> mist.start
}

/// The wisp request handler, ready to mount or test.
pub fn handler(store: StorageBackend) -> fn(Request) -> Response {
  fn(req) {
    case wisp.path_segments(req), req.method {
      [], Get -> explorer()
      ["health"], Get -> wisp.json_response("{\"status\":\"ok\"}", 200)
      ["v1", "events"], Post -> ingest(req, store)
      ["v1", "graph"], Get -> graph(req, store)
      _, _ -> wisp.not_found()
    }
  }
}

/// Serve the bundled time-travel explorer (priv/explorer.html), same-origin.
fn explorer() -> Response {
  case simplifile.read("priv/explorer.html") {
    Ok(html) -> wisp.html_response(html, 200)
    Error(_) -> wisp.not_found()
  }
}

fn ingest(req: Request, store: StorageBackend) -> Response {
  use body <- wisp.require_json(req)
  case decode.run(body, wire.event_decoder()) {
    Error(_) -> error(400, "invalid event payload")
    Ok(#(tenant, ev)) -> {
      let _ = store.ensure_ready(tenant)
      case store.write(tenant, fn(s) { ingest.apply(s, ev) }) {
        Ok(_) -> wisp.json_response("{\"status\":\"accepted\"}", 202)
        Error(e) -> error(500, "write failed: " <> string.inspect(e))
      }
    }
  }
}

fn graph(req: Request, store: StorageBackend) -> Response {
  case build_query(wisp.get_query(req)) {
    Error(msg) -> error(400, msg)
    Ok(query) ->
      case store.query_at_time(query) {
        Ok(g) -> wisp.json_response(wire.graph_json(g), 200)
        Error(e) -> error(500, "query failed: " <> string.inspect(e))
      }
  }
}

fn build_query(q: List(#(String, String))) -> Result(TimelineQuery, String) {
  use tenant <- result.try(required(q, "tenant"))
  use external_id <- result.try(required(q, "externalId"))
  use kind <- result.try(required(q, "kind"))
  use role_s <- result.try(required(q, "role"))
  use role <- result.try(
    model.role_from_label(wire.capitalize(role_s))
    |> option.to_result("unknown role: " <> role_s),
  )
  let at = case get(q, "at") {
    Some(s) ->
      int.parse(s)
      |> result.map(timestamp.from_unix_seconds)
      |> result.unwrap(timestamp.system_time())
    None -> timestamp.system_time()
  }
  Ok(TimelineQuery(
    tenant: Tenant(tenant),
    root: Identity(external_id, kind, role),
    at: at,
    direction: parse_direction(get(q, "direction")),
    max_depth: int_or(get(q, "depth"), 5),
    page: int_or(get(q, "page"), 0),
    page_size: int_or(get(q, "pageSize"), 100),
  ))
}

fn parse_direction(s: Option(String)) -> storage.Direction {
  case s {
    Some("upstream") -> Upstream
    Some("downstream") -> Downstream
    _ -> Both
  }
}

fn error(status: Int, message: String) -> Response {
  wisp.json_response(
    json.object([#("error", json.string(message))]) |> json.to_string,
    status,
  )
}

fn get(q: List(#(String, String)), key: String) -> Option(String) {
  list.key_find(q, key) |> option.from_result
}

fn required(q: List(#(String, String)), key: String) -> Result(String, String) {
  get(q, key) |> option.to_result("missing query parameter: " <> key)
}

fn int_or(s: Option(String), default: Int) -> Int {
  case s {
    Some(v) -> int.parse(v) |> result.unwrap(default)
    None -> default
  }
}
