//// tern/server — a wisp/mist HTTP API over a `StorageBackend`.
////
////   GET  /health          → liveness
////   POST /v1/events       → ingest a lineage event (one atomic transaction)
////   GET  /v1/graph?...     → temporal traversal, as JSON
////
//// SSE streaming joins in M4.5 (it needs the streaming backend method).

import gleam/dict
import gleam/dynamic/decode.{type Decoder}
import gleam/http.{Get, Post}
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp.{type Timestamp}
import mist
import tern/core/event.{
  type LineageEvent, type Link, type Links, type Op, ConsumerLinks, Create,
  Delete, EntityLinks, LineageEvent, Link, OperationLinks, OriginLinks, Update,
}
import tern/core/model.{
  type Edge, type Node, type NodeRole, type Tenant, Consumer, Entity, Identity,
  Operation, Origin, Tenant,
}
import tern/core/storage.{
  type StorageBackend, type TimelineQuery, Both, Downstream, TimelineQuery,
  Upstream,
}
import tern/ingest
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
      ["health"], Get -> wisp.json_response("{\"status\":\"ok\"}", 200)
      ["v1", "events"], Post -> ingest(req, store)
      ["v1", "graph"], Get -> graph(req, store)
      _, _ -> wisp.not_found()
    }
  }
}

// --- POST /v1/events -------------------------------------------------------

fn ingest(req: Request, store: StorageBackend) -> Response {
  use body <- wisp.require_json(req)
  case decode.run(body, event_decoder()) {
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

// --- GET /v1/graph ---------------------------------------------------------

fn graph(req: Request, store: StorageBackend) -> Response {
  let q = wisp.get_query(req)
  case build_query(q) {
    Error(msg) -> error(400, msg)
    Ok(query) ->
      case store.query_at_time(query) {
        Ok(g) -> wisp.json_response(graph_json(g), 200)
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
    model.role_from_label(capitalize(role_s))
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

// --- JSON: event in --------------------------------------------------------

/// Decodes a request body into `(Tenant, LineageEvent)`. Wire shape:
/// `{ "tenant", "role", "externalId", "kind", "name", "operation",
///    "occurredAt", "append"?, "sources"?, "targets"? }`
fn event_decoder() -> Decoder(#(Tenant, LineageEvent)) {
  use tenant <- decode.field("tenant", decode.string)
  use role <- decode.field("role", role_decoder())
  use external_id <- decode.field("externalId", decode.string)
  use kind <- decode.field("kind", decode.string)
  use name <- decode.field("name", decode.string)
  use op <- decode.field("operation", op_decoder())
  use occurred <- decode.field("occurredAt", decode.int)
  use append <- decode.optional_field("append", False, decode.bool)
  use sources <- decode.optional_field(
    "sources",
    [],
    decode.list(link_decoder()),
  )
  use targets <- decode.optional_field(
    "targets",
    [],
    decode.list(link_decoder()),
  )
  decode.success(#(
    Tenant(tenant),
    LineageEvent(
      role: role,
      external_id: external_id,
      kind: kind,
      name: name,
      operation: op,
      links: links_for(role, sources, targets),
      append: append,
      occurred_at: timestamp.from_unix_seconds(occurred),
      properties: dict.new(),
    ),
  ))
}

fn links_for(
  role: NodeRole,
  sources: List(Link),
  targets: List(Link),
) -> Links {
  case role {
    Origin -> OriginLinks(targets)
    Consumer -> ConsumerLinks(sources)
    Operation -> OperationLinks(sources, targets)
    Entity -> EntityLinks
  }
}

fn link_decoder() -> Decoder(Link) {
  use external_id <- decode.field("externalId", decode.string)
  use kind <- decode.field("kind", decode.string)
  use columns <- decode.optional_field(
    "columns",
    [],
    decode.list(decode.string),
  )
  decode.success(Link(external_id, kind, columns))
}

fn role_decoder() -> Decoder(NodeRole) {
  use s <- decode.then(decode.string)
  case model.role_from_label(capitalize(s)) {
    Some(r) -> decode.success(r)
    None -> decode.failure(Entity, "NodeRole")
  }
}

fn op_decoder() -> Decoder(Op) {
  use s <- decode.then(decode.string)
  case s {
    "create" -> decode.success(Create)
    "update" -> decode.success(Update)
    "delete" -> decode.success(Delete)
    _ -> decode.failure(Create, "Op")
  }
}

// --- JSON: graph out -------------------------------------------------------

fn graph_json(g: storage.PagedGraph) -> String {
  json.object([
    #("nodes", json.array(g.nodes, node_json)),
    #("edges", json.array(g.edges, edge_json)),
    #("total", json.int(g.total)),
    #("page", json.int(g.page)),
  ])
  |> json.to_string
}

fn node_json(n: Node) -> json.Json {
  let model.NodeId(id) = n.id
  json.object([
    #("nodeId", json.string(id)),
    #("externalId", json.string(n.external_id)),
    #("kind", json.string(n.kind)),
    #("role", json.string(model.role_label(n.role))),
    #("name", json.string(n.name)),
    #("validFrom", json.int(unix(n.valid_from))),
    #("deletedAt", null_or_int(n.deleted_at)),
  ])
}

fn edge_json(e: Edge) -> json.Json {
  let model.NodeId(from) = e.from
  let model.NodeId(to) = e.to
  json.object([
    #("from", json.string(from)),
    #("to", json.string(to)),
    #("label", json.string(e.label)),
    #("validFrom", json.int(unix(e.valid_from))),
    #("deletedAt", null_or_int(e.deleted_at)),
  ])
}

// --- helpers ---------------------------------------------------------------

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

fn null_or_int(t: Option(Timestamp)) -> json.Json {
  case t {
    Some(ts) -> json.int(unix(ts))
    None -> json.null()
  }
}

fn unix(t: Timestamp) -> Int {
  let #(secs, _) = timestamp.to_unix_seconds_and_nanoseconds(t)
  secs
}

fn capitalize(s: String) -> String {
  case string.pop_grapheme(s) {
    Ok(#(head, tail)) -> string.uppercase(head) <> string.lowercase(tail)
    Error(_) -> s
  }
}
