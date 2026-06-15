//// tern/wire — JSON encode/decode for the HTTP API and the event consumer.
////
//// One place defines the event wire format and the graph response shape, so the
//// server and the consumer can't drift apart.

import gleam/dict
import gleam/dynamic/decode.{type Decoder}
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/string
import gleam/time/timestamp.{type Timestamp}
import tern/core/event.{
  type LineageEvent, type Link, type Links, type Op, ConsumerLinks, Create,
  Delete, EntityLinks, LineageEvent, Link, OperationLinks, OriginLinks, Update,
}
import tern/core/model.{
  type Edge, type Node, type NodeRole, type Tenant, Consumer, Entity, Operation,
  Origin, Tenant,
}
import tern/core/storage.{type PagedGraph}

// --- event in --------------------------------------------------------------

/// Decode `(Tenant, LineageEvent)`. Wire shape:
/// `{ "tenant", "role", "externalId", "kind", "name", "operation",
///    "occurredAt", "append"?, "sources"?, "targets"? }`
pub fn event_decoder() -> Decoder(#(Tenant, LineageEvent)) {
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

// --- graph out -------------------------------------------------------------

pub fn graph_json(g: PagedGraph) -> String {
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

/// Lower-cases a role/op string then upper-cases the first letter, so wire
/// values like `"origin"` match the `Origin` label.
pub fn capitalize(s: String) -> String {
  case string.pop_grapheme(s) {
    Ok(#(head, tail)) -> string.uppercase(head) <> string.lowercase(tail)
    Error(_) -> s
  }
}
