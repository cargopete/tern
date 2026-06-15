//// tern/core/model — the generic lineage graph model.
////
//// Pure data + temporal logic, no I/O. This is the vocabulary every backend
//// and the server speak. Deliberately free of any Fathom-specific identifiers:
//// a node's identity is just `(external_id, kind, role)`.

import gleam/dict.{type Dict}
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/order
import gleam/string
import gleam/time/timestamp.{type Timestamp}

/// The four roles a lineage node can play. Each maps to its own AGE label
/// (AGE has no multi-labels), e.g. `Origin`.
pub type NodeRole {
  Origin
  Entity
  Operation
  Consumer
}

pub fn role_label(role: NodeRole) -> String {
  case role {
    Origin -> "Origin"
    Entity -> "Entity"
    Operation -> "Operation"
    Consumer -> "Consumer"
  }
}

pub fn role_from_label(label: String) -> Option(NodeRole) {
  case label {
    "Origin" -> Some(Origin)
    "Entity" -> Some(Entity)
    "Operation" -> Some(Operation)
    "Consumer" -> Some(Consumer)
    _ -> None
  }
}

/// An internally-assigned node id (a UUID string in practice). Generation is an
/// effect, so it happens in the backend layer — core stays pure.
pub type NodeId {
  NodeId(String)
}

/// A property value. A small, comparable JSON-ish union keeps core testable and
/// decoupled from any JSON library; the backend maps it to/from agtype.
pub type PropValue {
  S(String)
  I(Int)
  F(Float)
  B(Bool)
  Null
}

/// A tenant boundary. Each tenant gets its own AGE graph.
pub type Tenant {
  Tenant(namespace: String)
}

/// Deterministic AGE graph name for a tenant. AGE graph names must be valid
/// identifiers, so the namespace is slugified and prefixed `tern_`. (A uuid5
/// hash can replace `slug` later without changing any caller.)
pub fn graph_name(tenant: Tenant) -> String {
  "tern_" <> slug(tenant.namespace)
}

fn slug(s: String) -> String {
  s
  |> string.lowercase
  |> string.to_graphemes
  |> list.fold("", fn(acc, c) {
    case is_ident_char(c) {
      True -> acc <> c
      False -> acc <> "_"
    }
  })
}

fn is_ident_char(c: String) -> Bool {
  string.contains("abcdefghijklmnopqrstuvwxyz0123456789_", c)
}

/// The merge identity of a node: two upserts with the same identity are the
/// same node. Backed by a per-label unique constraint in the AGE backend.
pub type Identity {
  Identity(external_id: String, kind: String, role: NodeRole)
}

/// A node in the lineage graph, with its temporal validity window.
pub type Node {
  Node(
    id: NodeId,
    external_id: String,
    kind: String,
    role: NodeRole,
    name: String,
    properties: Dict(String, PropValue),
    valid_from: Timestamp,
    deleted_at: Option(Timestamp),
  )
}

/// A directed edge (`flows_into` and friends), also temporally bounded.
pub type Edge {
  Edge(
    from: NodeId,
    to: NodeId,
    label: String,
    valid_from: Timestamp,
    deleted_at: Option(Timestamp),
  )
}

pub fn node_identity(node: Node) -> Identity {
  Identity(node.external_id, node.kind, node.role)
}

/// Is a temporal record live at instant `at`?
///
/// Live = it came into being at or before `at`, and had not yet been deleted as
/// of `at` (a deletion that happens strictly after `at` does not affect it).
/// This is the predicate every `as-of(T)` query is built on.
pub fn is_live_at(
  valid_from: Timestamp,
  deleted_at: Option(Timestamp),
  at: Timestamp,
) -> Bool {
  let created = case timestamp.compare(valid_from, at) {
    order.Gt -> False
    _ -> True
  }
  let not_deleted = case deleted_at {
    None -> True
    Some(d) ->
      case timestamp.compare(d, at) {
        order.Gt -> True
        _ -> False
      }
  }
  created && not_deleted
}

pub fn node_live_at(node: Node, at: Timestamp) -> Bool {
  is_live_at(node.valid_from, node.deleted_at, at)
}

pub fn edge_live_at(edge: Edge, at: Timestamp) -> Bool {
  is_live_at(edge.valid_from, edge.deleted_at, at)
}
