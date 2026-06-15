//// tern/ingest — apply a `LineageEvent` to a write session.
////
//// This is the bridge between the event contract (`tern/core/event`) and the
//// storage write side (`tern/core/storage`). It is used by both the HTTP server
//// and the (future) consumer, so the ingestion semantics live in exactly one
//// place. Call it inside `backend.write` so the whole event is one transaction.

import gleam/dict
import gleam/list
import gleam/option.{None}
import gleam/result
import tern/core/event.{type LineageEvent, Delete}
import tern/core/model.{type Identity, type Node, type NodeId, Identity, Node}
import tern/core/storage.{
  type NodeUpsert, type TernError, type WriteSession, EdgeUpsert, NodeUpsert,
}

/// Apply one event's mutations to a write session.
///
/// `Create`/`Update` upsert the node, snapshot it, and wire its implied edges
/// (creating any referenced entity nodes as needed). `Delete` soft-deletes the
/// node and its incident edges.
pub fn apply(
  session: WriteSession,
  ev: LineageEvent,
) -> Result(Nil, TernError) {
  case ev.operation {
    Delete -> {
      use id <- result.try(session.create_node(main_upsert(ev)))
      session.soft_delete_node(id, ev.occurred_at)
    }
    _ -> upsert(session, ev)
  }
}

fn upsert(session: WriteSession, ev: LineageEvent) -> Result(Nil, TernError) {
  use main_id <- result.try(session.create_node(main_upsert(ev)))
  use _ <- result.try(session.store_snapshot(node_of(ev, main_id)))

  // Ensure both endpoints of each implied edge exist (upserts are idempotent),
  // then create the edge. The main node is re-merged harmlessly.
  list.try_fold(event.implied_edges(ev), Nil, fn(_, spec) {
    use from_id <- result.try(ensure(session, ev, spec.from))
    use to_id <- result.try(ensure(session, ev, spec.to))
    session.create_edge(EdgeUpsert(from_id, to_id, "flows_into", ev.occurred_at))
  })
}

fn ensure(
  session: WriteSession,
  ev: LineageEvent,
  id: Identity,
) -> Result(NodeId, TernError) {
  // the main node keeps the event's name/properties; referenced entities get a
  // lightweight upsert named after their external id.
  let upsert = case id == main_identity(ev) {
    True -> main_upsert(ev)
    False -> NodeUpsert(id, id.external_id, dict.new(), ev.occurred_at)
  }
  session.create_node(upsert)
}

fn main_identity(ev: LineageEvent) -> Identity {
  Identity(ev.external_id, ev.kind, ev.role)
}

fn main_upsert(ev: LineageEvent) -> NodeUpsert {
  NodeUpsert(main_identity(ev), ev.name, ev.properties, ev.occurred_at)
}

fn node_of(ev: LineageEvent, id: NodeId) -> Node {
  Node(
    id: id,
    external_id: ev.external_id,
    kind: ev.kind,
    role: ev.role,
    name: ev.name,
    properties: ev.properties,
    valid_from: ev.occurred_at,
    deleted_at: None,
  )
}
