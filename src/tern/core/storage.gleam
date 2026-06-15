//// tern/core/storage — the pluggable storage contract.
////
//// Gleam has no traits; the idiomatic equivalent of an interface is a record of
//// functions. `StorageBackend` fronts every graph operation; `tern_age` (M2)
//// will be the first implementation. Anyone can build another (in-memory,
//// SQLite, Memgraph) without touching core.

import gleam/dict.{type Dict}
import gleam/option.{type Option}
import gleam/time/timestamp.{type Timestamp}
import tern/core/model.{
  type Edge, type Identity, type Node, type NodeId, type PropValue, type Tenant,
}

/// Errors a backend can raise. `Transient` failures are safe to retry (the
/// consumer requeues the whole event); `Permanent` ones are not.
pub type TernError {
  Transient(message: String)
  Permanent(message: String)
  NotFound
}

pub fn is_transient(error: TernError) -> Bool {
  case error {
    Transient(_) -> True
    _ -> False
  }
}

/// Traversal direction from the query root.
pub type Direction {
  Upstream
  Downstream
  Both
}

/// A point-in-time ("as-of") traversal request.
pub type TimelineQuery {
  TimelineQuery(
    tenant: Tenant,
    root: Identity,
    at: Timestamp,
    direction: Direction,
    max_depth: Int,
    page: Int,
    page_size: Int,
  )
}

pub type PagedGraph {
  PagedGraph(nodes: List(Node), edges: List(Edge), total: Int, page: Int)
}

/// Upsert inputs (the write side). Identity decides create-vs-match.
pub type NodeUpsert {
  NodeUpsert(
    identity: Identity,
    name: String,
    properties: Dict(String, PropValue),
    valid_from: Timestamp,
  )
}

pub type EdgeUpsert {
  EdgeUpsert(from: NodeId, to: NodeId, label: String, valid_from: Timestamp)
}

/// A single event's worth of mutations, applied atomically. On AGE this wraps
/// one Postgres transaction so the graph mutations *and* the node snapshot land
/// together or not at all. Dropping the session without `commit` rolls back.
pub type WriteSession {
  WriteSession(
    create_node: fn(NodeUpsert) -> Result(NodeId, TernError),
    create_edge: fn(EdgeUpsert) -> Result(Nil, TernError),
    soft_delete_node: fn(NodeId, Timestamp) -> Result(Nil, TernError),
    store_snapshot: fn(Node) -> Result(Nil, TernError),
    commit: fn() -> Result(Nil, TernError),
  )
}

/// The storage interface. (Streaming traversal joins this in M4, once a yielder
/// dependency is pulled in.)
pub type StorageBackend {
  StorageBackend(
    find_node: fn(Tenant, Identity) -> Result(Option(NodeId), TernError),
    query_at_time: fn(TimelineQuery) -> Result(PagedGraph, TernError),
    begin_write: fn(Tenant) -> Result(WriteSession, TernError),
  )
}
