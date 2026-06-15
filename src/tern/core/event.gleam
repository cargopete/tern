//// tern/core/event — the lineage ingestion contract.
////
//// An event describes one mutation to the lineage graph. The genuinely
//// valuable, backend-independent logic lives here: `implied_edges` turns an
//// event into the set of `flows_into` edges it asserts. Pure and testable.

import gleam/dict.{type Dict}
import gleam/list
import gleam/time/timestamp.{type Timestamp}
import tern/core/model.{
  type Identity, type NodeRole, type PropValue, Entity, Identity,
}

/// What an event does to its node.
pub type Op {
  Create
  Update
  Delete
}

/// A link from/to another node, with the columns involved (lineage at column
/// granularity). The linked node is always an `Entity`.
pub type Link {
  Link(external_id: String, kind: String, columns: List(String))
}

/// Role-shaped links. Origins push to targets; Consumers pull from sources;
/// Operations do both; Entities carry no intrinsic links (their edges come from
/// the Operations/Origins/Consumers that reference them).
pub type Links {
  OriginLinks(targets: List(Link))
  OperationLinks(sources: List(Link), targets: List(Link))
  ConsumerLinks(sources: List(Link))
  EntityLinks
}

pub type LineageEvent {
  LineageEvent(
    role: NodeRole,
    external_id: String,
    kind: String,
    name: String,
    operation: Op,
    links: Links,
    /// append = merge new links with existing; otherwise replace.
    append: Bool,
    occurred_at: Timestamp,
    properties: Dict(String, PropValue),
  )
}

/// A directed `flows_into` edge between two node identities.
pub type EdgeSpec {
  EdgeSpec(from: Identity, to: Identity)
}

/// The edges an event asserts, in dataflow direction. This is the heart of
/// lineage ingestion and is independent of any storage backend.
pub fn implied_edges(event: LineageEvent) -> List(EdgeSpec) {
  let this = Identity(event.external_id, event.kind, event.role)
  case event.links {
    OriginLinks(targets) ->
      list.map(targets, fn(t) { EdgeSpec(this, link_identity(t)) })
    ConsumerLinks(sources) ->
      list.map(sources, fn(s) { EdgeSpec(link_identity(s), this) })
    OperationLinks(sources, targets) -> {
      let ins = list.map(sources, fn(s) { EdgeSpec(link_identity(s), this) })
      let outs = list.map(targets, fn(t) { EdgeSpec(this, link_identity(t)) })
      list.append(ins, outs)
    }
    EntityLinks -> []
  }
}

fn link_identity(link: Link) -> Identity {
  Identity(link.external_id, link.kind, Entity)
}
