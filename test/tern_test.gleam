import gleam/dict
import gleam/list
import gleam/option.{None, Some}
import gleam/time/timestamp
import gleeunit
import gleeunit/should
import tern/core/event.{
  ConsumerLinks, EdgeSpec, Link, OperationLinks, OriginLinks,
}
import tern/core/model.{Consumer, Entity, Identity, Operation, Origin, Tenant}

pub fn main() {
  gleeunit.main()
}

// --- temporal predicate (the heart of as-of queries) -----------------------

fn at(secs: Int) {
  timestamp.from_unix_seconds(secs)
}

pub fn live_when_created_before_and_never_deleted_test() {
  model.is_live_at(at(10), None, at(20))
  |> should.be_true
}

pub fn live_when_created_exactly_at_query_time_test() {
  model.is_live_at(at(20), None, at(20))
  |> should.be_true
}

pub fn not_live_before_creation_test() {
  model.is_live_at(at(30), None, at(20))
  |> should.be_false
}

pub fn not_live_after_deletion_test() {
  model.is_live_at(at(10), Some(at(15)), at(20))
  |> should.be_false
}

pub fn still_live_if_deleted_after_query_time_test() {
  // deleted strictly after `at` → still live as of `at`
  model.is_live_at(at(10), Some(at(25)), at(20))
  |> should.be_true
}

pub fn not_live_at_exact_deletion_instant_test() {
  model.is_live_at(at(10), Some(at(20)), at(20))
  |> should.be_false
}

// --- tenant graph naming ---------------------------------------------------

pub fn graph_name_is_prefixed_and_slugged_test() {
  model.graph_name(Tenant("Acme Corp / Project-7"))
  |> should.equal("tern_acme_corp___project_7")
}

pub fn graph_name_is_deterministic_test() {
  let t = Tenant("tenant-abc")
  model.graph_name(t)
  |> should.equal(model.graph_name(t))
}

// --- identity --------------------------------------------------------------

pub fn node_identity_extracts_merge_key_test() {
  let node =
    model.Node(
      id: model.NodeId("n1"),
      external_id: "orders-db",
      kind: "postgres",
      role: Origin,
      name: "Orders DB",
      properties: dict.new(),
      valid_from: at(1),
      deleted_at: None,
    )
  model.node_identity(node)
  |> should.equal(Identity("orders-db", "postgres", Origin))
}

// --- implied edges (lineage ingestion logic) -------------------------------

fn entity(id: String) {
  Link(external_id: id, kind: "asset", columns: [])
}

pub fn origin_event_flows_into_each_target_test() {
  let ev =
    event.LineageEvent(
      role: Origin,
      external_id: "orders-db",
      kind: "postgres",
      name: "Orders DB",
      operation: event.Create,
      links: OriginLinks(targets: [entity("raw"), entity("customers")]),
      append: False,
      occurred_at: at(1),
      properties: dict.new(),
    )
  let me = Identity("orders-db", "postgres", Origin)
  event.implied_edges(ev)
  |> should.equal([
    EdgeSpec(me, Identity("raw", "asset", Entity)),
    EdgeSpec(me, Identity("customers", "asset", Entity)),
  ])
}

pub fn operation_event_wires_sources_in_and_targets_out_test() {
  let ev =
    event.LineageEvent(
      role: Operation,
      external_id: "enrich",
      kind: "transform",
      name: "Enrich",
      operation: event.Create,
      links: OperationLinks(sources: [entity("raw")], targets: [entity("enr")]),
      append: False,
      occurred_at: at(1),
      properties: dict.new(),
    )
  let me = Identity("enrich", "transform", Operation)
  let edges = event.implied_edges(ev)

  // one inbound (source -> op) and one outbound (op -> target)
  list.length(edges) |> should.equal(2)
  list.contains(edges, EdgeSpec(Identity("raw", "asset", Entity), me))
  |> should.be_true
  list.contains(edges, EdgeSpec(me, Identity("enr", "asset", Entity)))
  |> should.be_true
}

pub fn consumer_event_pulls_from_each_source_test() {
  let ev =
    event.LineageEvent(
      role: Consumer,
      external_id: "dash",
      kind: "dashboard",
      name: "Dashboard",
      operation: event.Create,
      links: ConsumerLinks(sources: [entity("enr")]),
      append: False,
      occurred_at: at(1),
      properties: dict.new(),
    )
  let me = Identity("dash", "dashboard", Consumer)
  event.implied_edges(ev)
  |> should.equal([EdgeSpec(Identity("enr", "asset", Entity), me)])
}
