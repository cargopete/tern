//// Integration tests for the AGE backend. Gated behind `TERN_IT=1` so the
//// default `gleam test` stays green without a database; run locally with:
////
////   docker compose up -d
////   TERN_IT=1 gleam test

import envoy
import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/time/timestamp
import gleeunit/should
import pog
import tern/age
import tern/core/event.{Create, Link, OriginLinks}
import tern/core/model.{
  type Tenant, Consumer, Entity, Identity, Operation, Origin, Tenant,
}
import tern/core/storage.{
  Downstream, EdgeUpsert, NodeUpsert, TimelineQuery, Upstream,
}
import tern/ingest
import youid/uuid

/// Run `f` with a live AGE backend, or skip if TERN_IT isn't set.
fn with_backend(f: fn(storage.StorageBackend, Tenant) -> Nil) -> Nil {
  case envoy.get("TERN_IT") {
    Ok("1") -> {
      let cfg =
        pog.default_config(process.new_name("tern_it"))
        |> pog.host("localhost")
        |> pog.port(5455)
        |> pog.database("tern")
        |> pog.user("postgres")
        |> pog.password(Some("postgres"))
        |> pog.pool_size(2)
      let assert Ok(started) = pog.start(cfg)
      let backend = age.backend(started.data)
      // unique tenant per run keeps tests isolated from each other
      let tenant = Tenant("it_" <> uuid.v4_string())
      let assert Ok(_) = backend.ensure_ready(tenant)
      f(backend, tenant)
    }
    _ -> Nil
  }
}

fn at(s: Int) {
  timestamp.from_unix_seconds(s)
}

fn entity_id(x: String) {
  Identity(external_id: x, kind: "asset", role: Entity)
}

pub fn ensure_ready_is_idempotent_test() {
  use backend, tenant <- with_backend
  // a second ensure_ready on the same tenant must not error
  backend.ensure_ready(tenant)
  |> should.equal(Ok(Nil))
}

pub fn write_and_find_a_node_test() {
  use backend, tenant <- with_backend
  let origin =
    Identity(external_id: "orders-db", kind: "postgres", role: Origin)

  let assert Ok(_) =
    backend.write(tenant, fn(s) {
      let assert Ok(_) =
        s.create_node(NodeUpsert(origin, "Orders DB", dict.new(), at(100)))
      Ok(Nil)
    })

  backend.find_node(tenant, origin) |> is_some |> should.be_true
}

pub fn upserts_are_idempotent_on_identity_test() {
  use backend, tenant <- with_backend
  let id = Identity(external_id: "dup", kind: "postgres", role: Origin)

  // two upserts of the same identity must converge on ONE node_id
  let assert Ok(_) =
    backend.write(tenant, fn(s) {
      let assert Ok(first) =
        s.create_node(NodeUpsert(id, "v1", dict.new(), at(1)))
      let assert Ok(second) =
        s.create_node(NodeUpsert(id, "v2", dict.new(), at(2)))
      first |> should.equal(second)
      Ok(Nil)
    })
  Nil
}

pub fn nodes_and_edge_persist_together_test() {
  use backend, tenant <- with_backend
  let origin = Identity(external_id: "src", kind: "postgres", role: Origin)
  let ent = entity_id("raw")

  let assert Ok(_) =
    backend.write(tenant, fn(s) {
      let assert Ok(a) =
        s.create_node(NodeUpsert(origin, "Source", dict.new(), at(1)))
      let assert Ok(b) =
        s.create_node(NodeUpsert(ent, "raw_orders", dict.new(), at(1)))
      let assert Ok(_) = s.create_edge(EdgeUpsert(a, b, "flows_into", at(1)))
      Ok(Nil)
    })

  backend.find_node(tenant, origin) |> is_some |> should.be_true
  backend.find_node(tenant, ent) |> is_some |> should.be_true
}

pub fn a_failed_write_rolls_back_test() {
  use backend, tenant <- with_backend
  let ghost = Identity(external_id: "ghost", kind: "postgres", role: Origin)

  // create a node, then fail the unit of work — nothing should persist
  let result =
    backend.write(tenant, fn(s) {
      let assert Ok(_) =
        s.create_node(NodeUpsert(ghost, "Ghost", dict.new(), at(1)))
      Error(storage.Permanent("deliberate abort"))
    })
  result |> should.be_error

  backend.find_node(tenant, ghost) |> should.equal(Ok(None))
}

// --- M3: temporal traversal ------------------------------------------------

// pipeline: src ─▶ raw ─▶ etl ─▶ enr ─▶ dash
fn p_origin() {
  Identity("src", "postgres", Origin)
}

fn p_raw() {
  Identity("raw", "asset", Entity)
}

fn p_etl() {
  Identity("etl", "transform", Operation)
}

fn p_enr() {
  Identity("enr", "asset", Entity)
}

fn p_dash() {
  Identity("dash", "dashboard", Consumer)
}

fn seed_pipeline(backend: storage.StorageBackend, tenant: Tenant) {
  let assert Ok(_) =
    backend.write(tenant, fn(s) {
      let assert Ok(o) =
        s.create_node(NodeUpsert(p_origin(), "Source", dict.new(), at(1)))
      let assert Ok(r) =
        s.create_node(NodeUpsert(p_raw(), "raw", dict.new(), at(1)))
      let assert Ok(t) =
        s.create_node(NodeUpsert(p_etl(), "etl", dict.new(), at(1)))
      let assert Ok(e) =
        s.create_node(NodeUpsert(p_enr(), "enr", dict.new(), at(1)))
      let assert Ok(d) =
        s.create_node(NodeUpsert(p_dash(), "dash", dict.new(), at(1)))
      let assert Ok(_) = s.create_edge(EdgeUpsert(o, r, "flows_into", at(1)))
      let assert Ok(_) = s.create_edge(EdgeUpsert(r, t, "flows_into", at(1)))
      let assert Ok(_) = s.create_edge(EdgeUpsert(t, e, "flows_into", at(1)))
      let assert Ok(_) = s.create_edge(EdgeUpsert(e, d, "flows_into", at(1)))
      Ok(Nil)
    })
  Nil
}

pub fn downstream_traversal_reaches_the_whole_pipeline_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_origin(),
      at(100),
      Downstream,
      5,
      0,
      100,
    ))
  list.length(g.nodes) |> should.equal(5)
}

pub fn upstream_traversal_reaches_back_to_the_origin_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_dash(),
      at(100),
      Upstream,
      5,
      0,
      100,
    ))
  list.length(g.nodes) |> should.equal(5)
}

pub fn depth_bounds_the_traversal_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  // depth 1 downstream from the origin: just src + raw
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_origin(),
      at(100),
      Downstream,
      1,
      0,
      100,
    ))
  list.length(g.nodes) |> should.equal(2)
}

pub fn as_of_includes_a_node_before_its_deletion_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  let assert Ok(Some(etl_id)) = backend.find_node(tenant, p_etl())
  let assert Ok(_) =
    backend.write(tenant, fn(s) { s.soft_delete_node(etl_id, at(5)) })

  // querying as-of t=3 (before the delete at t=5) still sees all 5 nodes
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_origin(),
      at(3),
      Downstream,
      5,
      0,
      100,
    ))
  list.length(g.nodes) |> should.equal(5)
}

pub fn as_of_excludes_a_node_after_its_deletion_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  let assert Ok(Some(etl_id)) = backend.find_node(tenant, p_etl())
  let assert Ok(_) =
    backend.write(tenant, fn(s) { s.soft_delete_node(etl_id, at(5)) })

  // querying as-of t=7 (after the delete) drops the deleted node
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_origin(),
      at(7),
      Downstream,
      5,
      0,
      100,
    ))
  list.length(g.nodes) |> should.equal(4)
}

pub fn pagination_limits_the_page_but_reports_total_test() {
  use backend, tenant <- with_backend
  seed_pipeline(backend, tenant)
  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      p_origin(),
      at(100),
      Downstream,
      5,
      0,
      2,
    ))
  list.length(g.nodes) |> should.equal(2)
  g.total |> should.equal(5)
}

// --- M4: ingest.apply (event → graph) --------------------------------------

pub fn ingest_apply_builds_graph_from_an_event_test() {
  use backend, tenant <- with_backend
  let ev =
    event.LineageEvent(
      role: Origin,
      external_id: "odb",
      kind: "postgres",
      name: "Orders DB",
      operation: Create,
      links: OriginLinks([Link("t1", "asset", [])]),
      append: False,
      occurred_at: at(1),
      properties: dict.new(),
    )
  let assert Ok(_) = backend.write(tenant, fn(s) { ingest.apply(s, ev) })

  let assert Ok(g) =
    backend.query_at_time(TimelineQuery(
      tenant,
      Identity("odb", "postgres", Origin),
      at(10),
      Downstream,
      5,
      0,
      100,
    ))
  // the origin and the entity it targets, joined by one edge
  list.length(g.nodes) |> should.equal(2)
  list.length(g.edges) |> should.equal(1)
}

// --- tiny test helpers -----------------------------------------------------

fn is_some(r: Result(Option(a), b)) -> Bool {
  case r {
    Ok(Some(_)) -> True
    _ -> False
  }
}
