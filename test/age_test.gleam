//// Integration tests for the AGE backend. Gated behind `TERN_IT=1` so the
//// default `gleam test` stays green without a database; run locally with:
////
////   docker compose up -d
////   TERN_IT=1 gleam test

import envoy
import gleam/dict
import gleam/erlang/process
import gleam/option.{type Option, None, Some}
import gleam/time/timestamp
import gleeunit/should
import pog
import tern/age
import tern/core/model.{type Tenant, Entity, Identity, Origin, Tenant}
import tern/core/storage.{EdgeUpsert, NodeUpsert}
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

// --- tiny test helpers -----------------------------------------------------

fn is_some(r: Result(Option(a), b)) -> Bool {
  case r {
    Ok(Some(_)) -> True
    _ -> False
  }
}
