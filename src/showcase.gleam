//// Seed an evolving data-lineage story into AGE, for the web explorer to
//// time-travel through:
////   docker compose up -d
////   gleam run -m showcase     # seed
////   gleam run -m serve        # then open http://localhost:8080/
////
//// The story (unix-second timestamps):
////   t=1000  orders-db  ─▶ raw_orders, customers
////   t=2000  enrich-orders (raw_orders + customers ─▶ enriched_orders)
////   t=3000  exec-dashboard consumes enriched_orders
////   t=4000  a second pipeline: events-api ─▶ raw_events ─▶ sessionize ─▶ sessions
////   t=5000  enrich-orders is deleted  (the orders pipeline breaks here)

import gleam/dict
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/time/timestamp.{type Timestamp}
import pog
import tern/age
import tern/core/event.{
  type LineageEvent, type Link, ConsumerLinks, Create, Delete, LineageEvent,
  Link, OperationLinks, OriginLinks,
}
import tern/core/model.{Consumer, Operation, Origin, Tenant}
import tern/ingest

const tenant_ns = "showcase"

pub fn main() {
  let assert Ok(started) =
    pog.default_config(process.new_name("tern_showcase"))
    |> pog.host("localhost")
    |> pog.port(5455)
    |> pog.database("tern")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.start
  let store = age.backend(started.data)
  let tenant = Tenant(tenant_ns)
  let assert Ok(_) = store.ensure_ready(tenant)

  list.each(events(), fn(ev) {
    let assert Ok(_) = store.write(tenant, fn(s) { ingest.apply(s, ev) })
  })

  io.println(
    "✓ seeded tenant '"
    <> tenant_ns
    <> "' (t=1000..5000). Now: gleam run -m serve  →  open http://localhost:8080/",
  )
}

fn at(s: Int) -> Timestamp {
  timestamp.from_unix_seconds(s)
}

fn ent(id: String) -> Link {
  Link(id, "asset", [])
}

fn events() -> List(LineageEvent) {
  [
    LineageEvent(
      Origin,
      "orders-db",
      "postgres",
      "Orders DB",
      Create,
      OriginLinks([ent("raw_orders"), ent("customers")]),
      False,
      at(1000),
      dict.new(),
    ),
    LineageEvent(
      Operation,
      "enrich-orders",
      "dbt",
      "Enrich orders",
      Create,
      OperationLinks([ent("raw_orders"), ent("customers")], [
        ent("enriched_orders"),
      ]),
      False,
      at(2000),
      dict.new(),
    ),
    LineageEvent(
      Consumer,
      "exec-dashboard",
      "dashboard",
      "Exec Dashboard",
      Create,
      ConsumerLinks([ent("enriched_orders")]),
      False,
      at(3000),
      dict.new(),
    ),
    LineageEvent(
      Origin,
      "events-api",
      "kafka",
      "Events API",
      Create,
      OriginLinks([ent("raw_events")]),
      False,
      at(4000),
      dict.new(),
    ),
    LineageEvent(
      Operation,
      "sessionize",
      "spark",
      "Sessionize",
      Create,
      OperationLinks([ent("raw_events")], [ent("sessions")]),
      False,
      at(4000),
      dict.new(),
    ),
    // the orders pipeline's transform is decommissioned
    LineageEvent(
      Operation,
      "enrich-orders",
      "dbt",
      "Enrich orders",
      Delete,
      OperationLinks([], []),
      False,
      at(5000),
      dict.new(),
    ),
  ]
}
