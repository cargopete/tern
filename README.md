<h1 align="center">tern</h1>

<p align="center">
  <b>Embeddable, temporal data-lineage for the BEAM.</b><br>
  Written in <a href="https://gleam.run">Gleam</a>, backed by <a href="https://age.apache.org">Apache AGE</a> (Cypher inside PostgreSQL).
</p>

<p align="center">
  <a href="https://github.com/cargopete/tern/actions/workflows/test.yml"><img src="https://github.com/cargopete/tern/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
  <img src="https://img.shields.io/badge/status-pre--release-orange" alt="pre-release">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
</p>

---

> A tern flies the longest **migration** of any bird. This library tracks where your
> data came from, where it went, and what the graph looked like at any point in time —
> the lineage equivalent of never losing the thread on a very long journey.

**tern** records *data lineage*: a temporal, directed graph of how data flows through
your systems — origins, the entities data lives in, the operations that transform it,
and the consumers that read it. It answers the questions every data platform
eventually needs to:

- *Where did this table's data come from?* (upstream)
- *What breaks if I drop this source?* (downstream impact)
- *What did the pipeline look like last Tuesday at 14:00?* (temporal "as-of")

It is distilled from a production lineage service and rebuilt generic and open. It is
a small **library** you embed, not a platform you operate — the only infrastructure it
needs is a PostgreSQL database with the AGE extension.

tern is a sibling to [**wren**](https://github.com/cargopete/wren), the Gleam AMQP
library: in an event-driven setup, *wren carries the events and tern remembers where
they came from*.

---

## Table of contents

- [Status & roadmap](#status--roadmap)
- [Concepts](#concepts)
- [Quick start](#quick-start)
- [Usage](#usage)
- [Architecture](#architecture)
- [The storage contract](#the-storage-contract)
- [Why Apache AGE](#why-apache-age)
- [Design notes](#design-notes)
- [Testing](#testing)
- [Development](#development)
- [License](#license)

---

## Status & roadmap

**Early, but the foundations are solid and tested.** The write path runs against a
real Apache AGE database; the pure core is fully unit-tested.

| | Milestone | What it delivers |
|---|---|---|
| ✅ | **M0** · spike | Gleam ↔ Apache AGE round-trip: create a graph, run Cypher, decode `agtype` |
| ✅ | **M1** · `tern_core` | Node/edge model, lineage events, the `StorageBackend` behaviour, temporal `as-of` logic — pure, no I/O. **12 tests** |
| ✅ | **M2** · `tern_age` | The AGE backend: per-tenant graphs, idempotent node/edge upserts, atomic writes (graph + snapshot in one transaction), `find_node`. **5 integration tests** vs real AGE |
| ✅ | **M3** · traversal | Temporal traversal — `query_at_time`: `as-of(T)`, upstream/downstream/both, depth bounds, pagination. **6 integration tests** |
| ⬜ | **M3.5** · concurrency | Per-label unique constraint (AGE's `MERGE` is not atomic under concurrent writers); full temporal-reachability (path must be entirely live) |
| ✅ | **M4** · `tern_server` | A `wisp`/`mist` HTTP API — `POST /v1/events` (ingest), `GET /v1/graph` (query), health. Plus `tern/ingest` (event → graph). 1 integration test + verified end-to-end over HTTP |
| ⬜ | **M4.5** · streaming | `GET /v1/graph/stream` Server-Sent Events (needs the streaming backend method) |
| ✅ | **M5** · `tern_consumer` | A [`wren`](https://github.com/cargopete/wren)-driven event consumer; every event is the retry unit (`Ack`/`Retry`/`DeadLetter` from `TernError.is_transient`). 2 tests + verified end-to-end over RabbitMQ |
| 🟡 | **M6** · release-ready | CI stands up real AGE + RabbitMQ; docs build clean; CHANGELOG + release checklist. **Hex publish itself is gated** — see [`RELEASING.md`](./RELEASING.md) |

---

## Concepts

Lineage is a **directed graph**. Every node plays one of four **roles**, joined by
`flows_into` edges in the direction data moves:

```
Origin  ──▶  Entity  ──▶  Operation  ──▶  Entity  ──▶  Consumer
(source)     (asset)      (transform)     (asset)      (dashboard)
```

| Role | Meaning | Examples |
|------|---------|----------|
| `Origin` | external input where data enters | a Postgres source, an S3 bucket, a Kafka topic |
| `Entity` | a thing data lives in | a table, an asset, a file |
| `Operation` | something that transforms data | an ETL job, a dbt model, an enrichment step |
| `Consumer` | external output that reads data | a dashboard, a report, an export |

**Identity.** A node is identified by `(external_id, kind, role)` — *you* own those
strings; tern attaches no taxonomy. Two writes with the same identity are the same
node (idempotent upsert).

**Temporal by default.** Nodes and edges carry a `valid_from` and an optional
`deleted_at`. Nothing is destructively deleted — a delete just stamps `deleted_at`,
so any query can ask "what was live at instant *T*?". The predicate at the heart of
this is `model.is_live_at`.

**Tenancy.** Each `Tenant` maps to its own AGE graph (`tern_<slug>`), so tenants are
physically isolated and queries never need a tenant filter on every hop.

**Atomicity.** Each unit of work (typically one event) applies its graph mutations
*and* its node snapshot inside a **single PostgreSQL transaction** — all or nothing.
This is possible because the graph and the snapshot table live in the same database.

---

## Quick start

You need [Gleam](https://gleam.run) (with Erlang) and Docker.

```sh
# 1. Stand up Apache AGE — provisions the `tern` database, the extension, and
#    configures the session to auto-load AGE (see db/init.sql)
docker compose up -d

# 2. Run the bundled demo — writes and reads back a 5-node pipeline
gleam run
```

---

## Usage

### Connect and prepare a tenant

```gleam
import gleam/erlang/process
import gleam/option.{Some}
import pog
import tern/age
import tern/core/model.{Tenant}

pub fn main() {
  // Any started pog connection to an AGE-enabled database works.
  let assert Ok(started) =
    pog.default_config(process.new_name("tern"))
    |> pog.host("localhost")
    |> pog.port(5455)
    |> pog.database("tern")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.start

  let store = age.backend(started.data)
  let tenant = Tenant("acme/prod")

  // create the graph, labels and snapshot table (idempotent)
  let assert Ok(_) = store.ensure_ready(tenant)
}
```

### Ingest a lineage event

The pure `tern/core/event` module turns a high-level event into the edges it implies;
you apply them inside one atomic `write`:

```gleam
import gleam/dict
import gleam/time/timestamp
import tern/core/model.{Identity, Operation, Entity}
import tern/core/storage.{NodeUpsert, EdgeUpsert}

let now = timestamp.system_time()

let assert Ok(_) =
  store.write(tenant, fn(s) {
    // upsert the operation and the entity it produces
    let assert Ok(etl) =
      s.create_node(NodeUpsert(
        Identity("enrich-orders", "dbt", Operation),
        "Enrich orders",
        dict.new(),
        now,
      ))
    let assert Ok(out) =
      s.create_node(NodeUpsert(
        Identity("enriched_orders", "table", Entity),
        "enriched_orders",
        dict.new(),
        now,
      ))

    // wire the dataflow edge
    let assert Ok(_) = s.create_edge(EdgeUpsert(etl, out, "flows_into", now))
    Ok(Nil)
    // returning Ok commits; returning Error (or crashing) rolls the whole lot back
  })
```

### Deriving edges from an event

`implied_edges` is pure and backend-independent — handy in tests and in the consumer:

```gleam
import tern/core/event.{LineageEvent, OperationLinks, Link, Create}

let ev =
  LineageEvent(
    role: Operation,
    external_id: "enrich-orders",
    kind: "dbt",
    name: "Enrich orders",
    operation: Create,
    links: OperationLinks(
      sources: [Link("raw_orders", "table", ["id", "total"])],
      targets: [Link("enriched_orders", "table", ["id", "total", "name"])],
    ),
    append: False,
    occurred_at: now,
    properties: dict.new(),
  )

// [ raw_orders ─▶ enrich-orders , enrich-orders ─▶ enriched_orders ]
let edges = event.implied_edges(ev)
```

### Look a node up

```gleam
import gleam/option.{Some, None}

case store.find_node(tenant, Identity("enriched_orders", "table", Entity)) {
  Ok(Some(node_id)) -> // ...
  Ok(None) -> // not found
  Error(e) -> // storage error
}
```

### Traverse the graph as-of a point in time

```gleam
import tern/core/storage.{TimelineQuery, Downstream}

let assert Ok(graph) =
  store.query_at_time(TimelineQuery(
    tenant:,
    root: Identity("orders-db", "postgres", Origin),
    at: now,            // "as of" this instant
    direction: Downstream,
    max_depth: 5,
    page: 0,
    page_size: 100,
  ))

// graph.nodes / graph.edges (live as-of `at`), graph.total, graph.page
```

---

## HTTP server

`tern/server` exposes a `wisp`/`mist` API over any `StorageBackend`:

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/health` | liveness |
| `POST` | `/v1/events` | ingest a lineage event (one atomic transaction) |
| `GET`  | `/v1/graph` | temporal traversal as JSON |

Run it:

```sh
docker compose up -d
gleam run -m serve          # listens on :8080
```

Ingest an event and read the graph back:

```sh
curl -X POST localhost:8080/v1/events -H 'content-type: application/json' -d '{
  "tenant": "acme", "role": "origin", "externalId": "orders-db", "kind": "postgres",
  "name": "Orders DB", "operation": "create", "occurredAt": 1000,
  "targets": [{"externalId": "raw_orders", "kind": "asset"}]
}'

curl "localhost:8080/v1/graph?tenant=acme&externalId=orders-db&kind=postgres&role=origin&direction=downstream&depth=5"
# → {"nodes":[…],"edges":[…],"total":2,"page":0}
```

The JSON shape (`{nodes, edges, total, page}`) is deliberately simple to render in a
browser graph viewer.

## Event consumer (wren)

`tern/consumer` ingests lineage events from RabbitMQ via [wren](https://github.com/cargopete/wren),
tern's sibling AMQP library. The whole event is the retry unit, mapped straight from the
storage error:

| Outcome | wren verdict |
|---------|--------------|
| applied successfully | `Ack` |
| transient storage failure (`TernError.is_transient`) | `Retry` (redelivered) |
| undecodable, or permanent failure | `DeadLetter` |

```gleam
import wren
import tern/consumer

let assert Ok(conn) = wren.connect(wren.default_config())
let assert Ok(channel) = wren.open_channel(conn)
let assert Ok(_) = wren.declare_queue(channel, "tern.events")

// each delivered JSON event is decoded and applied in one transaction
let assert Ok(_) = consumer.start(channel, "tern.events", store)
```

Run the end-to-end demo (publishes an event, consumes it, queries it back):

```sh
docker compose up -d            # AGE + RabbitMQ
gleam run -m consume_demo
```

> *wren carries the events; tern remembers where they came from.*

## Architecture

tern is layered so the model is reusable and the storage is swappable:

```
                wren  (AMQP, Gleam)         ─┐
                wisp/mist (HTTP+SSE)        ─┤
                                             ▼
   tern/core ──── the model + events + the StorageBackend contract (pure)
        ▲                       │
        │ implements            │ used by
        │                       ▼
   tern/age ───────────────▶ Apache AGE / PostgreSQL
   (Cypher generation,        (graph + snapshot, one database)
    pog, transactions)
```

- **`tern/core`** — `model`, `event`, `storage`. No I/O, no database, target-agnostic.
  This is the vocabulary everything else speaks.
- **`tern/age`** — the first `StorageBackend`: Cypher generation, `pog` connection,
  transactions, agtype handling.

Planned layers (`tern_server`, `tern_consumer`) sit on top and depend only on the
`StorageBackend` contract — never on `tern/age` directly.

---

## The storage contract

Gleam has no traits, so the interface is a **record of functions**. Any backend
(AGE today; in-memory, SQLite, or Memgraph tomorrow) is just a value of this type:

```gleam
pub type StorageBackend {
  StorageBackend(
    ensure_ready:  fn(Tenant) -> Result(Nil, TernError),
    find_node:     fn(Tenant, Identity) -> Result(Option(NodeId), TernError),
    query_at_time: fn(TimelineQuery) -> Result(PagedGraph, TernError),
    write:         fn(Tenant, fn(WriteSession) -> Result(Nil, TernError))
                     -> Result(Nil, TernError),
  )
}
```

`write` hands a `WriteSession` to your callback and wraps it in one transaction —
return `Ok` to commit, `Error` to roll back:

```gleam
pub type WriteSession {
  WriteSession(
    create_node:      fn(NodeUpsert) -> Result(NodeId, TernError),
    create_edge:      fn(EdgeUpsert) -> Result(Nil, TernError),
    soft_delete_node: fn(NodeId, Timestamp) -> Result(Nil, TernError),
    store_snapshot:   fn(Node) -> Result(Nil, TernError),
  )
}
```

Errors are classified so callers (and the future consumer) can retry intelligently —
`Transient` failures are safe to retry, `Permanent` ones are not:

```gleam
pub type TernError {
  Transient(message: String)
  Permanent(message: String)
  NotFound
}
```

---

## Why Apache AGE

[Apache AGE](https://age.apache.org) is a PostgreSQL extension that runs openCypher
graph queries *inside* a normal Postgres database. For lineage that buys two things
nothing else does as cleanly:

1. **One datastore.** The graph and the temporal snapshots live in the same Postgres —
   one thing to run, secure and back up.
2. **Real transactional atomicity.** Because both are in one database, a single event's
   graph mutations and its snapshot commit together. With a separate graph engine they
   could drift apart on partial failure.

tern speaks to AGE through [`pog`](https://hexdocs.pm/pog/). AGE returns the `agtype`
type, which tern casts to `text` and parses — so no custom binary codec is needed.

---

## Design notes

A few hard-won details, carried over from the production service the ideas came from:

- **AGE has a narrower Cypher than Neo4j/Memgraph.** Notably there is no
  `ON CREATE SET` / `ON MATCH SET`. tern uses `MERGE` followed by an unconditional
  `SET` with `coalesce(...)` for create-only fields (e.g. the immutable `node_id`).
- **Per-session setup, once.** AGE needs its library loaded and `ag_catalog` on the
  search path. `db/init.sql` sets this at the **database** level
  (`session_preload_libraries`, `ALTER DATABASE ... SET search_path`), so pooled
  connections need no per-query `LOAD 'age'`.
- **Atomicity needs a shared database.** The snapshot table lives in the same database
  as the AGE graph; that is what lets one transaction cover both. If you split them, you
  lose the guarantee.
- **`MERGE` is not atomic under concurrency** (M3.5). Two concurrent upserts of the same
  identity can both create. The fix — a per-label unique constraint so the loser gets a
  retryable unique violation — is tracked as a hardening milestone.
- **Cypher is currently built by string interpolation** with single-quote escaping.
  Parameterised Cypher is a planned follow-up.
- **Traversal semantics (M3).** `query_at_time` does depth/direction in Cypher, then
  filters nodes and edges by `is_live_at(T)` in Gleam. It returns the live nodes within
  structural reach and the live edges among them. Full temporal *reachability* (where the
  entire path must be live, so a node orphaned by an upstream deletion drops out) is a
  planned refinement (M3.5).

---

## Testing

tern keeps two tiers of tests:

- **Pure unit tests** (`tern/core`) run anywhere with no database — temporal edge cases,
  graph naming, identity, `implied_edges`.
- **Integration tests** (`tern/age`) run against a real AGE instance and are gated behind
  an environment variable so the default run needs no database:

```sh
# pure tests only
gleam test

# everything, against the composed AGE database
docker compose up -d
TERN_IT=1 gleam test
```

---

## Development

```sh
docker compose up -d   # Apache AGE on localhost:5455
gleam run              # run the demo
gleam test             # pure tests (add TERN_IT=1 for integration)
gleam format           # format
```

See [`PLAN.md`](./PLAN.md) for the full extraction-and-build plan.

## License

[MIT](./LICENSE) © cargopete
