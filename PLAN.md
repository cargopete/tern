# Open-Source Lineage in Gleam — Extraction & Build Plan

**Status:** Plan only (no code yet)
**Date:** 2026-06-15
**Author:** Jenny (with Chief)
**Sibling project:** [`wren`](https://github.com/cargopete/wren) — the Gleam AMQP library distilled from bunnyhop. This plan reuses wren as the event-transport layer.

---

## 1. The idea in one breath

Take the lineage service's architecture — event-driven data-lineage tracking over a graph, with temporal "as-of" queries and atomic writes — strip every Fathom-ism, and rebuild it as a **generic, embeddable, open-source Gleam library + reference server** on the BEAM. Apache AGE (Cypher-in-Postgres) is the storage; **wren** carries the events; **wisp/mist** serves the API.

It is *not* a port of the Rust line-by-line. It's a re-imagining of the same **ideas** in idiomatic Gleam:

- A typed lineage **graph model** (entities + the operations that connect them).
- A **temporal** model — every query is "as the graph was at time T", soft-deletes included.
- **Atomic event ingestion** — one event's graph mutations + its snapshot land in one DB transaction.
- A **pluggable storage backend** (ship AGE/Postgres first).
- **Streaming** traversal for big graphs.
- **Multi-tenant** isolation (graph-per-tenant).

---

## 2. Naming

Following wren's small-BEAM-bird convention. Shortlist (recommendation first):

| Name | Why | Risk |
|------|-----|------|
| **Tern** ⭐ | Terns make the longest **migrations** of any bird — "migration" is both the lineage metaphor *and* a DB pun. Short, typeable, `tern.gleam` reads well. | none known |
| **Heron** | Stands still and watches the **flow** of water — a dataflow watcher. Evocative. | none known |
| **Magpie** | Collects and **remembers** shiny things — the snapshot/memory angle. | slightly long |
| ~~Finch~~ | Darwin's finches = lineage/descent (lovely pun) | **clashes** with Elixir's popular `Finch` HTTP client — avoid in BEAM-land |

**Recommendation: `tern`.** (This plan uses `tern` as the working name throughout.)

---

## 3. What we keep vs. what we drop

### Keep (the genuinely generic, valuable core)
- The **node/edge graph model** generalised away from Fathom's four fixed roles.
- The **temporal engine**: `valid_from` / `deleted_at`, and `MATCH … as-of(T)` traversal.
- **Atomic write sessions** (graph mutation + snapshot in one transaction).
- **Traversal queries**: direction (up/down/both), depth bounds, pagination.
- **Streaming** traversal (batched nodes/edges).
- **Graph-per-tenant** namespacing.
- The **AGE dialect learnings** from FTHM-16805 (per-type labels, MERGE+SET, unique constraints, temporal via UNWIND+min) — these are gold and directly reusable.

### Drop (Fathom-specific or redundant)
- **FRI / `fathom_resource_identifier`** → replace with a generic, caller-supplied `ExternalId` (opaque string) + `kind`.
- **`fathom_context`** (org/project headers) → generic `Tenant { namespace: String }`.
- **`fathom_data_api` resource types** → no fixed taxonomy; node "kind" is a free string the caller owns.
- **Memgraph/Neo4j (Bolt) backend** → dropped for v1 (no mature Gleam Bolt client; AGE was the destination anyway). Kept *conceptually* via the backend behaviour so someone could add it later.
- **HTTP resource-metadata enrichment** (the lineage service phones the metadata service to resolve names) → made an **optional pluggable resolver** the host app provides, not baked in.

---

## 4. Generalising the domain model

The Rust service hard-codes four node roles (`Source`, `Resource`, `Process`, `Sink`) and one edge (`flows_into`). We generalise to a small, open vocabulary the user configures:

```gleam
// tern/core/model.gleam  (sketch)
pub type NodeRole {
  Entity        // a thing data lives in   (was: Resource)
  Operation     // a thing that transforms (was: Process)
  Origin        // external input          (was: Source)
  Consumer      // external output         (was: Sink)
}

pub type Node {
  Node(
    id: Uuid,                 // internal, assigned
    external_id: String,      // caller's identity (opaque) — was the FRI
    kind: String,             // caller's free-form type, e.g. "postgres", "asset"
    role: NodeRole,
    name: String,
    properties: Dict(String, json.Json),
    valid_from: Timestamp,
    deleted_at: Option(Timestamp),
  )
}

pub type Edge {
  Edge(from: Uuid, to: Uuid, label: String, valid_from: Timestamp, deleted_at: Option(Timestamp))
}

pub type Tenant { Tenant(namespace: String) }   // → AGE graph "tern_<hash(namespace)>"
```

The merge identity stays **`(external_id, kind, role)`** — the same key the Rust unique-constraint protects.

### Events (the ingestion contract)
```gleam
pub type Operation { Create Update Delete }

pub type LineageEvent {
  LineageEvent(
    role: NodeRole,
    external_id: String,
    kind: String,
    operation: Operation,
    name: String,
    // role-shaped payload: an Operation lists sources+targets, an Origin lists targets, etc.
    links: Links,
    append: Bool,            // the "append vs replace" semantics we tested today
    occurred_at: Timestamp,
    properties: Dict(String, json.Json),
  )
}
```

---

## 5. Architecture — Gleam packages

Ship as **a small family of packages** (mirrors how wren is one focused lib, but lineage is bigger):

```
tern_core      — model, event types, the StorageBackend behaviour, temporal logic (pure, target-agnostic)
tern_age       — the Apache AGE / Postgres backend (pog), Cypher generation, migrations, write sessions
tern_server    — wisp/mist HTTP API: ingest, query, SSE stream, health
tern_consumer  — wren-driven event consumer with retry/DLQ (the "every event is the retry unit" model)
tern           — umbrella / reference app wiring the above together (the runnable demo)
```

`tern_core` depends on nothing target-specific, so the model and event logic could even run on the **JS target**. Storage/server/consumer are BEAM-only.

### The backend as a Gleam "behaviour"
Gleam has no traits; the idiomatic equivalent is a **record of functions** (a first-class module interface):

```gleam
// tern_core/storage.gleam
pub type StorageBackend {
  StorageBackend(
    create_node:    fn(Tenant, NodeUpsert) -> Result(NodeResult, TernError),
    create_edge:    fn(Tenant, EdgeUpsert) -> Result(Created, TernError),
    soft_delete:    fn(Tenant, Uuid, Timestamp) -> Result(Nil, TernError),
    find_node:      fn(Tenant, Identity) -> Result(Option(Uuid), TernError),
    query_at_time:  fn(TimelineQuery) -> Result(PagedGraph, TernError),
    stream_at_time: fn(TimelineQuery) -> Yielder(GraphBatch),   // streaming traversal
    begin_write:    fn(Tenant) -> Result(WriteSession, TernError),
  )
}
```

`tern_age` constructs one of these backed by `pog`. Anyone can build another (Memgraph, in-memory, SQLite-graph) without touching core.

---

## 6. Subsystem mapping (Rust → Gleam)

| Concern | Rust (today) | Gleam (`tern`) |
|---|---|---|
| Graph store | `age_graph_client` + `AgeRepository` | `tern_age` over **`pog`** (raw Cypher-in-SQL: `SELECT * FROM cypher(...)`) |
| Backend abstraction | `LineageGraphRepository` trait | `StorageBackend` record-of-functions |
| Atomic writes | `LineageWriteSession` (one PG txn) | `pog.transaction` wrapping graph + snapshot writes |
| Temporal snapshots | `lineage_node_snapshot` table | same table, written in the same txn |
| Temporal traversal | `get_lineage_at_time` (UNWIND + min(CASE)) | identical Cypher, generated in `tern_age` |
| Streaming | `stream_lineage_at_time` → SSE | `Yielder(GraphBatch)` → **mist** chunked SSE |
| HTTP API | `poem-openapi` | **`wisp`** + **`mist`** |
| Event consumer | `bunnyhop` consumer bin | **`wren`** consumer (retry/DLQ built-in!) |
| Tenancy | graph `lineage_<uuid5>` | graph `tern_<hash(namespace)>` |
| IDs / time / json | uuid / chrono / serde | `youid` / `gleam_time`(+`birl`) / `gleam_json` |
| Tests | testcontainers (real AGE) | CI stands up a real AGE Postgres (wren's pattern) |

**The wren synergy is the headline:** the OSS lineage consumer is literally built on the OSS AMQP library — two distilled-from-the-same-platform projects clicking together. Great story.

---

## 7. The hard parts (and how we handle them)

1. **No Gleam Bolt driver.** → AGE/Postgres is the only v1 backend. This is *fine* — it's where the platform landed anyway, and it keeps the dependency surface to just `pog`. The behaviour leaves the door open.
2. **Cypher-in-Postgres ergonomics.** AGE returns `agtype`; we'll need a small typed (de)serialisation layer in `tern_age` (parse `agtype` → `gleam_json` → model). The Rust client already proved the query shapes; we translate them.
3. **Streaming on the BEAM.** mist supports chunked/SSE responses; we drive it from a `Yielder` that pulls AGE result batches (cursor or `LIMIT/OFFSET` windows). Backpressure is natural on BEAM.
4. **Atomicity = shared database.** Same rule as the Rust design: AGE graph and snapshot store live in one Postgres so a single `pog.transaction` covers both. Document loudly.
5. **AGE's non-atomic MERGE.** Carry over the **unique-constraint-per-label** fix and the single-flight graph init — these were hard-won in FTHM-16805 and apply verbatim.
6. **Testing without testcontainers.** Follow wren: CI provisions a real AGE Postgres service; tests run against it. A `docker-compose.yml` for local dev.

---

## 8. Phased delivery (milestones)

- **M0 — Spike (1–2 days).** Prove `pog` can run AGE Cypher and parse `agtype` into Gleam types. De-risks everything. Single file, throwaway.
- **M1 — `tern_core`.** Model, events, `StorageBackend` behaviour, temporal types, pure logic + tests. No I/O.
- **M2 — `tern_age`.** AGE backend: migrations, graph-per-tenant init, node/edge upsert (MERGE + unique constraints), `find_node`. Integration tests vs real AGE.
- **M3 — Temporal + write sessions.** `query_at_time` traversal (up/down/both, depth, pagination) + atomic `begin_write`/commit (graph + snapshot in one txn). Port the temporal Cypher.
- **M4 — `tern_server`.** wisp/mist: ingest endpoint, query endpoint, **SSE stream**, health. The browser viewer we built today drops straight on top (same JSON shapes).
- **M5 — `tern_consumer`.** wren consumer: decode events, run the M3 write path, every-event-is-the-retry-unit, DLQ. Closes the wren ↔ tern loop.
- **M6 — Polish & publish.** README (wren-style status checklist), `ROADMAP.md`, `CHANGELOG.md`, docs, examples, Hex publish (`gleam publish`), CI green.

Each milestone is independently demoable. M0–M3 is the genuinely novel bit; M4–M5 is plumbing onto wisp/wren.

---

## 9. Repo & publishing

- **One repo, multiple packages** (Gleam supports a workspace via path deps; or separate repos like wren if we want clean Hex packages). Recommend: **monorepo, publish `tern_core` + `tern_age` to Hex** first; server/consumer as examples until stable.
- License: MIT (matches wren).
- README mirrors wren's voice: a "what it does" checklist of ticked features, a "quick taste" code block, a design section, ROADMAP/CHANGELOG.
- CI (GitHub Actions): `gleam test` against a real AGE Postgres service container; `gleam format --check`; `gleam docs build`.

---

## 10. Open questions for Chief

1. **Scope of v1:** library-first (`tern_core` + `tern_age` on Hex) or full reference server+consumer in the first release? *(Recommend: library-first, server/consumer as examples.)*
2. **Node-role vocabulary:** keep the fixed four roles (Entity/Operation/Origin/Consumer) for familiarity, or go fully open (just `kind` + caller-defined roles)? *(Recommend: fixed four — they map cleanly to AGE per-type labels and to most lineage use-cases.)*
3. **Name:** `tern`, or one of the alternates?
4. **JS target for `tern_core`:** worth keeping core target-agnostic (browser-side lineage models) or BEAM-only and simpler? *(Recommend: keep core target-agnostic — cheap to maintain, nice for FE.)*
5. **Repo home:** under `cargopete/` alongside wren?

---

## 11. Why this is a good open-source bet

- **Real gap:** there is no off-the-shelf, embeddable, *temporal* data-lineage engine for the BEAM — or many ecosystems. OpenLineage is a spec + heavy Java/Python stack; this is a small, typed, embeddable library.
- **Distilled from production:** the temporal model, atomic writes, and AGE concurrency fixes are battle-tested (FTHM-16805), not invented.
- **Composes with wren:** two small birds from the same nest — a tidy, demonstrable story.
- **Apache AGE needs love:** almost nothing in the BEAM world speaks AGE; `tern_age` would be a genuine first.
```
       wren  (AMQP, Gleam)            ──┐
                                        ├──►  tern  (lineage, Gleam)  ──►  Apache AGE / Postgres
       wisp/mist (HTTP+SSE, Gleam)    ──┘
```
