# tern

Embeddable, **temporal data-lineage** for the BEAM — written in [Gleam](https://gleam.run),
backed by [Apache AGE](https://age.apache.org) (Cypher inside PostgreSQL).

> A tern flies the longest **migration** of any bird. This library tracks where your
> data came from, where it went, and what the graph looked like at any point in time —
> the lineage equivalent of never losing the thread on a very long journey.

Distilled from a production data-lineage service, rebuilt generic and open. Sibling to
[`wren`](https://github.com/cargopete/wren) (the AMQP library) — wren carries the
events, **tern** remembers where they came from.

## Status

**M0 — spike (early).** The foundational risk is retired: Gleam (`pog`) talks to
Apache AGE, runs Cypher mutations and reads, and brings `agtype` back into Gleam
values. Run it yourself in under a minute (below).

What `tern` is growing into — the roadmap:

- ✅ **M0** · Gleam ↔ Apache AGE round-trip (create graph, Cypher write/read, decode `agtype`)
- ✅ **M1** · `tern_core` — node/edge model, lineage events, the `StorageBackend` behaviour, temporal `as-of` logic (pure, no I/O; 12 tests)
- ✅ **M2** · `tern_age` — the AGE backend: per-tenant graphs, idempotent node/edge upserts (`MERGE` + `coalesce`), atomic writes (graph + snapshot in one transaction), find. 5 integration tests against real AGE.
- ⬜ **M3** · temporal traversal (`as-of(T)`, up/down/both, depth, pagination) + soft-delete revival
- ⬜ **M3.5** · concurrent-writer hardening (per-label unique constraint — AGE's `MERGE` isn't atomic)
- ⬜ **M4** · `tern_server` — `wisp`/`mist` HTTP API: ingest, query, **SSE streaming**, health
- ⬜ **M5** · `tern_consumer` — `wren`-driven event ingestion with retry / dead-letter (every event is the retry unit)
- ⬜ **M6** · docs, examples, Hex publish

## The idea

Lineage is a directed graph of how data flows, made of four node roles joined by
`flows_into` edges:

```
Origin ──▶ Entity ──▶ Operation ──▶ Entity ──▶ Consumer
(source)   (asset)    (transform)   (asset)    (dashboard)
```

Every mutation is **temporal** — nodes and edges carry `valid_from` / `deleted_at`,
so any query can ask "what did this graph look like at time T?". Each event's graph
mutations and its node snapshot land in **one PostgreSQL transaction** — all or nothing.

## Quick start

```sh
# 1. Stand up Apache AGE (provisions the `tern` db + extension automatically)
docker compose up -d

# 2. Run the M0 spike
gleam run
```

You should see a five-node lineage pipeline written and read back via Cypher.

## Design

- **Storage is a behaviour, not a hard dependency.** A `StorageBackend` record-of-functions
  fronts every graph operation; the AGE/Postgres backend is the first implementation.
- **Temporal by default.** Soft-deletes and `as-of` traversal, not destructive updates.
- **Atomic ingestion.** Graph + snapshot share one database so a single transaction covers both.
- **Multi-tenant.** One AGE graph per tenant namespace (`tern_<hash>`), disjoint from anything else.

See [`oss-gleam-lineage-plan.md`](https://github.com/cargopete/tern) for the full extraction plan.

## Development

```sh
docker compose up -d   # Apache AGE on localhost:5455
gleam run              # run the spike
gleam test             # run the tests
gleam format           # format
```

## License

MIT
