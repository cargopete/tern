# Changelog

## Unreleased

The initial build, milestone by milestone. Not yet published to Hex — see
[`RELEASING.md`](./RELEASING.md) for the prerequisites.

- **M0 — spike.** Proved Gleam (`pog`) can talk to Apache AGE: create a graph, run
  Cypher mutations and reads, and decode `agtype` back into Gleam values.
- **M1 — `tern/core`.** The pure model: `Node`/`Edge`/`NodeRole`/`Identity`/`Tenant`,
  lineage `LineageEvent`s with `implied_edges`, the `StorageBackend` behaviour, and the
  temporal `is_live_at` predicate. No I/O. (12 tests)
- **M2 — `tern/age`.** The Apache AGE backend: per-tenant graphs, idempotent node/edge
  upserts (`MERGE` + `coalesce`), atomic writes (graph + snapshot in one transaction),
  `find_node`. (5 integration tests)
- **M3 — traversal.** `query_at_time`: as-of(T), upstream/downstream/both, depth bounds,
  pagination — depth/direction in Cypher, temporal filtering via `is_live_at`. (6 tests)
- **M4 — `tern/server`.** A `wisp`/`mist` HTTP API (`POST /v1/events`, `GET /v1/graph`,
  `/health`) plus `tern/ingest` (event → graph). Verified end-to-end over HTTP.
- **M5 — `tern/consumer`.** A [wren](https://github.com/cargopete/wren)-driven event
  consumer; `Ack`/`Retry`/`DeadLetter` mapped from `TernError.is_transient`. Verified
  end-to-end over RabbitMQ. Shared `tern/wire` JSON codec. (2 tests)

26 tests total, green against real AGE + RabbitMQ.
