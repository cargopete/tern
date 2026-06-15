//// tern/age — the Apache AGE storage backend.
////
//// Implements `tern/core/storage.StorageBackend` over `pog` against a Postgres
//// database with the AGE extension. The graph lives in an AGE graph per tenant;
//// node snapshots live in a plain table in the SAME database, so one Postgres
//// transaction covers both (the atomic-write guarantee).
////
//// M2 scope: ensure-ready, atomic writes (node/edge/snapshot), find, idempotent
//// upserts. Temporal traversal (`query_at_time`) lands in M3.

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import gleam/time/timestamp.{type Timestamp}
import pog
import tern/core/model.{
  type Identity, type Node, type NodeId, type PropValue, type Tenant, B, F, I,
  NodeId, Null, S,
}
import tern/core/storage.{
  type EdgeUpsert, type NodeUpsert, type PagedGraph, type StorageBackend,
  type TernError, type TimelineQuery, type WriteSession, Permanent, Transient,
  WriteSession,
}
import youid/uuid

/// Build a `StorageBackend` from a started pog connection. The connection's
/// database must have the AGE extension and auto-load it per session (see the
/// project's `db/init.sql`).
pub fn backend(conn: pog.Connection) -> StorageBackend {
  storage.StorageBackend(
    ensure_ready: fn(tenant) { ensure_ready(conn, tenant) },
    find_node: fn(tenant, identity) { find_node(conn, tenant, identity) },
    query_at_time: fn(query) { query_at_time(conn, query) },
    write: fn(tenant, work) { write(conn, tenant, work) },
  )
}

// --- ensure ready ----------------------------------------------------------

fn ensure_ready(
  conn: pog.Connection,
  tenant: Tenant,
) -> Result(Nil, TernError) {
  let g = model.graph_name(tenant)

  // snapshot table (shared database → same transaction as the graph writes)
  use _ <- result.try(exec(
    conn,
    "CREATE TABLE IF NOT EXISTS tern_node_snapshot (
       id          bigserial PRIMARY KEY,
       graph       text        NOT NULL,
       node_id     text        NOT NULL,
       external_id text        NOT NULL,
       kind        text        NOT NULL,
       role        text        NOT NULL,
       name        text        NOT NULL,
       properties  jsonb       NOT NULL DEFAULT '{}',
       valid_from  bigint      NOT NULL,
       recorded_at timestamptz NOT NULL DEFAULT now()
     )",
  ))

  // create the graph if it isn't already in the AGE catalogue
  use exists <- result.try(graph_exists(conn, g))
  use _ <- result.try(case exists {
    True -> Ok(Nil)
    False -> exec(conn, "SELECT 1 FROM (SELECT create_graph('" <> g <> "')) _x")
  })

  // pre-create labels so the first concurrent MERGE can't race on implicit
  // table creation. Errors (label already exists) are ignored.
  list.each(["Origin", "Entity", "Operation", "Consumer"], fn(label) {
    let _ =
      exec(
        conn,
        "SELECT 1 FROM (SELECT create_vlabel('"
          <> g
          <> "', '"
          <> label
          <> "')) _x",
      )
    Nil
  })
  let _ =
    exec(
      conn,
      "SELECT 1 FROM (SELECT create_elabel('" <> g <> "', 'flows_into')) _x",
    )

  Ok(Nil)
}

fn graph_exists(conn: pog.Connection, g: String) -> Result(Bool, TernError) {
  let row = {
    use n <- decode.field(0, decode.int)
    decode.success(n)
  }
  let q =
    pog.query("SELECT count(*) FROM ag_catalog.ag_graph WHERE name = $1")
    |> pog.parameter(pog.text(g))
    |> pog.returning(row)
  case pog.execute(q, conn) {
    Ok(returned) ->
      case returned.rows {
        [n, ..] -> Ok(n > 0)
        [] -> Ok(False)
      }
    Error(e) -> Error(Transient(string.inspect(e)))
  }
}

// --- write (atomic) --------------------------------------------------------

fn write(
  conn: pog.Connection,
  tenant: Tenant,
  work: fn(WriteSession) -> Result(Nil, TernError),
) -> Result(Nil, TernError) {
  let g = model.graph_name(tenant)
  let outcome =
    pog.transaction(conn, fn(tx) {
      let session =
        WriteSession(
          create_node: fn(u) { create_node(tx, g, u) },
          create_edge: fn(u) { create_edge(tx, g, u) },
          soft_delete_node: fn(id, at) { soft_delete_node(tx, g, id, at) },
          store_snapshot: fn(node) { store_snapshot(tx, g, node) },
        )
      work(session)
    })
  case outcome {
    Ok(_) -> Ok(Nil)
    Error(pog.TransactionRolledBack(e)) -> Error(e)
    Error(pog.TransactionQueryError(e)) -> Error(Transient(string.inspect(e)))
  }
}

fn create_node(
  tx: pog.Connection,
  g: String,
  u: NodeUpsert,
) -> Result(NodeId, TernError) {
  let id = u.identity
  let fresh = uuid.v7_string()
  // MERGE on (external_id, kind) for this role's label; AGE has no ON CREATE
  // SET, so SET unconditionally and `coalesce` the create-only node_id.
  let cy =
    "MERGE (n:"
    <> model.role_label(id.role)
    <> " {external_id:'"
    <> esc(id.external_id)
    <> "', kind:'"
    <> esc(id.kind)
    <> "'}) SET n.name = '"
    <> esc(u.name)
    <> "', n.valid_from = "
    <> unix(u.valid_from)
    <> ", n.node_id = coalesce(n.node_id, '"
    <> fresh
    <> "') RETURN n.node_id"
  use rows <- result.try(cypher(tx, g, cy))
  case rows {
    [v, ..] -> Ok(NodeId(dequote(v)))
    [] -> Error(Permanent("create_node returned no node_id"))
  }
}

fn create_edge(
  tx: pog.Connection,
  g: String,
  u: EdgeUpsert,
) -> Result(Nil, TernError) {
  let NodeId(from) = u.from
  let NodeId(to) = u.to
  let cy =
    "MATCH (a {node_id:'"
    <> esc(from)
    <> "'}), (b {node_id:'"
    <> esc(to)
    <> "'}) MERGE (a)-[e:"
    <> esc(u.label)
    <> "]->(b) SET e.valid_from = coalesce(e.valid_from, "
    <> unix(u.valid_from)
    <> ") RETURN e.valid_from"
  use _ <- result.try(cypher(tx, g, cy))
  Ok(Nil)
}

fn soft_delete_node(
  tx: pog.Connection,
  g: String,
  node: NodeId,
  at: Timestamp,
) -> Result(Nil, TernError) {
  let NodeId(nid) = node
  let t = unix(at)
  // soft-delete the node, then its incident edges (both directions)
  use _ <- result.try(cypher(
    tx,
    g,
    "MATCH (n {node_id:'"
      <> esc(nid)
      <> "'}) SET n.deleted_at = "
      <> t
      <> " RETURN n.node_id",
  ))
  use _ <- result.try(cypher(
    tx,
    g,
    "MATCH (n {node_id:'"
      <> esc(nid)
      <> "'})-[e]-(m) SET e.deleted_at = "
      <> t
      <> " RETURN count(e)",
  ))
  Ok(Nil)
}

fn store_snapshot(
  tx: pog.Connection,
  g: String,
  node: Node,
) -> Result(Nil, TernError) {
  let q =
    pog.query(
      "INSERT INTO tern_node_snapshot
         (graph, node_id, external_id, kind, role, name, properties, valid_from)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)",
    )
    |> pog.parameter(pog.text(g))
    |> pog.parameter(pog.text(node_id_string(node.id)))
    |> pog.parameter(pog.text(node.external_id))
    |> pog.parameter(pog.text(node.kind))
    |> pog.parameter(pog.text(model.role_label(node.role)))
    |> pog.parameter(pog.text(node.name))
    |> pog.parameter(pog.text(props_json(node.properties)))
    |> pog.parameter(pog.int(unix_int(node.valid_from)))
  case pog.execute(q, tx) {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(Transient(string.inspect(e)))
  }
}

// --- read ------------------------------------------------------------------

fn find_node(
  conn: pog.Connection,
  tenant: Tenant,
  id: Identity,
) -> Result(Option(NodeId), TernError) {
  let g = model.graph_name(tenant)
  let cy =
    "MATCH (n:"
    <> model.role_label(id.role)
    <> " {external_id:'"
    <> esc(id.external_id)
    <> "', kind:'"
    <> esc(id.kind)
    <> "'}) RETURN n.node_id"
  use rows <- result.try(cypher(conn, g, cy))
  case rows {
    [v, ..] -> Ok(Some(NodeId(dequote(v))))
    [] -> Ok(None)
  }
}

fn query_at_time(
  _conn: pog.Connection,
  _query: TimelineQuery,
) -> Result(PagedGraph, TernError) {
  Error(Permanent("query_at_time arrives in M3"))
}

// --- cypher plumbing -------------------------------------------------------

/// Run a Cypher query against a graph, returning each row's first column as
/// text (the agtype result is cast to text so pog can decode it).
fn cypher(
  conn: pog.Connection,
  g: String,
  cy: String,
) -> Result(List(String), TernError) {
  let sql =
    "SELECT v::text FROM cypher('"
    <> g
    <> "', $$ "
    <> cy
    <> " $$) AS (v agtype)"
  let row = {
    use v <- decode.field(0, decode.string)
    decode.success(v)
  }
  case pog.query(sql) |> pog.returning(row) |> pog.execute(conn) {
    Ok(returned) -> Ok(returned.rows)
    Error(e) -> Error(classify(e))
  }
}

/// Plain SQL with no rows of interest.
fn exec(conn: pog.Connection, sql: String) -> Result(Nil, TernError) {
  case pog.query(sql) |> pog.execute(conn) {
    Ok(_) -> Ok(Nil)
    Error(e) -> Error(classify(e))
  }
}

/// Map a pog error to transient/permanent. Connection-level failures are
/// retryable; query structure errors are not.
fn classify(e: pog.QueryError) -> TernError {
  case e {
    pog.ConnectionUnavailable -> Transient("connection unavailable")
    pog.PostgresqlError(_, _, msg) -> Transient(msg)
    other -> Permanent(string.inspect(other))
  }
}

// --- helpers ---------------------------------------------------------------

/// Escape single quotes for embedding in a Cypher string literal.
/// (Parameterised Cypher is a hardening follow-up.)
fn esc(s: String) -> String {
  string.replace(s, "'", "''")
}

/// agtype scalars come back quoted (e.g. `"abc"`); strip surrounding quotes.
fn dequote(s: String) -> String {
  string.replace(s, "\"", "")
}

fn unix(t: Timestamp) -> String {
  int.to_string(unix_int(t))
}

fn unix_int(t: Timestamp) -> Int {
  let #(secs, _nanos) = timestamp.to_unix_seconds_and_nanoseconds(t)
  secs
}

fn node_id_string(id: NodeId) -> String {
  let NodeId(s) = id
  s
}

fn props_json(props: Dict(String, PropValue)) -> String {
  props
  |> dict.to_list
  |> list.map(fn(kv) {
    let #(k, v) = kv
    #(k, prop_to_json(v))
  })
  |> json.object
  |> json.to_string
}

fn prop_to_json(v: PropValue) -> json.Json {
  case v {
    S(s) -> json.string(s)
    I(i) -> json.int(i)
    F(f) -> json.float(f)
    B(b) -> json.bool(b)
    Null -> json.null()
  }
}
