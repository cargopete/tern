//// tern — open-source, temporal data-lineage on the BEAM.
////
//// M0 SPIKE: prove that Gleam (`pog`) can talk to Apache AGE — create a graph,
//// run Cypher mutations and reads, and bring `agtype` back into Gleam values.
//// This de-risks the whole project; it is intentionally throwaway and will be
//// replaced by `tern_core` + `tern_age` in M1/M2.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/string
import pog

const graph = "tern_demo"

pub fn main() {
  io.println("\n🐦  tern · M0 spike — Gleam → Apache AGE\n")

  // The `tern` database is configured (ALTER DATABASE) to preload the AGE
  // library and set the ag_catalog search_path on every session, so no
  // per-query `LOAD 'age'` is needed.
  let pool = process.new_name("tern_pool")
  let config =
    pog.default_config(pool)
    |> pog.host("localhost")
    |> pog.port(5455)
    |> pog.database("tern")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.pool_size(4)

  let assert Ok(started) = pog.start(config)
  let conn = started.data
  io.println("✓ connected to AGE Postgres (localhost:5455/tern)")

  // reset the graph
  let _ = call_void(conn, "drop_graph('" <> graph <> "', true)")
  let assert Ok(_) = call_void(conn, "create_graph('" <> graph <> "')")
  io.println("✓ graph '" <> graph <> "' created")

  // write a small lineage pipeline — the generic shape tern will model:
  // Origin → Entity → Operation → Entity → Consumer
  let assert Ok(_) =
    cypher(
      conn,
      "
      CREATE (src:Origin {name:'orders-db'}),
             (raw:Entity {name:'raw_orders'}),
             (etl:Operation {name:'enrich orders'}),
             (enr:Entity {name:'enriched_orders'}),
             (dash:Consumer {name:'orders dashboard'}),
             (src)-[:flows_into]->(raw),
             (raw)-[:flows_into]->(etl),
             (etl)-[:flows_into]->(enr),
             (enr)-[:flows_into]->(dash)
      RETURN src.name
    ",
    )
  io.println("✓ wrote a 5-node lineage pipeline\n")

  // read the nodes back
  let assert Ok(nodes) =
    cypher(conn, "MATCH (n) RETURN labels(n)[0] + ': ' + n.name")
  io.println("nodes (" <> int.to_string(list.length(nodes)) <> "):")
  list.each(nodes, fn(r) { io.println("  • " <> dequote(r)) })

  // read the edges back
  let assert Ok(edges) =
    cypher(
      conn,
      "MATCH (a)-[e]->(b) RETURN a.name + ' --' + type(e) + '--> ' + b.name",
    )
  io.println("\nedges (" <> int.to_string(list.length(edges)) <> "):")
  list.each(edges, fn(r) { io.println("  • " <> dequote(r)) })

  io.println("\n✅  M0 proven: Gleam runs AGE Cypher and reads agtype back.\n")
}

// --- helpers ---------------------------------------------------------------

/// Call a `void`-returning function (create_graph/drop_graph) safely. pg_types
/// has no codec for `void`, so we run the call inside a subquery and return a
/// text literal the driver can decode.
fn call_void(conn, fn_call: String) {
  let sql = "SELECT ''::text FROM (SELECT " <> fn_call <> ") AS _x"
  pog.query(sql) |> pog.execute(conn)
}

/// Run a Cypher query, returning each row's first column as text. The cypher()
/// result is declared `agtype` then cast to text in the outer SELECT, which is
/// what lets pog decode it without an agtype binary codec.
fn cypher(conn, c: String) {
  let sql =
    "SELECT v::text FROM cypher('"
    <> graph
    <> "', $$ "
    <> c
    <> " $$) AS (v agtype)"
  let row = {
    use v <- decode.field(0, decode.string)
    decode.success(v)
  }
  case pog.query(sql) |> pog.returning(row) |> pog.execute(conn) {
    Ok(returned) -> Ok(returned.rows)
    Error(e) -> Error(e)
  }
}

/// agtype text scalars come back quoted (e.g. "raw_orders"); strip the quotes.
fn dequote(s: String) -> String {
  string.replace(s, "\"", "")
}
