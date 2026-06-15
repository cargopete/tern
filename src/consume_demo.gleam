//// End-to-end demo of the wren → tern consumer loop:
////   docker compose up -d
////   gleam run -m consume_demo
////
//// Connects wren to RabbitMQ, starts the tern consumer against AGE, publishes
//// one lineage event, then queries the graph to prove it was ingested.

import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{Some}
import gleam/time/timestamp
import pog
import tern/age
import tern/consumer
import tern/core/model.{Identity, Origin, Tenant}
import tern/core/storage.{Downstream, TimelineQuery}
import wren

const queue = "tern.events"

pub fn main() {
  // AGE store
  let assert Ok(started) =
    pog.default_config(process.new_name("tern_consume"))
    |> pog.host("localhost")
    |> pog.port(5455)
    |> pog.database("tern")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.start
  let store = age.backend(started.data)
  let tenant = Tenant("consumer-demo")
  let assert Ok(_) = store.ensure_ready(tenant)

  // wren → RabbitMQ
  let assert Ok(conn) = wren.connect(wren.default_config())
  let assert Ok(channel) = wren.open_channel(conn)
  let assert Ok(_) = wren.declare_queue(channel, queue)
  let assert Ok(_) = consumer.start(channel, queue, store)
  io.println("✓ consumer subscribed to '" <> queue <> "'")

  // publish a lineage event
  let payload =
    "{\"tenant\":\"consumer-demo\",\"role\":\"origin\",\"externalId\":\"q-src\","
    <> "\"kind\":\"postgres\",\"name\":\"Queue Source\",\"operation\":\"create\","
    <> "\"occurredAt\":1000,\"targets\":[{\"externalId\":\"q-raw\",\"kind\":\"asset\"}]}"
  let assert Ok(_) =
    wren.publish_text(channel, exchange: "", routing_key: queue, text: payload)
  io.println("✓ published an event")

  // give the consumer a moment, then read the graph back
  process.sleep(2000)
  let assert Ok(g) =
    store.query_at_time(TimelineQuery(
      tenant,
      Identity("q-src", "postgres", Origin),
      timestamp.from_unix_seconds(2000),
      Downstream,
      5,
      0,
      100,
    ))
  io.println(
    "✓ graph now has "
    <> int.to_string(list.length(g.nodes))
    <> " nodes and "
    <> int.to_string(list.length(g.edges))
    <> " edges (expected 2 and 1)",
  )
}
