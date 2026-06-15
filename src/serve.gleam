//// Runnable entry point for the HTTP server (dev/demo):
////   docker compose up -d
////   gleam run -m serve

import gleam/erlang/process
import gleam/io
import gleam/option.{Some}
import pog
import tern/age
import tern/server

pub fn main() {
  let assert Ok(started) =
    pog.default_config(process.new_name("tern_serve"))
    |> pog.host("localhost")
    |> pog.port(5455)
    |> pog.database("tern")
    |> pog.user("postgres")
    |> pog.password(Some("postgres"))
    |> pog.start
  let store = age.backend(started.data)
  let assert Ok(_) = server.start(store, 8080)
  io.println("tern server listening on http://localhost:8080")
  process.sleep_forever()
}
