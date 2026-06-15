//// tern/consumer — ingest lineage events from RabbitMQ via wren.
////
//// The whole event is the retry unit: a transient storage failure asks wren to
//// redeliver (`Retry`); an undecodable or permanently-failing event is
//// dead-lettered. This is the natural fit for `TernError.is_transient` and
//// closes the loop with wren — wren carries the events, tern remembers them.

import gleam/json
import tern/core/storage.{type StorageBackend}
import tern/ingest
import tern/wire
import wren

/// Subscribe to `queue` and apply each delivered event to `store`. Returns the
/// supervised wren consumer (which restarts and re-subscribes on crash).
pub fn start(
  channel: wren.Channel,
  queue: String,
  store: StorageBackend,
) -> Result(wren.Consumer, wren.WrenError) {
  wren.start_consumer(channel, queue, fn(msg) { handle(msg, store) })
}

/// Like `start`, but with wren's retry/dead-letter infrastructure: `Retry`
/// verdicts flow through the delay queues, `DeadLetter` (and exhausted retries)
/// land on the DLQ.
pub fn start_with_retry(
  channel: wren.Channel,
  infra: wren.RetryInfrastructure,
  store: StorageBackend,
) -> Result(wren.Consumer, wren.WrenError) {
  wren.start_consumer_with_retry(channel, fn(msg) { handle(msg, store) }, infra)
}

/// Settle one delivery. Exposed for wiring; see `process` for the decode+apply
/// logic (which is testable without a broker).
pub fn handle(
  message: wren.Message,
  store: StorageBackend,
) -> wren.Confirmation {
  case wren.message_text(message) {
    Ok(text) -> process(text, store)
    Error(_) -> wren.DeadLetter
  }
}

/// Decode an event payload and apply it atomically, returning the broker
/// verdict. Pure of any wren mailbox concerns, so it's unit-testable with just a
/// JSON string and a backend.
pub fn process(payload: String, store: StorageBackend) -> wren.Confirmation {
  case json.parse(payload, wire.event_decoder()) {
    Error(_) -> wren.DeadLetter
    Ok(#(tenant, ev)) -> {
      let _ = store.ensure_ready(tenant)
      case store.write(tenant, fn(s) { ingest.apply(s, ev) }) {
        Ok(_) -> wren.Ack
        Error(e) ->
          case storage.is_transient(e) {
            True -> wren.Retry
            False -> wren.DeadLetter
          }
      }
    }
  }
}
