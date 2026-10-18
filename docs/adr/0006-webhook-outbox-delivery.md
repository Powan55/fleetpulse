# 0006 Webhook delivery via outbox

## Context
Every alert transition (opened, acked, resolved) sends a webhook. Delivery must survive sink outages, retry with backoff, and never silently drop anything. Two processor replicas may run the worker. HTTP calls must never happen inside the state transaction.

## Options
- A separate delivery service. A fourth product service for one job.
- CDC (Debezium). New infrastructure with no requirement behind it.
- A polling worker in the processor that claims outbox rows with `FOR UPDATE SKIP LOCKED` and a lease. Chosen.

## Decision
- The outbox row is written in the same transaction as the alert change, by the processor or the api.
- Every processor replica polls every 250 ms and claims up to 50 rows, delivering 10 in parallel. The claim sets `next_attempt_at` to now plus a lease (timeout + 5 s), so a crashed worker's rows come back automatically.
- A row is only claimable if no earlier row of the same alert is still pending. Transitions of one alert are delivered strictly in order; a dead row doesn't block the next one.
- The HTTP POST happens outside any transaction, with a 5 s timeout and headers `Idempotency-Key` and `traceparent`.
- Results:
  - 2xx: delivered.
  - 408, 429, 5xx, timeout, connection error: retry after 1 s, doubling to a 30 s cap, with 20% jitter. After 20 attempts (about 7.5 minutes) the row is dead.
  - Any other 4xx: dead right away. The sink only returns 400 for a malformed payload, which is a bug and should be loud, not retried for minutes.
- Dead rows are kept and counted. Delivered rows are purged after 24 h.
- Idempotency key is `alert-<id>-<transition>`. Each transition happens at most once per alert, so the key is stable across retries and replicas.

## Consequences
- A crash after the POST but before marking it delivered causes a duplicate delivery with the same key. That's the at-least-once trade.
- One alert whose deliveries keep failing only delays that alert's later transitions.
- Target throughput is about 100 deliveries a second per replica with a healthy sink.
