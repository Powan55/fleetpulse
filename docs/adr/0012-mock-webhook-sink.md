# 0012 Mock webhook sink

## Context
Webhooks need a receiver we control: one that records every delivery by idempotency key and can fail on demand (error rate, slow responses, full outage). It is a test helper, not part of the product.

## Options
- Built into the simulator. A simulator restart would wipe the sink's records, and the two test roles would be coupled.
- A separate small container. Chosen.
- An off-the-shelf mock server. Would need scripting for keyed counts and failure modes.

## Decision
- `sink/` is a small Spring Boot app with an in-memory store: key to count and timestamps, plus a bounded list of recent payloads. API in `contracts/sink.openapi.yaml`.
- It records a delivery only when it answers 2xx. Outages are simulated with `mode.outage` (503), not by stopping the container.
- Same health, tracing and logging setup as the services, so the webhook trace ends in the sink.

## Consequences
- One more JVM, about 150-250 MiB.
- Stopping the sink loses its records.
