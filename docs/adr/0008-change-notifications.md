# 0008 Change notifications with LISTEN/NOTIFY

## Context
The api streams device and alert changes to browsers. Changes come from processor transactions and from the api's own ack/resolve. Notifications can be missed (api restart), so the UI must converge from snapshots anyway (0009).

## Options
- A state-change Kafka topic. Another producer in the processor's transaction path for a single consumer.
- The api polls change timestamps every 250 ms. Constant queries, and an index on a hot column that would slow the device updates.
- `pg_notify` inside the writing transaction, with the api listening. Chosen.

## Decision
- Channel `fleetpulse_changes`, payload of ids only: `{"t":"d","ids":[...]}` for devices, `{"t":"a","ids":[...]}` for alerts, at most 200 ids per call (Postgres limits payloads to 8000 bytes by default). Built with a JSON library and sent with bind parameters.
- Sent when a device row is created (so new devices appear live), when a device changes state, and when an alert is created or changed. Never for telemetry that only updates last-seen.
- Postgres delivers a notification only if the transaction commits, and folds identical payloads within one transaction.
- The api keeps one dedicated connection that LISTENs and polls for notifications from a single thread. It coalesces ids for 250 ms, re-reads the rows, and pushes them over SSE.
- If the listener connection drops (for example a Postgres restart), the api reconnects, re-LISTENs, and tells every client to re-snapshot.
- Any database role can NOTIFY, so the payload is only a hint: the api always re-reads current rows, and drops and counts payloads that don't parse.

## Consequences
- No new infrastructure or dependency.
- Commits that issue NOTIFY may serialize on Postgres's notification queue. Only state-change and alert transactions notify, so this is expected to be negligible; batch duration metrics will show it if not.
