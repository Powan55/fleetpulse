# 0007 Who writes alert rows

## Context
Operators ack and resolve alerts through the api. That write and its outbox row must commit together and be idempotent: acking an acked alert is a no-op, acking or resolving a resolved alert is a conflict. The processor auto-resolves alerts at the same time.

## Options
- The api sends a command and the processor applies it. An extra async hop, and the UI has to wait.
- Optimistic locking with a version column and a retry loop. More code than needed.
- Both writers use single-statement conditional updates on `status`; the row lock decides who wins. Chosen.

## Decision
- Ack: `UPDATE alert SET status='ACKED', ... WHERE alert_id=? AND status='OPEN' RETURNING *`. One row back: insert the outbox row and notify. No row: if already ACKED return 200 unchanged, if RESOLVED return 409, if missing 404.
- Resolve: same, `WHERE status IN ('OPEN','ACKED')`, `resolved_by='operator'`.
- Processor auto-resolve: same statement with `resolved_by='auto'`. No row means someone resolved it first; no outbox row.
- Processor open: `INSERT ... ON CONFLICT (device_id, rule_id) WHERE status IN ('OPEN','ACKED') DO NOTHING`. No row means one is already open, not an error.
- Each transition succeeds at most once per alert, so there is one outbox row per transition.
- The api's database role can update only the alert status columns and insert into the outbox. It cannot touch devices, history or telemetry. No sequence grant is needed for the outbox insert.
- Only the processor's worker sends webhooks, including for rows the api wrote.

## Consequences
- No lost updates: Postgres row locking plus the re-checked WHERE clause serialize the two writers.
- Acks work while the processor is down; the webhook just waits in the outbox.
