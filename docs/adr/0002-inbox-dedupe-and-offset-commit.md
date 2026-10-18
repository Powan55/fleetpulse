# 0002 Inbox dedupe and offset commit

## Context
No event Kafka has acknowledged may be lost, and none may be applied twice. Kafka redelivers after crashes and rebalances, and the simulator sends duplicates on purpose. Inbox insert, state update, history, alert change and outbox row must commit together, and the Kafka offset only after that. Load is around 1000 events per second.

## Options
- Kafka transactions / exactly-once. Only covers Kafka-to-Kafka, not Postgres.
- One database transaction per record. Simple, but 1000 commits a second and one device update per event.
- Batch listener, one transaction per poll batch, deduped by `event_id`. Chosen.

## Decision
- Spring Kafka batch listener, `AckMode.BATCH` (offsets commit after the listener returns), auto-commit off. Values are read as strings and parsed in app code, with no polymorphic JSON typing.
- Per batch:
  1. Validate every record. Validation has to be at least as strict as the database constraints, so a record that passes can never fail an insert: full-match patterns, real UUID and timestamp parsing, key equal to `device_id`, size limit, epoch not far in the future. Invalid records go to the dead-letter topic synchronously. Nothing is thrown.
  2. Drop in-batch duplicates in memory.
  3. `INSERT INTO inbox ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id`. Ids not returned are duplicates and have no further effect.
  4. Insert new device rows, then lock the batch's device rows in id order (no deadlocks).
  5. Apply each device's new events in `(epoch, seq)` order (see 0003, 0005).
  6. Write everything, send the change notification, commit.
- Error split:
  - Data errors (SQLState class 22 or 23) mean validation missed something or the processor has a bug. The batch is rolled back and retried one record per transaction. The record that fails again goes to the dead-letter topic with reason `db_constraint` and the original bytes; the rest commit. Any such record fails a chaos scenario, because a valid accepted event was dropped.
  - Everything else (lost connection, deadlock, timeouts, Kafka down) is retried forever with a 1 s backoff, pausing the consumer between attempts so it stays in the group. Infrastructure trouble never sends records to the dead-letter topic.
- Inbox rows are kept 30 minutes.

## Consequences
- Correctness comes from the inbox primary key plus the device row lock, not from partition ownership, so it holds during rebalances and with two replicas.
- A crash after a dead-letter publish but before the offset commit publishes that record again; dead-letter counts can exceed injected bad records.
- A non-data bug (bad SQL) blocks a partition with endless retries. It shows up in lag and a failure counter; that's preferred over dropping events.
- A duplicate arriving after the 30-minute window is caught as late but counted twice.
