# 0003 Event ordering and ids

## Context
Each device's `seq` must increase, the same simulator seed should give the same event ids, late events must never change state, and gaps must be counted. A simulator restart resets its in-memory counters, which would make every new event look old.

## Options
- Persist each device's seq counter. A crash can lose the last increments, reuse a seq and its event id, and then the dedupe silently drops new data.
- Random UUID per event. Not reproducible.
- Add the simulator's boot time (`epoch`) to the envelope and order by `(epoch, seq)`. Chosen.

## Decision
- `epoch` = `SIM_EPOCH` if set, else Unix ms at simulator boot. `seq` restarts at 1 per epoch.
- `event_id` = UUID v3 (`UUID.nameUUIDFromBytes`) of `seed:device_id:epoch:seq`. It is assigned at generation and reused for resends, duplicates and replays. With a pinned `SIM_EPOCH`, the same seed gives the same ids; by default each boot gets a new epoch, so a restarted simulator never collides with ids already in the inbox.
- These ids are uniformly spread. At 30 minutes of inbox (about 1.8 million rows) the primary-key index is roughly 90 MB, which fits Postgres's 384 MB buffer budget, so time-ordered ids aren't needed.
- Kafka key = `device_id`, so each device stays on one partition and keeps its order (the idempotent producer preserves order with up to 5 requests in flight).
- For each new event, compared to the device's last applied `(epoch, seq)`:
  - not greater: late. Recorded and counted, device row untouched.
  - same epoch, seq jumps: applied, and the gap size is counted.
  - higher epoch: applied as a fresh start.
  - otherwise: applied.
- An epoch more than 24 h ahead of the broker's append time is rejected, so one forged record can't freeze a device.

Topics:

| Topic | Partitions | Settings |
|---|---|---|
| `fleetpulse.telemetry.v1` | 6 | retention 2 h, 4 GiB cap, `message.timestamp.type=LogAppendTime` |
| `fleetpulse.telemetry.v1.dlq` | 1 | retention 24 h |

Six partitions split evenly over 1, 2 or 3 consumers. Auto-create is off; an init container creates the topics.

## Consequences
- Reconciliation is per device and current epoch.
- If the host clock goes backwards between simulator restarts, the new epoch is smaller and its events are treated as late.
