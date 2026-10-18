# 0004 Offline detection

## Context
A device silent for 10 s should go OFFLINE, within about 10-13 s of its last acknowledged event. Clock skew of a few minutes must not matter. Processor downtime or lag must not mark devices offline that kept sending. With two processor replicas, each device must go offline exactly once.

## Options
- Device `event_time`. Skewed by design.
- Processor time of the last apply. After any downtime every device looks silent until its backlog is processed.
- Broker append time of the last applied event, compared to database time, with the sweep paused while the consumer is behind. Chosen.

## Decision
- The telemetry topic uses `LogAppendTime`, so each record carries the broker's clock. That becomes `device.last_seen_at` for every applied in-order event. Broker and database share the same machine clock.
- Every second, in every replica:
  1. Check consumer lag per partition with the Kafka AdminClient. Skip if the call fails or any partition is more than 1000 records behind. After lag clears, wait 10 s before sweeping again.
  2. In one transaction, mark devices with `last_seen_at` older than 10 s as OFFLINE, using a conditional UPDATE over rows selected `FOR UPDATE SKIP LOCKED` (at most 500 per run). Write the transition, open the offline alert, write outbox rows, notify.
- Two replicas sweeping at once can't double-fire: `SKIP LOCKED` plus `state <> 'OFFLINE'` means each device transitions once. Rows locked by an apply batch are skipped because they are being refreshed.
- Why a per-partition limit: a single stuck partition at around 167 events per second takes 12 s to reach 2000 records of total lag, but its devices look silent after 8 s. Per partition, 1000 records is reached in about 6 s.
- The coldest partition is the risk: the gate only closes in time if that partition carries at least 143 events per second (1000 / 7 s). Device keys are fixed, so the split is deterministic; check it once and lower the limit if needed. A lower limit only delays detection.
- After a Kafka outage the simulator replays buffered events round-robin, so every device gets a fresh append within about a second, well inside the 10 s wait.
- A test hook can pause one partition to prove the gate. It is only registered when explicitly enabled, auto-resumes after 300 s, and the processor port is never published.

## Consequences
- One AdminClient call per second per replica.
- Devices that really died during a processor outage are flagged after catch-up plus 10 s, not immediately.
- If the partition balance ever forces a limit low enough to trip in normal operation, store the partition on each device row and skip only devices on lagging partitions.
