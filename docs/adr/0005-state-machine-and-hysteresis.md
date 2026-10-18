# 0005 State machine and hysteresis

## Context
Devices are HEALTHY, DEGRADED, FAILED, OFFLINE or RECOVERING. A metric oscillating around a threshold must not cause state ping-pong or an alert storm. When a device reconnects and replays a buffered fault that already ended, the alert must still be raised and resolved. Replay squeezes minutes of data into seconds, so any time-window rule would act differently on live and replayed data.

## Decision
- No clock. Hysteresis counts in-order telemetry samples. Heartbeats carry no metrics and don't count; late events don't count. Constants come from `contracts/rules.yaml`: 3 samples to worsen, 5 to improve, 5 in RECOVERING to reach HEALTHY.
- Each sample's severity is the worst over temperature, battery and error count: OK, DEGRADED or FAILED. Three counters on the device row track consecutive bad, failed and OK samples.

| From | To | When |
|---|---|---|
| HEALTHY, DEGRADED, RECOVERING | FAILED | 3 failed samples in a row |
| HEALTHY, RECOVERING | DEGRADED | 3 bad samples in a row |
| DEGRADED | HEALTHY | 5 OK samples in a row |
| FAILED | RECOVERING | 5 OK samples in a row |
| RECOVERING | HEALTHY | 5 OK samples in a row |
| OFFLINE | RECOVERING | any in-order event, heartbeat included |
| any except OFFLINE | OFFLINE | offline sweep (0004) |

  First match wins, at most one transition per event. Entering RECOVERING or OFFLINE resets the counters. So OFFLINE to HEALTHY takes 5 healthy samples, and FAILED to HEALTHY takes 10.
- Metric alert rules keep their own streaks per device and rule: open after 3 breaching samples, auto-resolve after 5 clear ones. If an operator resolves an alert while the condition persists, the streak restarts and a new alert needs 3 fresh breaching samples (about 4-7 s live).
- State rules (offline, failed) open on entering the state and resolve on leaving it. If an operator resolves one while the state persists, it re-opens 6 s later.
- An alert is `backfilled` when the sample that opened it was replayed from the device buffer. A fault that started and ended during a disconnect therefore produces one alert, opened and resolved, two webhooks, both backfilled.

## Consequences
- The rules are a pure function of state, counters and sample, easy to unit test and to reimplement as a reference model in tests.
- Live, 3 samples take about 6 s; replayed, nearly 0 s. That's intended.
- The densest flapping loop (RECOVERING to DEGRADED to FAILED and back) gives at most 3 transitions per 22 s per device.
- Streaks live on the device row, so one UPDATE per device per batch covers state and rules.
