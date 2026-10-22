"""Offline detection: timing against the last acknowledged event, no false OFFLINE under clock skew or a processor outage."""
import time
from datetime import datetime, timezone

import dock
import m3
import processor_kill
import topic
from chaos import Window, seconds
from lib import CLEANUP, Checks, Inconclusive, db_query, parse_ts, poll, sim
from reconcile import LagSampler

GATE_LAG = 1000
OPEN_WINDOW = (9.9, 13.0)
REOPEN_S = 11.0
SKEW_MS = 300_000
DOWNTIME_S = 120
CATCH_UP_BOUND_S = 210


def judge(ack, occurred, trips, every=1.0):
    """ack, occurred: epoch seconds. trips: wall times of samples with a partition over the gate limit.

    Returns (tripped, ok, deadline_s) where deadline_s is seconds after the ack.
    """
    delta = occurred - ack
    inside = [t for t in trips if ack <= t <= occurred]
    if not inside:
        return False, OPEN_WINDOW[0] <= delta <= OPEN_WINDOW[1], OPEN_WINDOW[1]
    reopen = max(inside) + every
    deadline = max(OPEN_WINDOW[1], reopen + REOPEN_S - ack)
    return True, OPEN_WINDOW[0] <= delta <= deadline, deadline


def _sampler():
    s = LagSampler(1.0)
    s.start()
    CLEANUP.append(s.finish)
    return s, time.time() - time.monotonic()


def run_timing(c: Checks):
    w = Window(c)
    gone = m3.pick(m3.device_ids(), 100, 25)
    s, off = _sampler()
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    m3.disconnect(gone, 120_000)
    if sim("GET", "/counters")["disconnectedDevices"] != 100:
        raise Inconclusive("disconnectedDevices is not 100: the disconnect did not take effect")
    time.sleep(3)
    mine = set(gone)
    ack = {d["deviceId"]: parse_ts(d["lastAckAt"]).timestamp() for d in sim("GET", "/ledger")["devices"] if d["deviceId"] in mine and d["lastAckAt"]}
    got = {}

    def swept():
        got.update(dict(db_query("SELECT device_id, min(occurred_at) FROM state_transition WHERE cause = 'offline_sweep' AND device_id = ANY(%s) AND occurred_at >= %s GROUP BY 1", (gone, since))))
        return len(got) == len(gone)

    poll(swept, 60, 0.5)
    sim("DELETE", "/disconnects")
    s.finish()
    trips = [t + off for t, _, _, mx in s.samples if mx > GATE_LAG]
    c.add("all 100 devices were swept to OFFLINE", len(got) == len(gone) and len(ack) == len(gone), f"{len(got)} swept, {len(ack)} with a last ack")
    rows, tripped = [], 0
    for d, at in got.items():
        t, ok, dl = judge(ack[d], at.timestamp(), trips, s.every)
        tripped += t
        rows.append((d, round(at.timestamp() - ack[d], 2), t, ok, round(dl, 1)))
    deltas = sorted(r[1] for r in rows)
    bad = [r for r in rows if not r[3]]
    if deltas:
        c.add("no device went OFFLINE earlier than 9.9 s after its last ack", deltas[0] >= OPEN_WINDOW[0], f"min {deltas[0]:.2f} s")
        c.add(
            "gate-open windows within [9.9, 13] s, tripped windows by max(13 s, reopen + 11 s)",
            not bad,
            f"{len(rows)} devices, {tripped} in tripped windows, delta min {deltas[0]:.2f} median {deltas[len(deltas) // 2]:.2f} max {deltas[-1]:.2f} s; gate state from 1 Hz partition lag, reopen estimated",
        )
    for r in bad[:10]:
        print(f"      device, delta s, tripped, ok, deadline s: {r}")
    w.finish(c, None)


def run_skew(c: Checks):
    w = Window(c)
    w.apply({"skewSets": [{"fraction": 0.25, "offsetMs": SKEW_MS}, {"fraction": 0.25, "offsetMs": -SKEW_MS}]}, seconds(300))
    if w.delta("skewedEvents") <= 0:
        raise Inconclusive("no event was skewed: chaos did not take effect")
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    rows = db_query("SELECT avg(extract(epoch FROM event_time - received_at)) FROM telemetry WHERE received_at > now() - interval '60 seconds' GROUP BY device_id")
    plus, minus = sum(1 for r in rows if r[0] >= 290), sum(1 for r in rows if r[0] <= -290)
    if not (225 <= plus <= 275 and 225 <= minus <= 275):
        raise Inconclusive(f"skew not visible in telemetry: {plus} devices at +5 min, {minus} at -5 min of {len(rows)}, expected about 250 each")
    c.add("about 25% of devices carry +5 min and 25% -5 min event times", True, f"{plus} at +5 min, {minus} at -5 min of {len(rows)} devices")
    n = db_query("SELECT count(*) FROM state_transition WHERE cause = 'offline_sweep' AND occurred_at >= %s", (since,))[0][0]
    off = db_query("SELECT count(*) FROM device WHERE state = 'OFFLINE'")[0][0]
    c.add("no live device went OFFLINE", n == 0 and off == 0, f"{n} offline sweeps, {off} OFFLINE now")
    w.finish(c)


def run_downtime(c: Checks):
    w = Window(c)
    control = m3.pick(m3.device_ids(), 1, 15)[0]
    s, off = _sampler()
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    k0, t_k0 = sim("GET", "/counters"), time.monotonic()
    before = dock.inspect("processor")["started_at"]
    m3.disconnect([control], 3_600_000)
    dock.control("processor", "stop")
    if db_query("SELECT state FROM device WHERE device_id = %s", (control,))[0][0] == "OFFLINE":
        raise Inconclusive("control device was swept before the processor stopped; run again")
    ack = parse_ts(sim("GET", f"/ledger/devices/{control}")["lastAckAt"]).timestamp()
    t_stopped = time.monotonic()
    rate = (sim("GET", "/counters")["accepted"] - k0["accepted"]) / max(1.0, t_stopped - t_k0)
    time.sleep(DOWNTIME_S / 2)
    k1 = sim("GET", "/counters")
    t1 = time.monotonic()
    time.sleep(max(0.0, t_stopped + DOWNTIME_S - 1 - t1))
    k2 = sim("GET", "/counters")
    grew = k2["accepted"] - k1["accepted"]
    if grew < 10_000:
        raise Inconclusive(f"only {grew} events accepted in the last minute of the outage: devices stopped sending")
    c.add("devices kept sending while the processor was down", True, f"{grew} events accepted in {time.monotonic() - t1:.0f} s")
    dock.control("processor", "start")
    t_start = time.time()
    ok, took = poll(lambda: processor_kill._caught_up(before), CATCH_UP_BOUND_S + 120, 1.0)
    c.add(f"caught up within {CATCH_UP_BOUND_S} s of the start", ok and took <= CATCH_UP_BOUND_S, f"{took:.0f} s")
    opened = [t + off for t, _, _, mx in s.samples if t + off > t_start and mx <= GATE_LAG]
    t_gate = min(opened) if opened else None
    poll(lambda: sum(topic.group_lag().values()) == 0, 120, 1.0)
    t_zero = time.time()
    peak = max((x[1] for x in s.samples if x[0] + off > t_start), default=0)
    c.note(f"capacity probe: live rate {rate:.0f} ev/s, backlog after start {peak} records, drained in {max(1.0, t_zero - t_start):.0f} s, C = R + backlog / t = {rate + peak / max(1.0, t_zero - t_start):.0f} ev/s")
    time.sleep(12)
    swept = dict(db_query("SELECT device_id, min(occurred_at) FROM state_transition WHERE cause = 'offline_sweep' AND occurred_at >= %s GROUP BY 1", (since,)))
    others = [d for d in swept if d != control]
    c.add("no continuously sending device went OFFLINE during the outage, catch-up and grace", not others, f"{len(others)} devices" + (f"; first {others[:3]}" if others else ""))
    alerts = db_query("SELECT count(*) FROM alert WHERE rule_id = 'device_offline' AND device_id <> %s AND opened_at >= %s", (control, since))[0][0]
    c.add("no device_offline alert for a sending device", alerts == 0, f"{alerts}")
    at = swept.get(control)
    ok_time = at is not None and t_gate is not None and ack + OPEN_WINDOW[0] <= at.timestamp() and t_start <= at.timestamp() <= t_gate + REOPEN_S + 1.0
    detail = "never" if at is None else f"{at.timestamp() - t_start:.1f} s after start, {at.timestamp() - ack:.1f} s after its last ack"
    c.add("the stopped device went OFFLINE after the restart, by gate reopen + 11 s", ok_time, f"{detail}; gate reopened {(t_gate or 0) - t_start:.1f} s after start (1 Hz estimate, +1 s sampling)")
    sim("DELETE", "/disconnects")
    s.finish()
    w.finish(c, None)
