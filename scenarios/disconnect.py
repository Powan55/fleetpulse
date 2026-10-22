"""Devices that lose their connection buffer events and replay them in order; a tiny buffer overflows by a known amount."""
import time
from datetime import datetime, timedelta, timezone

import dock
import m3
from chaos import Window
from lib import Checks, Inconclusive, db_query, parse_ts, poll, sim, sink

GONE = 200
FAULTED = 20
RATE = 1.0  # events per device per second: one heartbeat and one telemetry sample every 2 s


def _now():
    return datetime.now(timezone.utc)


def _sleep_until(t):
    time.sleep(max(0.0, (t - _now()).total_seconds()))


def _n(sql, ids):
    return db_query(sql, (list(ids),))[0][0]


def run_disconnect(c: Checks, window_s, replay_bound_s, healthy_bound_s):
    w = Window(c)
    gone = m3.pick(m3.device_ids(), GONE, 20)
    faulted = m3.pick(gone, FAULTED, 21)
    d = m3.disconnect(gone, window_s * 1000)
    t_gone = _now()
    until = parse_ts(d["until"])
    if sim("GET", "/counters")["disconnectedDevices"] != GONE:
        raise Inconclusive("disconnectedDevices is not 200 right after the call: the disconnect did not take effect")
    c.add("200 devices are disconnected", True, f"{window_s} s window, until {until:%H:%M:%S}")
    _sleep_until(t_gone + timedelta(seconds=10))
    m3.fault(faulted, "temperature", level="DEGRADED", duration_ms=10_000)
    _sleep_until(until - timedelta(seconds=1))
    offline = _n("SELECT count(*) FROM device WHERE device_id = ANY(%s) AND state = 'OFFLINE'", gone)
    buffered = sim("GET", "/counters")
    c.add("all 200 devices are OFFLINE before they reconnect", offline == GONE, f"{offline} OFFLINE, {buffered['bufferedEvents']} events buffered")
    replayed0 = buffered["replayed"]
    _sleep_until(until)
    t0 = time.monotonic()
    ok, took = poll(lambda: sim("GET", "/counters")["replayed"] - replayed0 >= buffered["bufferedEvents"], replay_bound_s * 3, 0.25)
    c.add(f"replay finished within {replay_bound_s} s of the reconnect", ok and took <= replay_bound_s, f"{took:.1f} s, replayed {sim('GET', '/counters')['replayed'] - replayed0} of {buffered['bufferedEvents']} buffered")
    ok, took = poll(lambda: _n("SELECT count(*) FROM device WHERE device_id = ANY(%s) AND state <> 'HEALTHY'", gone) == 0, healthy_bound_s * 3, 0.25)
    took = time.monotonic() - t0
    c.add(f"all 200 devices HEALTHY within {healthy_bound_s} s of the reconnect", ok and took <= healthy_bound_s, f"{took:.1f} s")
    poll(lambda: _n("SELECT count(*) FROM alert WHERE device_id = ANY(%s) AND status <> 'RESOLVED'", gone) == 0, 30, 0.5)
    k = sim("GET", "/counters")
    c.add("no buffer overflow", k["bufferOverflow"] - w.k0["bufferOverflow"] == 0, f"{k['bufferOverflow'] - w.k0['bufferOverflow']}")
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    sweeps = dict(db_query("SELECT device_id, count(*) FROM state_transition WHERE cause = 'offline_sweep' AND occurred_at >= %s GROUP BY 1", (since,)))
    c.add("exactly one offline sweep per disconnected device and none for others", set(sweeps) == set(gone) and set(sweeps.values()) == {1}, f"{len(sweeps)} devices swept, counts {sorted(set(sweeps.values()))}")
    off = db_query("SELECT device_id, count(*), bool_and(status = 'RESOLVED' AND resolved_by = 'auto') FROM alert WHERE rule_id = 'device_offline' AND opened_at >= %s GROUP BY 1", (since,))
    c.add("one device_offline alert per disconnected device, resolved, no duplicates", {r[0] for r in off} == set(gone) and all(r[1] == 1 and r[2] for r in off), f"{len(off)} devices, counts {sorted({r[1] for r in off})}")
    met = db_query("SELECT device_id, rule_id, count(*), bool_and(backfilled), bool_and(status = 'RESOLVED') FROM alert WHERE rule_id NOT IN ('device_offline') AND opened_at >= %s GROUP BY 1, 2", (since,))
    want = {(f, "temperature_high") for f in faulted}
    c.add("each faulted device has exactly one backfilled temperature alert, opened and resolved, and no other device has a metric alert", {(r[0], r[1]) for r in met} == want and all(r[2] == 1 and r[3] and r[4] for r in met), f"{len(met)} metric alert groups, expected {len(want)}")
    ids = dict(db_query("SELECT device_id, alert_id FROM alert WHERE rule_id = 'temperature_high' AND opened_at >= %s", (since,)))
    bad = []
    for dev in faulted:
        ok, _ = poll(lambda: len(sink("GET", f"/deliveries?alertId={ids.get(dev, 0)}&limit=10")) >= 2, 45, 1.0)
        got = {g["transition"]: g["backfilled"] for g in sink("GET", f"/deliveries?alertId={ids.get(dev, 0)}&limit=10")}
        if got != {"opened": True, "resolved": True}:
            bad.append((dev, got))
    c.add("each backfilled alert reached the sink as opened and resolved with backfilled=true", not bad, f"{len(faulted) - len(bad)} of {len(faulted)} ok" + (f"; first {bad[:2]}" if bad else ""))
    w.finish(c, None)


def run(c: Checks):
    run_disconnect(c, 60, 12, 20)


def run_long(c: Checks):
    run_disconnect(c, 300, 60, 70)


def run_overflow(c: Checks):
    cap = dock.inspect("simulator")["env"].get("SIM_BUFFER_CAPACITY")
    if cap != "10":
        raise Inconclusive(f"simulator buffer capacity is {cap}, not 10: recreate the simulator with SIM_BUFFER_CAPACITY=10 (see the README), then run this again")
    w = Window(c)
    n, window_s = 100, 30
    gone = m3.pick(m3.device_ids(), n, 22)
    d = m3.disconnect(gone, window_s * 1000)
    t_gone = _now()
    until = parse_ts(d["until"])
    _sleep_until(until - timedelta(seconds=0.5))
    k = sim("GET", "/counters")
    held = (_now() - t_gone).total_seconds()
    over = k["bufferOverflow"] - w.k0["bufferOverflow"]
    if over <= 0:
        raise Inconclusive("no event overflowed: the buffer is not at capacity 10")
    want = n * (held * RATE - int(cap))
    c.add("overflow is generated minus buffered for every disconnected device", abs(over - want) <= 2 * n, f"overflow {over}, expected about {want:.0f} = {n} devices x ({held:.1f} s x {RATE:g} ev/s - {cap}); tolerance 2 per device for the window edges")
    c.add("every disconnected device holds a full buffer", k["bufferedEvents"] >= n * int(cap), f"bufferedEvents {k['bufferedEvents']} for {n} devices x {cap}")
    _sleep_until(until)
    w.finish(c, None)
