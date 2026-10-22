"""A fault becomes a state change and alerts within the bound, and clearing it walks the device back on the hysteresis schedule."""
import time
from datetime import datetime, timezone

import m3
from chaos import Window
from lib import Checks, db_query, sim

BATCH = 10
BATCHES = 10
SEE_MAX_S = 8
SEE_MIN_S = 4.0
STEP_S = 0.15
CLEAR_WINDOW = {"FAILED": ((8.0, 11.0), (18.0, 21.0)), "DEGRADED": ((8.0, 11.0), None)}


def _poll_until(ids, seen, t0, probe, timeout):
    """probe(device_set_not_seen) -> subset now satisfied. Records seconds since t0."""
    while len(seen) < len(ids) and time.monotonic() - t0 < timeout:
        for d in probe([i for i in ids if i not in seen]):
            seen[d] = time.monotonic() - t0
        time.sleep(STEP_S)


def trial(level, metric, ids):
    rule = m3.METRIC_RULE[metric]
    fid = m3.fault(ids, metric, level=level)
    t0 = time.monotonic()
    vis = {}

    def visible(todo):
        st = m3.devices_in(level)
        pairs = m3.open_pairs()
        return [d for d in todo if d in st and (d, rule) in pairs and (level != "FAILED" or (d, "device_failed") in pairs)]

    _poll_until(ids, vis, t0, visible, 20)
    sim("DELETE", f"/faults/{fid}")
    t1 = time.monotonic()
    first, healthy, still_failed_alert = {}, {}, []
    if level == "FAILED":

        def recovering(todo):
            st = m3.devices_in("RECOVERING")
            hit = [d for d in todo if d in st]
            pairs = m3.open_pairs()
            still_failed_alert.extend(d for d in hit if (d, "device_failed") in pairs)
            return hit

        _poll_until(ids, first, t1, recovering, 30)

    def is_healthy(todo):
        st = m3.devices_in("HEALTHY")
        return [d for d in todo if d in st]

    _poll_until(ids, healthy, t1, is_healthy, 60)
    rule_open = m3.open_pairs()
    return vis, first, healthy, still_failed_alert, [d for d in ids if (d, rule) in rule_open]


def run(c: Checks):
    w = Window(c)
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    pool = m3.pick(m3.device_ids(), 2 * BATCH * BATCHES, 24)
    k = 0
    for level in ("DEGRADED", "FAILED"):
        vis, first, healthy, stale, unresolved, ids_all = {}, {}, {}, [], [], []
        for b in range(BATCHES):
            ids = pool[k * BATCH:(k + 1) * BATCH]
            k += 1
            ids_all += ids
            v, f, h, s, u = trial(level, m3.METRICS[b % 3], ids)
            vis.update(v), first.update(f), healthy.update(h)
            stale += s
            unresolved += u
        lat = sorted(vis.values())
        p99 = m3.pctl(lat, 0.99)
        c.add(f"{level}: every fault visible as state and alerts", len(vis) == len(ids_all), f"{len(vis)} of {len(ids_all)} within 20 s")
        c.add(f"{level}: visible within {SEE_MAX_S} s at p99", p99 is not None and p99 <= SEE_MAX_S, f"p50 {m3.pctl(lat, 0.5):.2f} p95 {m3.pctl(lat, 0.95):.2f} p99 {p99:.2f} max {lat[-1]:.2f} s, poll step {STEP_S} s" if lat else "no samples")
        c.add(f"{level}: never visible before 3 samples ({SEE_MIN_S} s)", bool(lat) and lat[0] >= SEE_MIN_S, f"min {lat[0]:.2f} s" if lat else "no samples")
        (lo, hi), full = CLEAR_WINDOW[level]
        if level == "FAILED":
            bad = {d: t for d, t in first.items() if not lo <= t <= hi}
            c.add(f"FAILED: RECOVERING within [{lo:g}, {hi:g}] s of the clear", len(first) == len(ids_all) and not bad, f"{len(first)} of {len(ids_all)} seen, {len(bad)} outside; min {min(first.values(), default=0):.2f} max {max(first.values(), default=0):.2f} s")
            c.add("FAILED: device_failed is resolved when RECOVERING is first visible", not stale, f"{len(stale)} still open")
            lo, hi = full
        bad = {d: t for d, t in healthy.items() if not lo <= t <= hi}
        c.add(f"{level}: HEALTHY within [{lo:g}, {hi:g}] s of the clear", len(healthy) == len(ids_all) and not bad, f"{len(healthy)} of {len(ids_all)} seen, {len(bad)} outside; min {min(healthy.values(), default=0):.2f} max {max(healthy.values(), default=0):.2f} s")
        c.add(f"{level}: metric alerts resolved once HEALTHY", not unresolved, f"{len(unresolved)} still open")
    failed_ids = pool[BATCH * BATCHES:]
    same = db_query(
        "SELECT count(DISTINCT a.device_id) FROM alert a JOIN alert b ON b.device_id = a.device_id AND b.rule_id = 'device_failed' AND b.opened_at = a.opened_at "
        "WHERE a.rule_id IN ('temperature_high', 'battery_low', 'error_burst') AND a.device_id = ANY(%s) AND a.opened_at >= %s",
        (failed_ids, since),
    )[0][0]
    c.add("FAILED: metric alert and device_failed opened in the same transaction", same == len(failed_ids), f"{same} of {len(failed_ids)} devices share an opened_at")
    w.finish(c, None)
