"""Flapping metrics on 200 devices: transitions and alerts equal the reference model, with hand-derived counts for the simple patterns."""
import time
from datetime import datetime, timezone

import m3
import oracle
from chaos import Window, seconds
from lib import Checks, Inconclusive, db_now, db_query, sim

GROUP = 25
HOLD_S = 135  # 120 s of flapping plus 15 s so the last RECOVERING -> HEALTHY of the long pattern lands before the cut


def _groups():
    """(name, level, steps, duration_ms) for 8 groups of 25 devices."""
    g = []
    for lv in ("DEGRADED", "FAILED"):
        g.append((f"P1 {lv}", lv, [(lv, 1), ("OK", 1)], None))
        g.append((f"P2 {lv}", lv, [(lv, 3), ("OK", 4)], None))
        g.append((f"P3 {lv}", lv, [(lv, 3), ("OK", 5)], 112_000))
    g.append(("P4 escalation", None, [("DEGRADED", 3), ("FAILED", 3), ("OK", 5)], None))
    g.append(("P4 escalation", None, [("DEGRADED", 3), ("FAILED", 3), ("OK", 5)], None))
    return g


def _runs(events):
    out, n = [], 0
    for _, sample, _ in events:
        if sample is not None and oracle.severity(sample) != oracle.OK:
            n += 1
        elif n:
            out.append(n)
            n = 0
    return out + ([n] if n else [])


def _hand(name, level, runs, tr, al, rule):
    """Expected (transitions, {rule: (opened, resolved)}) or None when the pattern did not run as designed."""
    if name.startswith("P1"):
        return 0, {}
    if name.startswith("P2"):
        return 1, ({rule: (1, 0)} if level == "DEGRADED" else {rule: (1, 0), "device_failed": (1, 0)})
    if name.startswith("P3") and runs == [3] * 7:
        return (14, {rule: (7, 7)}) if level == "DEGRADED" else (15, {rule: (7, 7), "device_failed": (7, 7)})
    return None


def run(c: Checks):
    w = Window(c)
    ids = m3.pick(m3.device_ids(), 200, 23)
    groups, members = _groups(), {}
    for g, (name, level, steps, dur) in enumerate(groups):
        mine = ids[g * GROUP:(g + 1) * GROUP]
        metric = m3.METRICS[g % 3]
        m3.fault(mine, metric, steps=steps, duration_ms=dur)
        members[g] = (mine, metric)
    time.sleep(seconds(HOLD_S))
    since = datetime.fromtimestamp(w.since / 1000, timezone.utc)
    n = db_query("SELECT count(*) FROM (SELECT device_id FROM telemetry WHERE device_id = ANY(%s) AND received_at >= %s AND (temperature_c >= 70 OR battery_pct <= 20 OR error_count >= 5) GROUP BY 1 HAVING count(*) >= 3) x", (ids, since))[0][0]
    if n < 190:
        raise Inconclusive(f"only {n} of 200 devices produced 3 bad samples: the patterns did not take effect")
    c.add("patterns took effect on the devices", True, f"{n} of 200 devices produced 3 or more bad samples")
    t_cut = db_now()
    sim("DELETE", "/faults")
    w.finish(c, None, t_cut=t_cut)
    data = (w.result or {}).get("window")
    if not data:
        c.note("hand-derived counts skipped: no reference-model window")
        return
    checked = skipped = 0
    bad = []
    for g, (name, level, steps, _) in enumerate(groups):
        mine, metric = members[g]
        rule = m3.METRIC_RULE[metric]
        for d in mine:
            runs = _runs([(e[0], e[2], e[3]) for e in data["events"].get(d, [])])
            exp = _hand(name, level, runs, None, None, rule)
            if exp is None:
                skipped += name.startswith("P3")
                continue
            tr = data["transitions"].get(d, [])
            al = {}
            for r, status, *_ in data["alerts"].get(d, []):
                o, res = al.get(r, (0, 0))
                al[r] = (o + 1, res + (status == "RESOLVED"))
            checked += 1
            if (len(tr), al) != exp:
                bad.append((d, name, f"transitions {len(tr)} alerts {al}", f"expected {exp}"))
    c.add("hand-derived counts hold for P1, P2 and P3 devices", not bad and checked > 0, f"{checked} devices checked, {skipped} P3 devices skipped (not exactly 7 runs of 3 bad samples)")
    for b in bad[:20]:
        print(f"      {b}")
