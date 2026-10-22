"""Shared scenario plumbing: baseline, a measured chaos window, reconciliation at the end."""
import os
import time

import m3
from lib import CLEANUP, Checks, Inconclusive, db_query, sim, wait_flowing
from reconcile import LagSampler, Monitor, event_id, reconcile

LAG_P99 = 2000
LAG_MAX = 5000
AGE_P99_S = 5


def seconds(default):
    return int(os.environ.get("FP_SCENARIO_SECONDS", default))


def now_ms():
    return int(time.time() * 1000)


class Window:
    """Starts monitors on a flowing, chaos-free baseline; apply() holds a chaos setting and snapshots counters."""

    def __init__(self, c: Checks):
        if sim("GET", "/generation")["paused"]:
            sim("PUT", "/generation", {"paused": False})
        sim("PUT", "/chaos", {})
        ok, detail, _ = wait_flowing(120)
        if not ok:
            raise Inconclusive(f"fleet not flowing at start: {detail}")
        c.note(f"baseline: {detail}")
        m3.settle()
        self.since = now_ms()
        led = sim("GET", "/ledger")
        self.seed, self.epoch = led["seed"], led["epoch"]
        self.base = {d["deviceId"]: (d["acceptedCount"], d["maxAcceptedSeq"]) for d in led["devices"]}
        self.k0 = sim("GET", "/counters")
        self.mon, self.lag = Monitor(), LagSampler()
        self.mon.start()
        self.lag.start()
        CLEANUP.append(self.abort)

    def abort(self):
        for t in (self.mon, self.lag):
            t.finish()
        for fn in (m3.reset_controls, lambda: sim("PUT", "/generation", {"paused": False})):
            try:
                fn()
            except Inconclusive as e:
                print(f"  note  cleanup step failed: {e}")

    def apply(self, chaos, secs):
        sim("PUT", "/chaos", chaos)
        time.sleep(secs)
        self.k1 = sim("GET", "/counters")

    def delta(self, key):
        return self.k1[key] - self.k0[key]

    def finish(self, c: Checks, lag_check="nfr12", t_cut=None, allow_dead=False):
        if sim("GET", "/ledger/devices/dev-0001")["epoch"] != self.epoch:
            raise Inconclusive("simulator restarted during the scenario (epoch changed)")
        try:
            self.result = reconcile(c, self.since, mon=self.mon, base=self.base, t_cut=t_cut, allow_dead=allow_dead)
        finally:
            self.mon.finish()
            self.lag.finish()
        if lag_check is None:
            return
        p99, mx, n = self.lag.pct(0.99), self.lag.pct(1.0), len(self.lag.samples)
        if lag_check == "nfr12":
            c.add("total lag p99 <= 2000 and max <= 5000 records", p99 is not None and p99 <= LAG_P99 and mx <= LAG_MAX, f"p99={p99} max={mx} over {n} samples")
        else:
            age = self.lag.age_p99()
            c.add(f"lag-based record age p99 <= {AGE_P99_S} s (lag / ingest rate, not a measured age)", age is not None and age <= AGE_P99_S, f"p99={age if age is None else round(age, 2)} s, lag p99={p99} max={mx}, {n} samples")


def event_id_sample(c: Checks, seed, n=300):
    rows = db_query("SELECT event_id, device_id, epoch, seq FROM inbox ORDER BY random() LIMIT %s", (n,))
    bad = [r for r in rows if str(r[0]) != event_id(seed, r[1], r[2], r[3])]
    c.add("sampled inbox event ids equal UUID v3 of seed:device:epoch:seq", rows and not bad, f"{len(rows)} sampled, {len(bad)} differ")
