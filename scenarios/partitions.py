"""Partition balance: per-partition produced rate and the sweep lag limit it must support."""
import time

import dock
import topic
from chaos import seconds
from lib import Checks, Inconclusive, wait_flowing
from reconcile import dlq_check

LIMIT_FACTOR = 7
DEFAULT_MAX_LAG = 1000


def run(c: Checks):
    ok, detail, _ = wait_flowing(120)
    if not ok:
        raise Inconclusive(f"fleet not flowing: {detail}")
    secs = seconds(60)
    start = int(time.time() * 1000)
    a = topic.watermarks()
    t0 = time.monotonic()
    time.sleep(secs)
    b = topic.watermarks()
    dt = time.monotonic() - t0
    rate = {p: (b[p][1] - a[p][1]) / dt for p in a}
    mean = sum(rate.values()) / len(rate)
    print("    partition  records  ev/s")
    for p in sorted(rate):
        print(f"    {p:>9}  {b[p][1] - a[p][1]:>7}  {rate[p]:.1f}")
    r_min, r_max = min(rate.values()), max(rate.values())
    c.note(f"mean {mean:.1f} ev/s, min {r_min:.1f}, max {r_max:.1f}, window {dt:.0f}s")
    env = dock.inspect("processor")["env"].get("FP_SWEEP_MAX_LAG")
    limit = int(env) if env else DEFAULT_MAX_LAG
    c.add(f"sweep lag limit {limit}{'' if env else ' (default, not set in the container)'} <= {LIMIT_FACTOR} x slowest partition", limit <= LIMIT_FACTOR * r_min, f"{LIMIT_FACTOR} x {r_min:.1f} = {LIMIT_FACTOR * r_min:.0f}")
    c.add("no partition above 1.25 x mean", r_max <= 1.25 * mean, f"max/mean = {r_max / mean:.2f}")
    dlq_check(c, start)
