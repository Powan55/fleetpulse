"""Webhook delivery while the receiver is down, failing a third of the time, or slower than the sender's timeout."""
import time

import m3
from chaos import Window
from lib import CLEANUP, Checks, Inconclusive, db_now, db_query, poll, sim, sink

DEVICES = 50
CYCLE = [("DEGRADED", 30), ("OK", 30)]
LEAD_S = 10
TAIL_S = 60
DELIVER_AFTER_RESTORE_S = 45


def _pending(t):
    return db_query("SELECT count(*) FROM outbox WHERE status = 'PENDING' AND created_at <= %s", (t,))[0][0]


def _run(c: Checks, mode, mode_s, guard_key, bound):
    w = Window(c)
    m3.fault(m3.pick(m3.device_ids(), DEVICES, 30), "temperature", steps=CYCLE)
    acker = m3.Acker()
    acker.start()
    CLEANUP.append(acker.finish)
    time.sleep(LEAD_S)
    sink("PUT", "/mode", mode)
    time.sleep(mode_s)
    sink("PUT", "/mode", {})
    t_restore = db_now()
    t0 = time.monotonic()
    ok, _ = poll(lambda: _pending(t_restore) == 0, 180, 0.5)
    took = time.monotonic() - t0
    s = sink("GET", "/deliveries/summary")
    if s[guard_key] <= 0:
        raise Inconclusive(f"sink {guard_key} is {s[guard_key]}: the mode did not take effect ({s})")
    c.add(f"sink {guard_key} > 0", True, f"{s}")
    if bound:
        c.add(f"everything created before the restore delivered within {bound} s", ok and took <= bound, f"{took:.1f} s")
    else:
        c.note(f"everything created before the restore delivered after {took:.1f} s")
    time.sleep(TAIL_S)
    sim("DELETE", "/faults")
    acker.finish()
    c.add("alerts were acked through the api during the run", len(acker.done) > 0 and acker.errors == 0, f"{len(acker.done)} acked, {acker.errors} errors")
    w.finish(c, None)


def run_outage(c: Checks):
    _run(c, {"outage": True}, 120, "rejectedInjected", DELIVER_AFTER_RESTORE_S)


def run_errors(c: Checks):
    _run(c, {"errorRate": 0.3}, 120, "rejectedInjected", None)


def run_slow(c: Checks):
    _run(c, {"delayMs": 6000}, 60, "duplicates", None)
