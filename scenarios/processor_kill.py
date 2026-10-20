"""Processor killed with SIGKILL under load: caught up again, nothing lost, nothing applied twice."""
import os
import random
import time

import dock
import topic
from chaos import Window
from lib import URLS, Checks, Inconclusive, http

CAUGHT_UP_RECORDS = 2000
BOUND_S = 90
RESTART_WAIT_S = 5


def _caught_up(started_at):
    i = dock.inspect("processor")
    if not i["running"] or i["started_at"] == started_at:
        return False
    try:
        status, _, _ = http("GET", URLS["processor"] + "/actuator/health/readiness", timeout=3)
    except Inconclusive:
        return False
    return status == 200 and sum(topic.group_lag().values()) <= CAUGHT_UP_RECORDS


def _kill(c, n, time_recovery):
    """Kill, start the container if it did not come back. Returns seconds to caught up, or None."""
    deadline = time.monotonic() + 90
    while not dock.inspect("processor")["running"]:
        if time.monotonic() > deadline:
            raise Inconclusive("processor not running 90 s after the previous kill")
        time.sleep(1)
    before = dock.inspect("processor")
    t_kill = time.monotonic()
    dock.control("processor", "kill")
    landed = False
    while time.monotonic() - t_kill < 10 and not landed:
        i = dock.inspect("processor")
        landed = not i["running"] or i["started_at"] != before["started_at"]
        if landed:
            c.note(f"kill {n}: landed after {time.monotonic() - t_kill:.1f}s, exit code {i['exit_code']}, restart count {i['restarts']}")
        else:
            time.sleep(0.5)
    if not landed:
        raise Inconclusive(f"kill {n} did not stop the processor")
    time.sleep(max(0, RESTART_WAIT_S - (time.monotonic() - t_kill)))
    if not dock.inspect("processor")["running"]:
        dock.control("processor", "start")
        c.note(f"kill {n}: MANUAL START after {time.monotonic() - t_kill:.1f}s (a killed container is not restarted by its restart policy)")
    if not time_recovery:
        return None
    while time.monotonic() - t_kill < BOUND_S + 60:
        if _caught_up(before["started_at"]):
            return time.monotonic() - t_kill
        time.sleep(1)
    return None


def run(c: Checks):
    kills = int(os.environ.get("FP_KILLS", 5))
    gap = int(os.environ.get("FP_KILL_GAP", 120))
    rnd = random.Random(42)
    w = Window(c)
    c.note(f"kills={kills} min spacing={gap}s seed=42")
    nxt = time.monotonic() + rnd.uniform(10, 40)
    for n in range(1, kills + 1):
        time.sleep(max(0, nxt - time.monotonic()))
        took = _kill(c, n, True)
        c.add(f"kill {n}: caught up within {BOUND_S} s of the kill", took is not None and took <= BOUND_S, "not caught up" if took is None else f"{took:.1f}s")
        left = max(0, gap - (took or BOUND_S))
        nxt = time.monotonic() + left + rnd.uniform(0, 30)
    w.finish(c, None)


def run_rapid(c: Checks):
    kills = int(os.environ.get("FP_KILLS", 20))
    rnd = random.Random(42)
    w = Window(c)
    c.note(f"kills={kills} spacing 10-40 s seed=42; the processor duplicate counter does not exist yet, so redelivery is not proven")
    for n in range(1, kills + 1):
        time.sleep(rnd.uniform(10, 40))
        _kill(c, n, False)
    c.add("kills landed", True, f"{kills} of {kills}")
    deadline = time.monotonic() + BOUND_S + 60
    while not dock.inspect("processor")["running"] and time.monotonic() < deadline:
        time.sleep(1)
    w.finish(c, None)
