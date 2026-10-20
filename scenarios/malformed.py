"""Malformed records go to the dead-letter topic with a reason and never block valid events."""
import time
from collections import Counter

import topic
from chaos import Window
from lib import Checks, Inconclusive, sim

KINDS = ("invalid_json", "missing_field", "bad_type", "oversize")
EACH = 100
EXPECT = {"invalid_json": 100, "schema": 200, "too_large": 100}


def run(c: Checks):
    w = Window(c)
    for kind in KINDS:
        sim("POST", "/chaos/malformed", {"count": EACH, "kind": kind})
        time.sleep(3)
    w.k1 = sim("GET", "/counters")
    if w.delta("malformedInjected") != EACH * len(KINDS):
        raise Inconclusive(f"malformedInjected moved by {w.delta('malformedInjected')}, expected {EACH * len(KINDS)}")
    c.add("malformed records injected", True, f"{w.delta('malformedInjected')}")
    w.finish(c)
    reasons = Counter(r for r, _ in topic.dlq(w.since))
    c.add("dead-letter records >= injected", sum(reasons.values()) >= EACH * len(KINDS), str(dict(reasons)))
    for reason, n in EXPECT.items():
        c.add(f"dead-letter reason {reason} >= {n}", reasons.get(reason, 0) >= n, f"{reasons.get(reason, 0)}")
