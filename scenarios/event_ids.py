"""Event ids on the topic are exactly the ones derived from the ledger's accepted sequence numbers."""
import time

import topic
from chaos import now_ms, seconds
from lib import Checks, Inconclusive, sim, wait_flowing
from reconcile import dlq_check, event_id


def run(c: Checks):
    ok, detail, _ = wait_flowing(120)
    if not ok:
        raise Inconclusive(f"fleet not flowing: {detail}")
    sim("PUT", "/generation", {"paused": False})
    led0 = sim("GET", "/ledger")
    start = now_ms()
    time.sleep(seconds(60))
    try:
        sim("PUT", "/generation", {"paused": True})
        led = sim("GET", "/ledger")
        if led["epoch"] != led0["epoch"]:
            raise Inconclusive("simulator restarted during the scenario")
        m0 = {d["deviceId"]: d["maxAcceptedSeq"] for d in led0["devices"]}
        expected = {}
        for d in led["devices"]:
            dev = d["deviceId"]
            rng = sim("GET", f"/ledger/devices/{dev}")["acceptedSeqRanges"]
            expected[dev] = {event_id(led["seed"], dev, led["epoch"], s) for lo, hi in rng for s in range(lo, hi + 1) if s > m0[dev]}
        by, nrec, conflicts = topic.events(start - 5000, led["epoch"], min_seq=m0)
    finally:
        sim("PUT", "/generation", {"paused": False})
    got = {dev: {e for e, _ in s.values()} for dev, s in by.items()}
    bad = [d for d in sorted(expected.keys() | got.keys()) if expected.get(d, set()) != got.get(d, set())]
    c.add("topic event ids equal the ledger-derived ids for every device", not bad, f"{len(bad)} devices differ; {nrec} records, {sum(map(len, expected.values()))} ledger events")
    for d in bad[:50]:
        print(f"      {d}: ledger-only {len(expected.get(d, set()) - got.get(d, set()))}, topic-only {len(got.get(d, set()) - expected.get(d, set()))}")
    c.add("no seq carries two different event ids", not conflicts, str(len(conflicts)))
    dlq_check(c, start)
