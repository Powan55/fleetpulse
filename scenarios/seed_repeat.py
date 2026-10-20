"""Same seed and pinned epoch: the simulator emits the same events, and the second run is fully deduplicated."""
import time

import dock
import topic
from chaos import now_ms, seconds
from lib import URLS, Checks, Inconclusive, db_query, http, poll, sim
from reconcile import dlq_check, quiesce

MIN_COMPARED = 15000


def _ready():
    try:
        return http("GET", URLS["simulator"] + "/actuator/health/readiness", timeout=3)[0] == 200
    except Inconclusive:
        return False


def _boot():
    dock.control("simulator", "restart")
    time.sleep(3)
    ok, _ = poll(_ready, 120)
    if not ok:
        raise Inconclusive("simulator not ready 120 s after restart")


def _applied(epoch):
    return db_query("SELECT coalesce(sum(applied_count), 0) FROM v_reconciliation WHERE epoch = %s", (epoch,))[0][0]


def run(c: Checks):
    if not dock.inspect("simulator")["env"].get("SIM_EPOCH"):
        raise Inconclusive("simulator has no pinned epoch: recreate it with SIM_EPOCH set above any earlier epoch, then run this again")
    start = now_ms()
    _boot()
    time.sleep(seconds(30))
    led1, _, _ = quiesce()
    epoch = led1["epoch"]
    applied1 = _applied(epoch)
    restart = now_ms()
    _boot()
    time.sleep(seconds(30) * 2 // 3)
    led2, _, _ = quiesce()
    sim("PUT", "/generation", {"paused": False})
    applied2 = _applied(epoch)
    if led2["epoch"] != epoch or led2["seed"] != led1["seed"]:
        raise Inconclusive("epoch or seed differ between the two runs")
    if led2["totalAccepted"] > led1["totalAccepted"]:
        raise Inconclusive("second run produced more events than the first; cannot compare as a subset")
    r1, _, _ = topic.events(start - 5000, epoch, until_ms=restart)
    r2, _, _ = topic.events(restart, epoch)
    compared = bad_id = bad_type = bad_payload = 0
    for dev, seqs in r2.items():
        for seq, (eid, payload, kind) in seqs.items():
            if seq in r1.get(dev, {}):
                compared += 1
                bad_id += r1[dev][seq][0] != eid
                bad_type += r1[dev][seq][2] != kind
                bad_payload += r1[dev][seq][1] != payload
    if compared < MIN_COMPARED:
        raise Inconclusive(f"only {compared} (device, seq) pairs in both runs")
    c.add("same event id for every (device, seq) in both runs", bad_id == 0, f"{compared} compared, {bad_id} differ")
    c.add("same event type for every (device, seq) in both runs", bad_type == 0, f"{bad_type} differ")
    c.add("same payload for every (device, seq) in both runs", bad_payload == 0, f"{bad_payload} differ")
    c.add("second run applied nothing new", applied2 == applied1, f"applied_count total {applied1} -> {applied2}")
    dlq_check(c, start)
