"""An old, already applied event sent again by a producer other than the simulator changes nothing."""
import time

import topic
from chaos import Window, now_ms
from lib import Checks, Inconclusive, db_query, poll, sim

MIN_AGE_S = 550
MAX_AGE_S = 1800
LOOK_BACK_S = 700


def _find():
    """First telemetry record on the topic from about 700 s ago, as a raw message."""
    for m in topic.read(topic.TOPIC, now_ms() - LOOK_BACK_S * 1000):
        d = topic.envelope(m)
        if d and d["type"] == "telemetry":
            return m, d
    raise Inconclusive("no telemetry record on the topic older than 10 min: let the stack run longer")


def _snap(dev, epoch, eid):
    return (
        db_query("SELECT applied_count, max_seq FROM v_reconciliation WHERE device_id = %s AND epoch = %s", (dev, epoch)),
        db_query("SELECT sum(applied_count) FROM v_reconciliation WHERE epoch = %s", (epoch,)),
        db_query("SELECT count(*) FROM inbox WHERE device_id = %s AND epoch = %s", (dev, epoch)),
        db_query("SELECT count(*) FROM telemetry WHERE device_id = %s AND epoch = %s", (dev, epoch)),
        db_query("SELECT event_id, received_at FROM inbox WHERE event_id = %s", (eid,)),
        db_query("SELECT count(*) FROM late_event WHERE event_id = %s", (eid,)),
    )


def _lag_zero():
    return sum(topic.group_lag().values()) == 0


def run(c: Checks):
    w = Window(c)
    m, d = _find()
    dev, epoch, eid = d["device_id"], d["epoch"], d["event_id"]
    row = db_query("SELECT extract(epoch FROM now() - received_at) FROM inbox WHERE event_id = %s", (eid,))
    if not row:
        raise Inconclusive(f"event {eid} is not in the inbox: it was never applied or the inbox row is pruned")
    age = float(row[0][0])
    if not MIN_AGE_S < age < MAX_AGE_S:
        raise Inconclusive(f"applied event is {age:.0f} s old, need more than {MIN_AGE_S} s and under {MAX_AGE_S} s")
    c.add("the event to resend is applied and inside the dedupe window", True, f"{dev} seq {d['seq']}, {age:.0f} s old")
    sim("PUT", "/generation", {"paused": True})
    ok, _ = poll(_lag_zero, 120)
    if not ok:
        raise Inconclusive("consumer lag not 0 after the pause")
    before = _snap(dev, epoch, eid)
    topic.produce(m.key(), m.value())
    time.sleep(3)
    ok, _ = poll(_lag_zero, 60)
    after = _snap(dev, epoch, eid)
    c.add("the resent record was consumed", ok, "lag back to 0")
    c.add("applied_count and max_seq of the device unchanged", before[0] == after[0], f"{before[0]} -> {after[0]}")
    c.add("applied_count of the whole fleet unchanged", before[1] == after[1], f"{before[1]} -> {after[1]}")
    c.add("no new inbox or telemetry row for the device", before[2:4] == after[2:4], f"inbox {before[2]} -> {after[2]}, telemetry {before[3]} -> {after[3]}")
    c.add("the original inbox row was not rewritten and nothing went to late_event", before[4] == after[4] and after[4] and after[5][0][0] == 0, f"{after[4]}")
    c.note("processor duplicate counter +1 is checked once the counter is exported")
    w.finish(c, None)
