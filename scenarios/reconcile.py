"""Reconciliation of the simulator ledger against the database and the topic, after quiescence.

The pure comparison functions take plain dicts so they can be tested without a stack.
"""
import hashlib
import re
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import m3checks
import oracle
import topic
from lib import Checks, Inconclusive, db_now, db_query, poll, sim, sink

MAX_ROWS = 50
TAMPER = re.compile(r"^ledger:(dev-[0-9]{4,6}):([+-][0-9]+)$")
QUIESCE_S = 300
INBOX_WINDOW_S = 25 * 60


def event_id(seed, device, epoch, seq):
    return str(uuid.UUID(bytes=hashlib.md5(f"{seed}:{device}:{epoch}:{seq}".encode()).digest(), version=3))


def epoch_since_ms(epoch):
    """Boot epochs are Unix ms; a pinned epoch may not be, then read the whole topic."""
    return epoch - 60_000 if 1_500_000_000_000 < epoch < 2_500_000_000_000 else 0


def counts_mismatch(left, right):
    """left/right {device: (count, max_seq)}; absent = (0, 0). Returns [(device, left, right)]."""
    return [(d, left.get(d, (0, 0)), right.get(d, (0, 0))) for d in sorted(left.keys() | right.keys()) if left.get(d, (0, 0)) != right.get(d, (0, 0))]


def gap_mismatch(ledger, stats):
    """Exact missing-seq count for devices with no late events. stats {device: (late_count, gap_count)}."""
    out = []
    for d, (accepted, max_seq) in sorted(ledger.items()):
        late, gap = stats.get(d, (0, 0))
        if late == 0 and gap != max_seq - accepted:
            out.append((d, max_seq - accepted, gap))
    return out


def counter_gap(k):
    """generated minus everything it can turn into; 0 at quiescence."""
    return k["generated"] - (k["accepted"] + k["dropped"] + k["bufferOverflow"] + k["bufferedEvents"])


def diagnose(ranges, topic_seqs, inbox_seqs):
    ledger_seqs = {s for lo, hi in ranges for s in range(lo, hi + 1)}
    if ledger_seqs - set(topic_seqs):
        return "ledger has acks the topic lacks (lost in Kafka, or ledger over-count)"
    if set(topic_seqs) - set(inbox_seqs):
        return "in topic, not applied (lost in processor)"
    if set(inbox_seqs) - ledger_seqs:
        return "applied but not in ledger (ledger under-count)"
    return "sources agree for this device"


def chain_breaks(rows):
    """rows [(device, from_state, to_state)] in id order. Devices whose chain does not link."""
    last, bad = {}, set()
    for d, frm, to in rows:
        if d in last and last[d] != frm:
            bad.add(d)
        last[d] = to
    return sorted(bad)


def regressions(prev, cur):
    return [(d, prev[d], v) for d, v in sorted(cur.items()) if d in prev and v < prev[d]]


def dups(ids):
    return sorted(i for i, n in Counter(ids).items() if n > 1)


def dlq_check(c, since_ms):
    reasons = Counter(r for r, _ in topic.dlq(since_ms))
    c.add("no dead-letter record with reason db_constraint", reasons.get("db_constraint", 0) == 0, f"dead-letter reasons since start: {dict(reasons)}")


def show(title, rows):
    if rows:
        print(f"    {title}: {len(rows)} total, first {min(len(rows), MAX_ROWS)}")
        for r in rows[:MAX_ROWS]:
            print(f"      {r}")


class Monitor(threading.Thread):
    """Polls every device's (epoch, seq) at 1 Hz and records any step backwards."""

    def __init__(self):
        super().__init__(daemon=True)
        self.bad, self.polls, self.errors = [], 0, 0
        self._stop_evt = threading.Event()

    def run(self):
        prev = {}
        while not self._stop_evt.wait(1.0):
            try:
                cur = {r[0]: (r[1], r[2]) for r in db_query("SELECT device_id, last_epoch, last_seq FROM device")}
            except Inconclusive:
                self.errors += 1
                continue
            self.bad += regressions(prev, cur)
            prev, self.polls = cur, self.polls + 1

    def finish(self):
        self._stop_evt.set()
        self.join(5)
        return self


class LagSampler(threading.Thread):
    """Samples total consumer-group lag every 2 s, with a lag-over-ingest-rate age estimate."""

    def __init__(self, every=2.0):
        super().__init__(daemon=True)
        self.samples, self._stop_evt, self.every = [], threading.Event(), every

    def run(self):
        while not self._stop_evt.wait(self.every):
            try:
                lag, end = topic.group_lag(with_end=True)
            except Inconclusive:
                continue
            self.samples.append((time.monotonic(), sum(lag.values()), end, max(lag.values(), default=0)))

    def finish(self):
        self._stop_evt.set()
        self.join(10)
        return self

    def pct(self, q):
        v = sorted(s[1] for s in self.samples)
        return v[int(q * (len(v) - 1))] if v else None

    def age_p99(self):
        """lag / ingest rate over a trailing 5 samples; a proxy, not a measured record age."""
        s, ages = self.samples, []
        for i in range(5, len(s)):
            rate = (s[i][2] - s[i - 5][2]) / max(s[i][0] - s[i - 5][0], 1e-9)
            if rate > 0:
                ages.append(s[i][1] / rate)
        ages.sort()
        return ages[int(0.99 * (len(ages) - 1))] if ages else None


def quiesce(t_cut=None):
    """Chaos off, wait for the simulator to drain, pause generation, final ledger, wait for lag 0 and no pending webhook.

    t_cut = database time before the pause; rows created later (offline sweeps of the paused fleet) are not compared.
    """
    sim("PUT", "/chaos", {})
    try:
        sink("PUT", "/mode", {})
    except Inconclusive as e:
        print(f"  note  sink mode not reset: {e}")

    def drained():
        k = sim("GET", "/counters")
        return k["bufferedEvents"] == 0 and k["disconnectedDevices"] == 0

    ok, _ = poll(drained, 60)
    if not ok:
        raise Inconclusive("simulator still buffering or disconnected after 60 s")
    t_cut = t_cut or db_now()
    sim("PUT", "/generation", {"paused": True})
    ledger, counters = sim("GET", "/ledger"), sim("GET", "/counters")

    def lag_zero():
        try:
            return sum(topic.group_lag().values()) == 0
        except Inconclusive:
            return False

    ok, took = poll(lag_zero, QUIESCE_S)
    if not ok:
        raise Inconclusive(f"consumer lag not 0 after {QUIESCE_S} s (is the processor running?)")
    ok, took2 = poll(lambda: db_query("SELECT count(*) FROM outbox WHERE status = 'PENDING' AND created_at <= %s", (t_cut,))[0][0] == 0, QUIESCE_S)
    if not ok:
        raise Inconclusive(f"webhooks still pending {QUIESCE_S} s after lag 0 (is the sink up and its mode clear?)")
    return ledger, counters, took + took2, t_cut


def reconcile(c: Checks, since_ms, tamper=None, mon=None, base=None, t_cut=None, allow_dead=False):
    """Quiesce, compare, resume generation. since_ms = scenario start, bounds the dead-letter read."""
    try:
        ledger, counters, took, t_cut = quiesce(t_cut)
        c.note(f"quiesced: lag 0 and no pending webhook after {took:.0f}s of waiting; cut at {t_cut:%H:%M:%S}")
        return compare(c, ledger, counters, since_ms, tamper, mon, base, t_cut, allow_dead)
    finally:
        try:
            sim("PUT", "/generation", {"paused": False})
        except Inconclusive as e:
            print(f"  note  could not resume generation: {e}")


def compare(c, ledger, counters, since_ms, tamper=None, mon=None, base=None, t_cut=None, allow_dead=False):
    """base = ledger devices at scenario start; the topic is then compared over the window only."""
    epoch, seed = ledger["epoch"], ledger["seed"]
    L = {d["deviceId"]: (d["acceptedCount"], d["maxAcceptedSeq"]) for d in ledger["devices"]}
    c.add("ledger total equals the sum of its devices", ledger["totalAccepted"] == sum(a for a, _ in L.values()), f"total={ledger['totalAccepted']}")
    if tamper:
        m = TAMPER.match(tamper)
        if not m or m.group(1) not in L:
            raise Inconclusive(f"bad --tamper value {tamper!r}")
        a, mx = L[m.group(1)]
        L[m.group(1)] = (a + int(m.group(2)), mx)
    stats = {r[0]: r[1:] for r in db_query("SELECT device_id, applied_count, max_seq, late_count, gap_count FROM v_reconciliation WHERE epoch = %s", (epoch,))}
    D = {d: (v[0], v[1]) for d, v in stats.items()}
    bad_db = counts_mismatch(L, D)
    c.add("applied_count and max_seq equal the ledger for every device", not bad_db, f"{len(bad_db)} of {len(L)} devices differ; epoch {epoch}")
    show("device, ledger, db", bad_db)

    if base is not None:
        b = {d: (v[0], v[1]) for d, v in base.items()}
        by, nrec, conflicts = topic.events(since_ms - 5000, epoch, min_seq={d: v[1] for d, v in b.items()})
        W = {d: (v[0] - b.get(d, (0, 0))[0], v[1] if v[0] > b.get(d, (0, 0))[0] else 0) for d, v in L.items()}
    elif 0 < (time.time() * 1000 - epoch) < 15 * 60_000:
        by, nrec, conflicts = topic.events(epoch_since_ms(epoch), epoch)
        W = L
    else:
        by, nrec, conflicts, W = {}, 0, [], None
        c.note("topic comparison skipped: no scenario window and the epoch is older than 15 min")
    T = {d: (len(s), max(s, default=0)) for d, s in by.items()}
    bad_topic = counts_mismatch(W, T) if W is not None else []
    if W is not None:
        c.add("topic unique events equal the ledger for every device", not bad_topic, f"{len(bad_topic)} devices differ; {nrec} records read")
    show("device, ledger, topic", bad_topic)
    c.add("no seq carries two different event ids", not conflicts, f"{len(conflicts)}")
    for d, *_ in (bad_db + bad_topic)[:5]:
        if base is not None:
            break
        rng = sim("GET", f"/ledger/devices/{d}")["acceptedSeqRanges"]
        inbox = [r[0] for r in db_query("SELECT seq FROM inbox WHERE device_id = %s AND epoch = %s", (d, epoch))]
        print(f"    diagnose {d}: {diagnose(rng, by.get(d, {}).keys(), inbox)}")

    age_min = (time.time() * 1000 - epoch) / 60_000
    if 0 < age_min < INBOX_WINDOW_S / 60:
        n = {r[0]: r[1] for r in db_query("SELECT device_id, count(*) FROM inbox WHERE epoch = %s GROUP BY 1", (epoch,))}
        bad = [(d, v[0], n.get(d, 0)) for d, v in sorted(D.items()) if n.get(d, 0) != v[0]]
        c.add("inbox rows per device equal applied_count", not bad, f"{len(bad)} differ")
        show("device, applied_count, inbox", bad)
    else:
        c.note("inbox row count skipped: epoch older than the inbox window")
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since_ms / 1000))
    ids = [r[0] for r in db_query("SELECT event_id FROM state_transition WHERE event_id IS NOT NULL AND occurred_at >= %s", (since,))]
    c.add("no event id caused two state transitions", not dups(ids), f"{len(ids)} transitions with an event id")
    rows = db_query("SELECT device_id, from_state, to_state FROM state_transition WHERE occurred_at >= %s ORDER BY device_id, id", (since,))
    c.add("state transition chains link", not chain_breaks(rows), f"{len(rows)} transitions")
    bad = db_query("SELECT d.device_id FROM device d JOIN LATERAL (SELECT to_state FROM state_transition t WHERE t.device_id = d.device_id ORDER BY id DESC LIMIT 1) l ON true WHERE d.state <> l.to_state")
    c.add("device state equals its last transition", not bad, f"{len(bad)} differ")

    cur = {r[0]: (r[1], r[2]) for r in db_query("SELECT device_id, last_epoch, last_seq FROM device")}
    bad = [(d, (epoch, mx), cur.get(d)) for d, (a, mx) in sorted(L.items()) if a and cur.get(d) != (epoch, mx)]
    c.add("device (epoch, seq) ends at the ledger maximum", not bad, f"{len(bad)} differ")
    show("device, ledger, device row", bad)
    if mon:
        c.add("no device moved backwards while running", not mon.bad, f"{mon.polls} polls, {mon.errors} failed polls, {len(mon.bad)} regressions")
        show("device, was, now", mon.bad)
    bad = gap_mismatch(L, {d: (v[2], v[3]) for d, v in stats.items()})
    c.add("gap_count equals missing seqs for devices without late events", not bad, f"{len(bad)} differ")
    show("device, expected gaps, gap_count", bad)
    c.add("generated equals accepted + dropped + overflow + buffered", counter_gap(counters) == 0, f"difference {counter_gap(counters)}; {counters}")
    dlq_check(c, since_ms)
    c.add("sent equals accepted + injected duplicates", counters["sent"] == counters["accepted"] + counters["injectedDups"], f"sent={counters['sent']} accepted={counters['accepted']} dups={counters['injectedDups']}")
    out = {"db": [m[0] for m in bad_db], "topic": [m[0] for m in bad_topic]}
    if t_cut is not None:
        since_dt = datetime.fromtimestamp(since_ms / 1000, timezone.utc)
        out["window"] = m3checks.run_all(c, since_dt, t_cut, epoch, base is not None, allow_dead)
    return out


def selftest():
    """[(name, detected)] where detected is True when a deliberately broken input is caught."""
    L = {"dev-0001": (10, 12), "dev-0002": (5, 5)}
    return [
        ("missing applied row", bool(counts_mismatch(L, {"dev-0001": (10, 12)}))),
        ("extra applied row", bool(counts_mismatch(L, {**L, "dev-0001": (11, 12)}))),
        ("wrong max_seq", bool(counts_mismatch(L, {**L, "dev-0002": (5, 6)}))),
        ("device only in db", bool(counts_mismatch(L, {**L, "dev-0003": (1, 1)}))),
        ("equal inputs pass", not counts_mismatch(L, dict(L))),
        ("wrong gap count", bool(gap_mismatch(L, {"dev-0001": (0, 0), "dev-0002": (0, 0)}))),
        ("exact gap count passes", not gap_mismatch(L, {"dev-0001": (0, 2), "dev-0002": (0, 0)})),
        ("counter equation off by one", counter_gap({"generated": 10, "accepted": 6, "dropped": 2, "bufferOverflow": 0, "bufferedEvents": 1}) != 0),
        ("event id caused two transitions", dups(["a", "b", "a"]) == ["a"]),
        ("broken transition chain", chain_breaks([("d", "HEALTHY", "FAILED"), ("d", "OFFLINE", "HEALTHY")]) == ["d"]),
        ("device moved backwards", bool(regressions({"d": (1, 5)}, {"d": (1, 4)}))),
        ("alert without its acked outbox row", bool(m3checks.outbox_mismatch([(1, 2, True, False, ["opened"])]))),
        ("outbox row without the alert transition", bool(m3checks.outbox_mismatch([(1, 2, False, False, ["opened", "acked"])]))),
        ("outbox count differs from version", bool(m3checks.outbox_mismatch([(1, 3, False, True, ["opened", "resolved"])]))),
        ("consistent alert and outbox pass", not m3checks.outbox_mismatch([(1, 3, True, True, ["opened", "acked", "resolved"]), (2, 1, False, False, ["opened"])])),
        ("two open alerts for one rule", m3checks.multi_open([("d", "r", "OPEN"), ("d", "r", "ACKED"), ("d", "x", "OPEN"), ("d", "r", "RESOLVED")]) == [("d", "r")]),
        ("missing sink key", bool(_hooks([("alert-1-opened", 1, 1, "DELIVERED", True)], [])["every non-dead outbox key reached the sink"])),
        ("spurious sink key", bool(_hooks([], [_key("alert-9-opened", 9, 1)])["no sink key without an outbox row"])),
        ("dead outbox row", bool(_hooks([("alert-1-opened", 1, 1, "DEAD", True)], [])["no dead outbox rows"])),
        ("pending outbox row at the cut", bool(_hooks([("alert-1-opened", 1, 1, "PENDING", True)], [_key("alert-1-opened", 1, 1)])["no pending outbox rows at the cut"])),
        ("rejected invalid payload", bool(_hooks([], [], {"rejectedInvalid": 1})["sink saw only valid payloads"])),
        ("sink order against alert version", bool(_hooks([("alert-1-opened", 1, 1, "DELIVERED", True), ("alert-1-resolved", 1, 2, "DELIVERED", True)], [_key("alert-1-opened", 1, 1, "2"), _key("alert-1-resolved", 1, 2, "1")])["per alert, first receipt follows alert version"])),
        ("duplicate deliveries and delivered rows pass", not any(_hooks([("alert-1-opened", 1, 1, "DELIVERED", True)], [{**_key("alert-1-opened", 1, 1), "count": 3}]).values())),
        ("model: wrong transition", bool(oracle.diff("d", *oracle.model(_samples(3)), [("HEALTHY", "FAILED", "telemetry", 3)], {}))),
        ("model: missing alert", bool(oracle.diff("d", *oracle.model(_samples(3)), oracle.model(_samples(3))[0], {}))),
        ("model: extra transition", bool(oracle.diff("d", [], {}, [("HEALTHY", "OFFLINE", "offline_sweep", None)], {}))),
        ("diagnosis: lost in processor", "processor" in diagnose([(1, 3)], [1, 2, 3], [1, 3])),
        ("diagnosis: lost in kafka", "Kafka" in diagnose([(1, 3)], [1, 3], [1, 3])),
        ("diagnosis: ledger under-count", "under-count" in diagnose([(1, 2)], [1, 2, 3], [1, 2, 3])),
    ]


def _key(key, alert, version, at="1"):
    return {"key": key, "count": 1, "alertId": alert, "alertVersion": version, "firstReceivedAt": at, "lastReceivedAt": at}


def _hooks(outbox, keys, summary=None):
    return m3checks.webhook_mismatch(outbox, keys, summary or {"rejectedInvalid": 0})


def _samples(n):
    return [(i + 1, (75.0, 80.0, 0), False) for i in range(n)]
