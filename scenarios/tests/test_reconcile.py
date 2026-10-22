import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import reconcile  # noqa: E402
import run  # noqa: E402

import time
from datetime import datetime, timezone

EPOCH = int(time.time() * 1000) - 120_000
DEVS = [f"dev-{i:04d}" for i in range(1, 6)]


class Stack:
    """Consistent fake: ledger, db and topic agree. Tests then break one source."""

    def __init__(self):
        self.ledger = {d: (10, 10) for d in DEVS}
        self.db = {d: (10, 10, 0, 0) for d in DEVS}
        self.topic = {d: {s: (f"e{d}{s}", {}) for s in range(1, 11)} for d in DEVS}
        self.dlq = []
        self.counters = {"generated": 50, "accepted": 50, "sent": 50, "injectedDups": 0, "dropped": 0, "bufferOverflow": 0, "bufferedEvents": 0, "disconnectedDevices": 0}

    def sim(self, method, path, body=None):
        if path == "/ledger":
            return {"seed": 42, "epoch": EPOCH, "totalAccepted": sum(a for a, _ in self.ledger.values()),
                    "devices": [{"deviceId": d, "acceptedCount": a, "maxAcceptedSeq": m} for d, (a, m) in self.ledger.items()]}
        if path == "/counters":
            return self.counters
        if path.startswith("/ledger/devices/"):
            return {"acceptedSeqRanges": [[1, self.ledger[path.rsplit("/", 1)[1]][0]]]}
        return {}

    def query(self, sql, params=None):
        if sql.startswith("SELECT now()"):
            return [(datetime.now(timezone.utc),)]
        if "FROM outbox WHERE status = 'PENDING'" in sql:
            return [(0,)]
        if "pg_indexes" in sql:
            return [(1,)]
        if "FROM alert" in sql:
            return []
        if "FROM v_reconciliation" in sql:
            return [(d,) + v for d, v in self.db.items()]
        if "FROM device" in sql and "last_epoch" in sql:
            return [(d, EPOCH, v[1]) for d, v in self.db.items()]
        if "FROM inbox" in sql:
            return [(d, v[0]) for d, v in self.db.items()]
        if "state_transition" in sql and "JOIN LATERAL" not in sql:
            return []
        if "JOIN LATERAL" in sql:
            return []
        return []

    def install(self):
        reconcile.sim = self.sim
        reconcile.sink = lambda *a, **k: {}
        reconcile.db_query = self.query
        reconcile.topic.group_lag = lambda *a, **k: {0: 0}
        reconcile.topic.dlq = lambda since=0: self.dlq
        reconcile.topic.events = lambda *a, **k: (self.topic, 0, [])
        reconcile.poll = lambda fn, timeout, every=1.0: (fn(), 0)

    def run(self, tamper=None):
        c = lib.Checks()
        reconcile.reconcile(c, 0, tamper=tamper)
        return c


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.s = Stack()
        self.s.install()

    def failed(self, c):
        return [r["check"] for r in c.failed]

    def test_consistent_passes(self):
        self.assertEqual(self.failed(self.s.run()), [])

    def test_missing_db_row_fails(self):
        del self.s.db["dev-0003"]
        self.assertIn("applied_count and max_seq equal the ledger for every device", self.failed(self.s.run()))

    def test_double_apply_fails(self):
        self.s.db["dev-0002"] = (11, 10, 0, 0)
        self.assertTrue(self.failed(self.s.run()))

    def test_topic_loss_fails(self):
        del self.s.topic["dev-0004"][7]
        self.assertIn("topic unique events equal the ledger for every device", self.failed(self.s.run()))

    def test_db_constraint_dlq_fails(self):
        self.s.dlq = [("db_constraint", "23514")]
        self.assertEqual(self.failed(self.s.run()), ["no dead-letter record with reason db_constraint"])

    def test_other_dlq_reasons_pass(self):
        self.s.dlq = [("schema", ""), ("too_large", "")]
        self.assertEqual(self.failed(self.s.run()), [])

    def test_counter_equation_fails(self):
        self.s.counters["generated"] = 51
        self.assertTrue(self.failed(self.s.run()))

    def test_tamper_fails_and_names_device(self):
        found = reconcile.compare
        out = {}

        def spy(*a, **k):
            out.update(found(*a, **k))
            return out

        reconcile.compare = spy
        try:
            c = self.s.run("ledger:dev-0001:+1")
        finally:
            reconcile.compare = found
        self.assertTrue(c.failed)
        self.assertEqual(out["db"], ["dev-0001"])

    def test_selftest_fixtures_all_detected(self):
        bad = [n for n, ok in reconcile.selftest() if not ok]
        self.assertEqual(bad, [])


class RunnerCli(unittest.TestCase):
    def test_bad_tamper_exit_2(self):
        for bad in ["ledger:dev-1:+1", "x", "ledger:dev-0001:1; id"]:
            self.assertEqual(run.main(["run.py", "reconcile", "--tamper", bad]), 2, bad)


if __name__ == "__main__":
    unittest.main()
