import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import m3checks  # noqa: E402
import reconcile  # noqa: E402

T0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=timezone.utc)
CUT = T0 + timedelta(seconds=120)
DEG = (75.0, 80.0, 0)
OKS = (30.0, 80.0, 0)


class Db:
    """Small consistent database: dev-0001 runs 3 bad samples (DEGRADED, one alert) then 5 good (HEALTHY, resolved)."""

    def __init__(self):
        self.events = [("dev-0001", s, f"00000000-0000-0000-0000-00000000000{s}", DEG if s <= 3 else OKS, False, T0 + timedelta(seconds=s)) for s in range(1, 9)]
        self.transitions = [("dev-0001", "HEALTHY", "DEGRADED", "telemetry", 3, T0 + timedelta(seconds=3)), ("dev-0001", "DEGRADED", "HEALTHY", "telemetry", 8, T0 + timedelta(seconds=8))]
        self.alerts = [("dev-0001", "temperature_high", "RESOLVED", "auto", False, "00000000-0000-0000-0000-000000000003", 1)]
        self.outbox_by_alert = [(1, 2, False, True, ["opened", "resolved"])]
        self.index = 1
        self.open_rows = []

    def query(self, sql, params=None):
        if "LEFT JOIN telemetry" in sql:
            return [(d, s, e, x[0], x[1], x[2], r, at) for d, s, e, x, r, at in self.events]
        if "FROM state_transition WHERE occurred_at >=" in sql:
            return self.transitions
        if "DISTINCT ON" in sql:
            return []
        if "SELECT device_id, rule_id, CASE WHEN resolved_at" in sql:
            return self.alerts
        if "LEFT JOIN outbox" in sql:
            return self.outbox_by_alert
        if "pg_indexes" in sql:
            return [(self.index,)]
        if "status IN ('OPEN', 'ACKED')" in sql:
            return self.open_rows
        if "trigger_event_id IS NOT NULL" in sql:
            return []
        return []


class M3(unittest.TestCase):
    def run_checks(self, db):
        m3checks.db_query = db.query
        c = lib.Checks()
        m3checks.check_alert_tables(c, T0, CUT)
        w = m3checks.load_window(1, T0, CUT)
        m3checks.check_model(c, w)
        return [r["check"] for r in c.failed]

    def test_consistent_passes(self):
        self.assertEqual(self.run_checks(Db()), [])

    def test_wrong_transition_fails(self):
        db = Db()
        db.transitions[0] = ("dev-0001", "HEALTHY", "FAILED", "telemetry", 3, T0 + timedelta(seconds=3))
        self.assertIn("transitions and alerts equal the reference model for every device", self.run_checks(db))

    def test_missing_alert_fails(self):
        db = Db()
        db.alerts = []
        self.assertIn("transitions and alerts equal the reference model for every device", self.run_checks(db))

    def test_alert_not_resolved_fails(self):
        db = Db()
        db.alerts = [("dev-0001", "temperature_high", "OPEN", None, False, "00000000-0000-0000-0000-000000000003", 1)]
        self.assertIn("transitions and alerts equal the reference model for every device", self.run_checks(db))

    def test_missing_outbox_row_fails(self):
        db = Db()
        db.outbox_by_alert = [(1, 2, False, True, ["opened"])]
        self.assertIn("outbox rows per alert match its transitions and version", self.run_checks(db))

    def test_missing_index_and_two_open_fail(self):
        db = Db()
        db.index = 0
        db.open_rows = [("d", "r", "OPEN"), ("d", "r", "ACKED")]
        f = self.run_checks(db)
        self.assertIn("index alert_one_open exists", f)
        self.assertIn("no (device, rule) has more than one open alert", f)

    def test_sweep_row_positions_after_the_last_event_seen(self):
        db = Db()
        db.events = [e for e in db.events if e[1] <= 2]
        db.transitions = [("dev-0001", "HEALTHY", "OFFLINE", "offline_sweep", None, T0 + timedelta(seconds=15))]
        db.alerts = [("dev-0001", "device_offline", "OPEN", None, False, None, 1)]
        self.assertEqual(self.run_checks(db), [])

    def test_operator_resolved_device_is_skipped(self):
        db = Db()
        db.alerts = [("dev-0001", "temperature_high", "RESOLVED", "operator", False, "00000000-0000-0000-0000-000000000003", 1)]
        db.transitions = []
        self.assertNotIn("transitions and alerts equal the reference model for every device", self.run_checks(db))

    def test_webhook_checks_pass_and_fail(self):
        keys = [{"key": "alert-1-opened", "count": 2, "alertId": 1, "alertVersion": 1, "firstReceivedAt": "1", "lastReceivedAt": "2"}]
        m3checks.sink = lambda m, p, b=None: keys if p.endswith("keys") else {"rejectedInvalid": 0}
        m3checks.db_query = lambda sql, params=None: [("alert-1-opened", 1, 1, "DELIVERED", True)]
        c = lib.Checks()
        m3checks.check_webhooks(c, T0, CUT)
        self.assertEqual(c.failed, [])
        m3checks.db_query = lambda sql, params=None: [("alert-1-opened", 1, 1, "DELIVERED", True), ("alert-2-opened", 2, 1, "DELIVERED", True)]
        c = lib.Checks()
        m3checks.check_webhooks(c, T0, CUT)
        self.assertEqual([r["check"] for r in c.failed], ["every non-dead outbox key reached the sink"])

    def test_selftest_fixtures_all_detected(self):
        self.assertEqual([n for n, ok in reconcile.selftest() if not ok], [])


if __name__ == "__main__":
    unittest.main()
