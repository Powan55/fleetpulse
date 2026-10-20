import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import cold_reset  # noqa: E402

NOW = datetime.now(timezone.utc)


def row(age, oldest, n=1000):
    return [(NOW - timedelta(seconds=age), age, oldest, n, 1)]


class ColdReset(unittest.TestCase):
    def run_cold_reset(self, rows):
        lib.db_query = lambda sql: rows
        cold_reset.db_query = lib.db_query
        cold_reset.wait_flowing = lambda t: (True, "stub", 0)
        cold_reset.get_json = lambda url: {"total": 1000, "byState": {}}
        c = lib.Checks()
        cold_reset.run(c)
        return c

    def test_old_rows_fail(self):
        c = self.run_cold_reset(row(60, NOW - timedelta(hours=1)))
        self.assertEqual(len(c.failed), 1)

    def test_fresh_passes(self):
        self.assertEqual(self.run_cold_reset(row(60, NOW - timedelta(seconds=30))).failed, [])

    def test_stale_postgres_inconclusive(self):
        with self.assertRaises(lib.Inconclusive):
            self.run_cold_reset(row(5000, NOW))


if __name__ == "__main__":
    unittest.main()
