import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import s02  # noqa: E402

NOW = datetime.now(timezone.utc)


def row(age, oldest, n=1000):
    return [(NOW - timedelta(seconds=age), age, oldest, n, 1)]


class S02(unittest.TestCase):
    def run_s02(self, rows):
        lib.db_query = lambda sql: rows
        s02.db_query = lib.db_query
        s02.wait_flowing = lambda t: (True, "stub", 0)
        s02.get_json = lambda url: {"total": 1000, "byState": {}}
        c = lib.Checks()
        s02.run(c)
        return c

    def test_old_rows_fail(self):
        c = self.run_s02(row(60, NOW - timedelta(hours=1)))
        self.assertEqual(len(c.failed), 1)

    def test_fresh_passes(self):
        self.assertEqual(self.run_s02(row(60, NOW - timedelta(seconds=30))).failed, [])

    def test_stale_postgres_inconclusive(self):
        with self.assertRaises(lib.Inconclusive):
            self.run_s02(row(5000, NOW))


if __name__ == "__main__":
    unittest.main()
