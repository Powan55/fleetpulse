import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import run  # noqa: E402


class RunnerTests(unittest.TestCase):
    def test_bad_ids_exit_2(self):
        for bad in ["S-01; rm -rf /", "S-01 && id", "../S-01", "S-99", "$(id)", "s-01", ""]:
            self.assertEqual(run.main(["run.py", "run", bad]), 2, bad)

    def test_bad_usage_exit_2(self):
        self.assertEqual(run.main(["run.py", "run"]), 2)
        self.assertEqual(run.main(["run.py", "nope", "S-01"]), 2)

    def test_flowing_can_fail(self):
        now = datetime.now(timezone.utc)

        def devs(n, age):
            ts = (now - timedelta(seconds=age)).isoformat()
            return {"items": [{"deviceId": f"dev-{i:04d}", "lastSeenAt": ts} for i in range(n)]}

        self.assertTrue(lib.flowing(devs(1000, 1), now)[0])
        self.assertFalse(lib.flowing(devs(999, 1), now)[0])
        self.assertFalse(lib.flowing(devs(1000, 30), now)[0])


if __name__ == "__main__":
    unittest.main()
