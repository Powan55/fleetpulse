import ast
import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import alert_actions  # noqa: E402
import flap  # noqa: E402
import lib  # noqa: E402
import offline  # noqa: E402
import run  # noqa: E402
import sse  # noqa: E402


class Registry(unittest.TestCase):
    def test_every_scenario_resolves_to_a_function(self):
        here = os.path.join(os.path.dirname(__file__), "..")
        for name, target in run.SCENARIOS.items():
            mod, fn = target.split(":")
            tree = ast.parse(open(os.path.join(here, mod + ".py")).read())
            self.assertIn(fn, [n.name for n in tree.body if isinstance(n, ast.FunctionDef)], name)
            self.assertRegex(name, lib.SLUG_RE.pattern)

    def test_m3_slugs_present(self):
        want = "processor-downtime replay-old-event disconnect-replay disconnect-replay-long buffer-overflow clock-skew flap-patterns fault-latency offline-timing alert-actions sink-outage sink-errors sink-slow sse-resync".split()
        self.assertEqual([w for w in want if w not in run.SCENARIOS], [])

    def test_unknown_slug_still_exit_2(self):
        self.assertEqual(run.main(["run.py", "run", "sink-outage; id"]), 2)


class OfflineJudge(unittest.TestCase):
    def test_open_gate_window(self):
        self.assertEqual(offline.judge(100.0, 111.0, []), (False, True, 13.0))
        self.assertFalse(offline.judge(100.0, 109.8, [])[1])
        self.assertFalse(offline.judge(100.0, 113.5, [])[1])

    def test_tripped_window_extends_the_deadline(self):
        tripped, ok, deadline = offline.judge(100.0, 123.5, [105.0, 112.0], every=1.0)
        self.assertEqual((tripped, ok), (True, True))
        self.assertAlmostEqual(deadline, 24.0)
        self.assertFalse(offline.judge(100.0, 125.0, [105.0, 112.0], every=1.0)[1])

    def test_never_earlier_than_9_9_even_when_tripped(self):
        self.assertFalse(offline.judge(100.0, 109.0, [101.0])[1])

    def test_trips_outside_the_window_do_not_count(self):
        self.assertEqual(offline.judge(100.0, 111.0, [50.0, 200.0])[0], False)


class Flap(unittest.TestCase):
    def test_runs_and_hand_counts(self):
        bad, ok = (75.0, 80.0, 0), (30.0, 80.0, 0)
        ev = [(i, bad if (i // 4) % 2 == 0 and i % 4 < 3 else ok, False) for i in range(1, 60)]
        self.assertEqual(flap._runs([(1, bad, False), (2, bad, False), (3, ok, False), (4, bad, False)]), [2, 1])
        self.assertEqual(flap._hand("P1 DEGRADED", "DEGRADED", [], None, None, "temperature_high"), (0, {}))
        self.assertEqual(flap._hand("P2 FAILED", "FAILED", [], None, None, "r"), (1, {"r": (1, 0), "device_failed": (1, 0)}))
        self.assertEqual(flap._hand("P3 DEGRADED", "DEGRADED", [3] * 7, None, None, "r"), (14, {"r": (7, 7)}))
        self.assertEqual(flap._hand("P3 FAILED", "FAILED", [3] * 7, None, None, "r")[0], 15)
        self.assertIsNone(flap._hand("P3 FAILED", "FAILED", [3] * 6, None, None, "r"))
        self.assertIsNone(flap._hand("P4 escalation", None, [], None, None, "r"))
        self.assertEqual(len(ev), 59)

    def test_groups_cover_200_devices(self):
        self.assertEqual(len(flap._groups()) * flap.GROUP, 200)


class Problems(unittest.TestCase):
    def test_problem_shape(self):
        good = {"type": "urn:fleetpulse:problem:alert-resolved", "title": "t", "status": 409, "code": "ALERT_RESOLVED"}
        self.assertTrue(alert_actions.problem_ok("application/problem+json", good, 409))
        self.assertFalse(alert_actions.problem_ok("application/json", good, 409))
        self.assertFalse(alert_actions.problem_ok("application/problem+json", {**good, "type": "x"}, 409))
        self.assertFalse(alert_actions.problem_ok("application/problem+json", {**good, "code": "NOPE"}, 409))
        self.assertFalse(alert_actions.problem_ok("application/problem+json", good, 400))

    def test_server_errors_counted(self):
        calls = alert_actions.Calls()
        calls._count("GET", "/x", 200)
        calls._count("GET", "/y", 503)
        self.assertEqual((calls.total, len(calls.server_errors)), (2, 1))


class SseState(unittest.TestCase):
    def test_older_versions_are_dropped(self):
        known = {}
        sse.apply_devices(known, [{"deviceId": "d", "stateVersion": 3, "state": "FAILED"}])
        sse.apply_devices(known, [{"deviceId": "d", "stateVersion": 2, "state": "HEALTHY"}])
        self.assertEqual(known["d"], (3, "FAILED"))
        a = {}
        sse.apply_alerts(a, [{"alertId": 1, "version": 2, "status": "ACKED"}])
        sse.apply_alerts(a, [{"alertId": 1, "version": 1, "status": "OPEN"}])
        self.assertEqual(a[1], (2, "ACKED"))

    def test_differences_find_stale_device_and_alert(self):
        self.assertEqual(sse.differences({"d": (1, "HEALTHY")}, {}, {"d": (1, "HEALTHY")}, {}), ([], []))
        self.assertEqual(sse.differences({"d": (1, "HEALTHY")}, {}, {"d": (2, "FAILED")}, {})[0], ["d"])
        self.assertEqual(sse.differences({}, {}, {}, {5: (1, "OPEN")})[1], [5])
        self.assertEqual(sse.differences({}, {7: (3, "RESOLVED")}, {}, {})[1], [])

    def test_schema_shape_checks(self):
        self.assertEqual(sse.problems("hello", {"serverInstance": "a", "at": "t"}), [])
        self.assertTrue(sse.problems("devices", {"items": []}))
        self.assertTrue(sse.problems("resync", {"reason": "x"}))
        self.assertTrue(sse.problems("bogus", {}))


class FakeApi(BaseHTTPRequestHandler):
    devices = [{"deviceId": "dev-0001", "state": "HEALTHY", "stateVersion": 1, "stateChangedAt": "t", "lastSeenAt": "t"}]

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path.startswith("/api/v1/stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            body = "retry: 2000\n\nevent: hello\ndata: " + json.dumps({"serverInstance": "x", "at": "t"}) + "\n\n"
            ev = {"items": [{**self.devices[0], "state": "FAILED", "stateVersion": 2}]}
            body += "event: devices\ndata: " + json.dumps(ev) + "\n\n"
            self.wfile.write(body.encode())
            self.wfile.flush()
            self.server.done.wait(5)
            return
        data = {"items": self.devices if "/devices" in self.path else []}
        raw = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class ProbeAgainstFake(unittest.TestCase):
    def test_probe_snapshots_then_applies_newer_events_and_can_be_dropped(self):
        srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeApi)
        srv.done = threading.Event()
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        old = lib.URLS["api"]
        lib.URLS["api"] = sse.URLS["api"] = f"http://127.0.0.1:{srv.server_address[1]}"
        try:
            p = sse.Probe()
            p.start()
            self.assertTrue(p.ready.wait(5))
            for _ in range(50):
                if p.devices.get("dev-0001") == (2, "FAILED"):
                    break
                threading.Event().wait(0.1)
            self.assertEqual(p.devices["dev-0001"], (2, "FAILED"))
            self.assertEqual(p.errors, [])
            p.drop()
            p.join(5)
            self.assertFalse(p.is_alive())
        finally:
            lib.URLS["api"] = sse.URLS["api"] = old
            srv.done.set()
            srv.shutdown()


if __name__ == "__main__":
    unittest.main()
