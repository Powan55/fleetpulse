import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import oracle  # noqa: E402

OKS = (30.0, 80.0, 0)
DEG = (75.0, 80.0, 0)
FAIL = (95.0, 80.0, 0)


def stream(*runs):
    """runs = (sample, count) pairs -> events with seq from 1, all telemetry, live."""
    out, seq = [], 0
    for sample, n in runs:
        for _ in range(n):
            seq += 1
            out.append((seq, sample, False))
    return out


def cycles(level, ok_n, bad_n, n):
    return stream(*[(level, bad_n), (OKS, ok_n)] * n)


def kinds(trans):
    return [(a, b) for a, b, _, _ in trans]


class Model(unittest.TestCase):
    def test_new_device_is_healthy_and_needs_three_bad_samples(self):
        tr, al = oracle.model(stream((DEG, 2)))
        self.assertEqual((tr, al), ([], {}))
        tr, al = oracle.model(stream((DEG, 3)))
        self.assertEqual(kinds(tr), [("HEALTHY", "DEGRADED")])
        self.assertEqual(al["temperature_high"], [(3, False, False)])

    def test_p1_alternate_nothing(self):
        self.assertEqual(oracle.model(cycles(DEG, 1, 1, 60)), ([], {}))

    def test_p2_three_bad_four_good_one_transition_no_resolve(self):
        tr, al = oracle.model(cycles(DEG, 4, 3, 15))
        self.assertEqual(kinds(tr), [("HEALTHY", "DEGRADED")])
        self.assertEqual([a[1] for a in al["temperature_high"]], [False])
        tr, al = oracle.model(cycles(FAIL, 4, 3, 15))
        self.assertEqual(kinds(tr), [("HEALTHY", "FAILED")])
        self.assertEqual((len(al["temperature_high"]), len(al["device_failed"])), (1, 1))
        self.assertFalse(al["device_failed"][0][1])

    def test_p3_degraded_seven_cycles_14_transitions_7_alerts(self):
        tr, al = oracle.model(cycles(DEG, 5, 3, 7))
        self.assertEqual(len(tr), 14)
        self.assertEqual(len(al["temperature_high"]), 7)
        self.assertTrue(all(a[1] for a in al["temperature_high"]))

    def test_p3_failed_seven_cycles_14_then_15_with_trailing_healthy(self):
        tr, al = oracle.model(cycles(FAIL, 5, 3, 7))
        self.assertEqual(len(tr), 14)
        self.assertEqual(kinds(tr)[:3], [("HEALTHY", "FAILED"), ("FAILED", "RECOVERING"), ("RECOVERING", "FAILED")])
        tr, al = oracle.model(cycles(FAIL, 5, 3, 7) + [(100 + i, OKS, False) for i in range(5)])
        self.assertEqual(len(tr), 15)
        self.assertEqual(tr[-1][:2], ("RECOVERING", "HEALTHY"))
        self.assertEqual((len(al["temperature_high"]), len(al["device_failed"])), (7, 7))
        self.assertTrue(all(a[1] for a in al["temperature_high"] + al["device_failed"]))

    def test_failed_to_healthy_takes_ten_ok_samples(self):
        tr, _ = oracle.model(stream((FAIL, 3), (OKS, 9)))
        self.assertEqual(kinds(tr), [("HEALTHY", "FAILED"), ("FAILED", "RECOVERING")])
        tr, _ = oracle.model(stream((FAIL, 3), (OKS, 10)))
        self.assertEqual(kinds(tr)[-1], ("RECOVERING", "HEALTHY"))

    def test_p4_escalation_three_transitions_per_22_s(self):
        tr, _ = oracle.model(stream((DEG, 3), (FAIL, 3), (OKS, 5), (DEG, 3), (FAIL, 3), (OKS, 5)))
        self.assertEqual(kinds(tr), [("HEALTHY", "DEGRADED"), ("DEGRADED", "FAILED"), ("FAILED", "RECOVERING"), ("RECOVERING", "DEGRADED"), ("DEGRADED", "FAILED"), ("FAILED", "RECOVERING")])
        self.assertEqual(oracle.bounds("d", tr[:3], {}, 22), [])
        self.assertTrue(oracle.bounds("d", tr[:3] * 2, {}, 22))

    def test_offline_then_heartbeat_recovers_and_five_samples_heal(self):
        ev = stream((OKS, 2)) + [(3, None, True)] + [(4 + i, OKS, True) for i in range(5)]
        tr, al = oracle.model(ev, sweeps=[2])
        self.assertEqual(kinds(tr), [("HEALTHY", "OFFLINE"), ("OFFLINE", "RECOVERING"), ("RECOVERING", "HEALTHY")])
        self.assertEqual(tr[0][3:], (None,))
        self.assertEqual(tr[1][2:], ("event_after_offline", 3))
        self.assertEqual(al["device_offline"], [(None, True, False)])

    def test_first_telemetry_after_offline_counts(self):
        ev = [(1, OKS, False)] + [(2 + i, OKS, True) for i in range(5)]
        tr, _ = oracle.model(ev, sweeps=[1])
        self.assertEqual(kinds(tr)[-1], ("RECOVERING", "HEALTHY"))
        self.assertEqual(tr[-1][3], 6)

    def test_backfilled_follows_replayed_opening_sample(self):
        ev = [(1, OKS, False)] + [(2 + i, DEG, True) for i in range(3)] + [(5 + i, OKS, True) for i in range(5)]
        _, al = oracle.model(ev)
        self.assertEqual(al["temperature_high"], [(4, True, True)])

    def test_sweep_while_failed_resolves_device_failed(self):
        _, al = oracle.model(stream((FAIL, 3)), sweeps=[3])
        self.assertEqual(al["device_failed"], [(3, True, False)])
        self.assertEqual(al["device_offline"], [(None, False, False)])

    def test_severity_bounds_inclusive(self):
        self.assertEqual(oracle.severity((70, 100, 0)), oracle.DEGRADED)
        self.assertEqual(oracle.severity((69.9, 20.1, 4)), oracle.OK)
        self.assertEqual(oracle.severity((20, 20, 0)), oracle.DEGRADED)
        self.assertEqual(oracle.severity((20, 5, 0)), oracle.FAILED)
        self.assertEqual(oracle.severity((20, 50, 20)), oracle.FAILED)

    def test_sweep_positions(self):
        self.assertEqual(oracle.sweep_positions([1, 2, 3, 10], [0, 2, 9, 11]), [0, 2, 3, 4])

    def test_diff_detects_wrong_transition_and_alert(self):
        tr, al = oracle.model(stream((DEG, 3)))
        self.assertEqual(oracle.diff("d", tr, al, tr, al), [])
        self.assertTrue(oracle.diff("d", tr, al, [("HEALTHY", "FAILED", "telemetry", 3)], al))
        self.assertTrue(oracle.diff("d", tr, al, tr, {}))
        self.assertTrue(oracle.diff("d", tr, al, tr, {"temperature_high": [(3, True, False)]}))

    def test_not_allowed(self):
        self.assertEqual(oracle.not_allowed([("HEALTHY", "OFFLINE", "offline_sweep")]), [])
        self.assertTrue(oracle.not_allowed([("OFFLINE", "HEALTHY", "telemetry")]))


class RulesFile(unittest.TestCase):
    """Constants in the oracle equal the contract file (mount ./contracts at /contracts, or run from the repo)."""

    def text(self):
        for p in (os.environ.get("FP_RULES_FILE", ""), "/contracts/rules.yaml", str(Path(__file__).resolve().parents[2] / "contracts" / "rules.yaml")):
            if p and Path(p).is_file():
                return Path(p).read_text()
        self.fail("rules.yaml not found: mount ./contracts at /contracts")

    def test_hysteresis_and_severity_and_rules_match(self):
        t = self.text()
        for name, val in (("enter_samples", oracle.ENTER), ("exit_samples", oracle.EXIT), ("recover_samples", oracle.RECOVER)):
            self.assertEqual(int(re.search(name + r":\s*(\d+)", t).group(1)), val, name)
        sev = {m[0]: (m[1], float(m[2]), float(m[4])) for m in re.findall(r"(\w+):\s*\{ degraded: \{ (gte|lte): ([\d.]+) \},\s+failed: \{ (gte|lte): ([\d.]+) \} \}", t)}
        self.assertEqual(sev, {k: (v[0], float(v[1]), float(v[2])) for k, v in oracle.SEVERITY.items()})
        rules = {m[0]: (m[1], m[2], float(m[3])) for m in re.findall(r"- id: (\w+)\s+kind: metric[^\n]*\n\s+metric: (\w+)\s+op: (\w+)[^\n]*\n\s+threshold: ([\d.]+)", t)}
        self.assertEqual(rules, {k: (v[0], v[1], float(v[2])) for k, v in oracle.METRIC_RULES.items()})


if __name__ == "__main__":
    unittest.main()
