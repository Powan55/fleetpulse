import os
import sys
import types
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import dock  # noqa: E402
import lib  # noqa: E402

ID = "a" * 64


class DockTests(unittest.TestCase):
    def fake(self, stdout="", rc=0):
        self.calls = []

        def run(argv, timeout=60):
            self.calls.append(argv)
            return types.SimpleNamespace(returncode=rc, stdout=stdout, stderr="")

        dock._run = run

    def test_unknown_service_and_verb_rejected_before_docker(self):
        self.fake(ID)
        for svc in ["bogus", "proxy", "web", "processor; id", "", "../processor"]:
            with self.assertRaises(ValueError):
                dock.container(svc)
        for verb in ["rm", "exec", "run", "create", "kill; id", ""]:
            with self.assertRaises(ValueError):
                dock.control("processor", verb)
        self.assertEqual(self.calls, [])

    def test_lookup_filters_by_project_and_service_label(self):
        self.fake(ID)
        self.assertEqual(dock.container("processor"), ID)
        argv = self.calls[0]
        self.assertIn(f"label=com.docker.compose.project={dock.PROJECT}", argv)
        self.assertIn("label=com.docker.compose.service=processor", argv)

    def test_zero_or_many_matches_refused(self):
        for out in ["", ID + "\n" + "b" * 64, "not-an-id"]:
            self.fake(out)
            with self.assertRaises(lib.Inconclusive):
                dock.container("kafka")

    def test_kill_argv_is_a_fixed_list(self):
        self.fake(ID)
        dock.control("processor", "kill")
        self.assertEqual(self.calls[-1], ["docker", "kill", "-s", "KILL", ID])


if __name__ == "__main__":
    unittest.main()
