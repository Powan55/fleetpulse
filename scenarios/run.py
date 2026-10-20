"""Scenario runner. Usage: run.py run <name> | reconcile [--tamper ledger:<device>:<+n|-n>] | list.

Exit codes: 0 PASS, 1 FAIL, 2 INCONCLUSIVE (bad name, environment not ready, precondition not met).
"""
import importlib
import json
import sys
from datetime import datetime, timezone

import lib
from lib import FAIL, INCONCLUSIVE, OUT_DIR, PASS, SLUG_RE, URLS, Checks, Inconclusive, machine_label

SCENARIOS = {
    "warm-start": "warm_start:run",
    "cold-reset": "cold_reset:run",
    "reconcile-selftest": "reconcile_selftest:run",
    "dedupe": "dedupe:run",
    "dedupe-all": "dedupe:run_all",
    "drop": "drop:run",
    "reorder": "reorder:run",
    "malformed": "malformed:run",
    "processor-kill": "processor_kill:run",
    "processor-kill-rapid": "processor_kill:run_rapid",
    "event-id-unique": "event_ids:run",
    "seed-repeat": "seed_repeat:run",
    "partition-balance": "partitions:run",
}


def load(name):
    mod, fn = SCENARIOS[name].split(":")
    return getattr(importlib.import_module(mod), fn)


def reconcile_only(c, tamper):
    from chaos import now_ms
    from reconcile import reconcile

    reconcile(c, now_ms(), tamper=tamper)


def execute(name, body):
    started = datetime.now(timezone.utc)
    print(f"{name}  start {started:%Y-%m-%dT%H:%M:%SZ}")
    print(f"  machine: {machine_label()}")
    print(f"  config: {json.dumps(URLS)}")
    checks = Checks()
    try:
        body(checks)
        code, verdict = (FAIL, "FAIL") if checks.failed else (PASS, "PASS")
    except Inconclusive as e:
        print(f"  INCONCLUSIVE: {e}")
        checks.rows.append({"check": "precondition", "ok": False, "detail": str(e)})
        code, verdict = INCONCLUSIVE, "INCONCLUSIVE"
    finally:
        for fn in lib.CLEANUP:
            try:
                fn()
            except Exception as e:
                print(f"  note  cleanup failed: {e}")
    print(f"{name}  {verdict}  exit {code}  ({len(checks.rows) - len(checks.failed)}/{len(checks.rows)} checks ok)")
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / f"{name}-{started:%Y%m%dT%H%M%SZ}.json").write_text(
            json.dumps({"name": name, "verdict": verdict, "started": started.isoformat(), "machine": machine_label(), "checks": checks.rows}, indent=2)
        )
    except OSError as e:
        print(f"  note  could not write result file: {e}")
    return code


def main(argv):
    args = argv[1:]
    if args == ["list"]:
        print(" ".join(sorted(SCENARIOS)))
        return PASS
    if args in ([], ["-h"], ["--help"]):
        print(__doc__)
        return PASS
    if len(args) == 2 and args[0] == "run":
        name = args[1]
        if not SLUG_RE.match(name) or name not in SCENARIOS:
            print(f"unknown scenario (known: {' '.join(sorted(SCENARIOS))})", file=sys.stderr)
            return INCONCLUSIVE
        return execute(name, load(name))
    if args[:1] == ["reconcile"] and len(args) in (1, 3):
        tamper = None
        if len(args) == 3:
            from reconcile import TAMPER

            if args[1] != "--tamper" or not TAMPER.match(args[2]):
                print("bad --tamper value", file=sys.stderr)
                return INCONCLUSIVE
            tamper = args[2]
        return execute("reconcile", lambda c: reconcile_only(c, tamper))
    print(__doc__, file=sys.stderr)
    return INCONCLUSIVE


if __name__ == "__main__":
    sys.exit(main(sys.argv))
