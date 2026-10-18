"""Scenario runner. Usage: run.py run <ID> | list.

Exit codes: 0 PASS, 1 FAIL, 2 INCONCLUSIVE (bad id, env not ready, precondition not met).
"""
import json
import sys
from datetime import datetime, timezone

from lib import FAIL, ID_RE, INCONCLUSIVE, OUT_DIR, PASS, URLS, Checks, Inconclusive, machine_label


def scenarios():
    import s01, s02

    return {"S-01": s01.run, "S-02": s02.run}


def main(argv):
    if len(argv) >= 2 and argv[1] == "list":
        print(" ".join(sorted(scenarios())))
        return PASS
    if len(argv) != 3 or argv[1] != "run":
        print(__doc__)
        return PASS if argv[1:] in (["-h"], ["--help"], []) else INCONCLUSIVE
    sid = argv[2]
    table = scenarios()
    if not ID_RE.match(sid) or sid not in table:
        print(f"unknown scenario id (known: {' '.join(sorted(table))})", file=sys.stderr)
        return INCONCLUSIVE

    started = datetime.now(timezone.utc)
    print(f"{sid}  start {started:%Y-%m-%dT%H:%M:%SZ}")
    print(f"  machine: {machine_label()}")
    print(f"  config: {json.dumps({k: v for k, v in URLS.items()})}  seed: n/a  epoch: n/a")
    checks = Checks()
    try:
        table[sid](checks)
        code = FAIL if checks.failed else PASS
        verdict = "FAIL" if checks.failed else "PASS"
    except Inconclusive as e:
        print(f"  INCONCLUSIVE: {e}")
        checks.rows.append({"check": "precondition", "ok": False, "detail": str(e)})
        code, verdict = INCONCLUSIVE, "INCONCLUSIVE"
    print(f"{sid}  {verdict}  exit {code}  ({len(checks.rows) - len(checks.failed)}/{len(checks.rows)} checks ok)")
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / f"{sid}-{started:%Y%m%dT%H%M%SZ}.json").write_text(
            json.dumps({"id": sid, "verdict": verdict, "started": started.isoformat(), "machine": machine_label(), "checks": checks.rows}, indent=2)
        )
    except OSError as e:
        print(f"  note  could not write result file: {e}")
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
