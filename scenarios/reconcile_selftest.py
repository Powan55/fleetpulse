"""Proof that reconciliation can fail: broken inputs are caught, then a live run with a tampered ledger fails."""
import dock
from chaos import now_ms
from lib import Checks
from reconcile import reconcile, selftest

TAMPER = "ledger:dev-0001:+1"


def run(c: Checks):
    for name, ok in selftest():
        c.add(f"fixture: {name}", ok)
    for call in (lambda: dock.container("bogus"), lambda: dock.control("processor", "rm")):
        try:
            call()
            c.add("docker allowlist rejects an unknown target or verb", False)
        except ValueError as e:
            c.add("docker allowlist rejects an unknown target or verb", True, str(e))
    clean = Checks("clean run: ")
    reconcile(clean, now_ms())
    c.add("untampered ledger reconciles", not clean.failed, f"{len(clean.rows)} checks, {len(clean.failed)} failed")
    tampered = Checks("tampered run: ")
    found = reconcile(tampered, now_ms(), tamper=TAMPER)
    c.add("tampered ledger fails reconciliation", bool(tampered.failed), f"{len(tampered.failed)} checks failed")
    c.add("the failure names dev-0001 and no other device", found["db"] == ["dev-0001"] and set(found["topic"]) <= {"dev-0001"}, str(found))
