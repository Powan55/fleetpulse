"""Alert actions over REST: ack, resolve, conflicts, reopen timing, bad input, and the outbox rows that go with them."""
import json
import time
import urllib.error
import urllib.request
from datetime import timezone

import m3
from chaos import Window
from lib import URLS, Checks, Inconclusive, api, db_query, parse_ts, poll, sim

CODES = {"VALIDATION_FAILED", "NOT_FOUND", "ALERT_RESOLVED", "UNSUPPORTED_MEDIA_TYPE", "INTERNAL"}
METRIC_REOPEN = (4.0, 7.0)
STATE_REOPEN = (6.0, 8.0)


class Calls:
    """api calls that count 5xx answers."""

    def __init__(self):
        self.server_errors, self.total = [], 0

    def __call__(self, method, path, body=None):
        s, b = api(method, path, body)
        self._count(method, path, s)
        return s, b

    def _count(self, method, path, status):
        self.total += 1
        if status >= 500:
            self.server_errors.append((method, path, status))

    def raw(self, method, path, data=None, ctype=None):
        """(status, content-type, parsed body) for a hand-built request."""
        h = {"Content-Type": ctype} if ctype else {}
        req = urllib.request.Request(f"{URLS['api']}/api/v1{path}", data=data, method=method, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                s, ct, raw = r.status, r.headers.get("Content-Type", ""), r.read()
        except urllib.error.HTTPError as e:
            s, ct, raw = e.code, e.headers.get("Content-Type", ""), e.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            raise Inconclusive(f"api unreachable: {e}")
        self._count(method, path, s)
        try:
            return s, ct, json.loads(raw) if raw else None
        except ValueError:
            return s, ct, None


def problem_ok(ct, body, status):
    """RFC 9457 shape with the contract's code and type."""
    if not (isinstance(body, dict) and "problem+json" in ct):
        return False
    code = body.get("code")
    return code in CODES and body.get("status") == status and body.get("type") == "urn:fleetpulse:problem:" + str(code).lower().replace("_", "-") and bool(body.get("title"))


def _open(call, device, rule, not_id=None):
    s, b = call("GET", f"/alerts?deviceId={device}&ruleId={rule}&status=OPEN,ACKED&size=10")
    return next((a for a in (b or {}).get("items", []) if a["alertId"] != not_id), None) if s == 200 else None


def _wait_open(call, device, rule, timeout, not_id=None):
    got, took = poll(lambda: _open(call, device, rule, not_id), timeout, 0.2)
    return got, took


def _same(rest, row):
    """REST alert vs database row (status, version, backfilled, resolved_by, timestamps to 2 ms)."""
    status, version, back, by, acked, resolved, opened, dev, rule = row
    t = lambda s: None if s is None else parse_ts(s).astimezone(timezone.utc)  # noqa: E731
    close = lambda a, b: (a is None and b is None) or (a is not None and b is not None and abs((a - b).total_seconds()) < 0.002)  # noqa: E731
    return (
        (rest["status"], rest["version"], rest["backfilled"], rest.get("resolvedBy"), rest["deviceId"], rest["ruleId"]) == (status, version, back, by, dev, rule)
        and close(t(rest.get("ackedAt")), acked)
        and close(t(rest.get("resolvedAt")), resolved)
        and close(t(rest["openedAt"]), opened)
    )


def run(c: Checks):
    w = Window(c)
    call = Calls()
    dev_a, dev_b = m3.pick(m3.device_ids(), 2, 26)
    m3.fault([dev_a], "temperature", level="DEGRADED")
    a, _ = _wait_open(call, dev_a, "temperature_high", 20)
    if not a:
        raise Inconclusive("no temperature_high alert within 20 s of a DEGRADED fault")
    aid = a["alertId"]
    s1, b1 = call("POST", f"/alerts/{aid}/ack", {})
    s2, b2 = call("POST", f"/alerts/{aid}/ack", {})
    c.add("ack twice: both 200, status ACKED, version unchanged by the second", (s1, s2) == (200, 200) and b1["status"] == b2["status"] == "ACKED" and b1["version"] == b2["version"], f"{s1}/{s2} versions {b1 and b1['version']}/{b2 and b2['version']}")
    c.add("ack twice: one acked outbox row, written with the ack", m3.outbox_for(aid) == ["opened", "acked"], f"{m3.outbox_for(aid)}")
    s, b = call("POST", f"/alerts/{aid}/resolve", {})
    t_res = time.monotonic()
    c.add("manual resolve: 200, RESOLVED by operator, one resolved outbox row", s == 200 and b["status"] == "RESOLVED" and b["resolvedBy"] == "operator" and m3.outbox_for(aid) == ["opened", "acked", "resolved"], f"{s} {b and b['status']} {m3.outbox_for(aid)}")
    new, _ = _wait_open(call, dev_a, "temperature_high", 20, not_id=aid)
    took = time.monotonic() - t_res
    lo, hi = METRIC_REOPEN
    c.add(f"metric alert re-opens within [{lo:g}, {hi:g}] s of the resolve while the fault persists", bool(new) and lo <= took <= hi, f"{took:.2f} s (0.2 s poll)")
    before = (api("GET", f"/alerts/{aid}")[1], m3.outbox_for(aid))
    for verb in ("ack", "resolve"):
        s, b = call("POST", f"/alerts/{aid}/{verb}", {})
        c.add(f"{verb} on a RESOLVED alert: 409 ALERT_RESOLVED with the alert in the body", s == 409 and b and b.get("code") == "ALERT_RESOLVED" and b.get("alert", {}).get("alertId") == aid, f"{s}")
    c.add("the 409 calls changed nothing", (api("GET", f"/alerts/{aid}")[1], m3.outbox_for(aid)) == before, "alert and outbox identical")
    row = db_query("SELECT status, version, backfilled, resolved_by, acked_at, resolved_at, opened_at, device_id, rule_id FROM alert WHERE alert_id = %s", (aid,))[0]
    c.add("REST alert fields equal the database row", _same(api("GET", f"/alerts/{aid}")[1], row), f"alert {aid}")

    m3.fault([dev_b], "temperature", level="FAILED")
    f, _ = _wait_open(call, dev_b, "device_failed", 20)
    if not f:
        raise Inconclusive("no device_failed alert within 20 s of a FAILED fault")
    s, old = call("POST", f"/alerts/{f['alertId']}/resolve", {})
    again, _ = _wait_open(call, dev_b, "device_failed", 30, not_id=f["alertId"])
    gap = (parse_ts(again["openedAt"]) - parse_ts(old["resolvedAt"])).total_seconds() if again and s == 200 else None
    lo, hi = STATE_REOPEN
    c.add(f"device_failed re-opens within [{lo:g}, {hi:g}] s of resolved_at while the state persists", gap is not None and lo <= gap <= hi, f"{gap if gap is None else round(gap, 2)} s between server timestamps")
    sim("DELETE", "/faults")

    live = f"/alerts/{aid}"
    cases = [
        ("GET", "/alerts/abc", None, None, 400),
        ("GET", "/alerts/0", None, None, 400),
        ("GET", "/alerts/9999999999", None, None, 404),
        ("GET", "/devices/bad", None, None, 400),
        ("GET", "/devices/dev-99999", None, None, 404),
        ("GET", "/alerts?status=BOGUS", None, None, 400),
        ("GET", "/alerts?size=0", None, None, 400),
        ("GET", "/alerts?size=201", None, None, 400),
        ("GET", "/alerts?page=-1", None, None, 400),
        ("GET", "/alerts?ruleId=bogus", None, None, 400),
        ("GET", "/alerts?deviceId=x", None, None, 400),
        ("GET", "/devices?size=1001", None, None, 400),
        ("POST", "/alerts/9999999999/ack", b"{}", "application/json", 404),
        ("POST", live + "/ack", b"{", "application/json", 400),
        ("POST", live + "/ack", b"", "application/x-www-form-urlencoded", 415),
        ("POST", live + "/ack", b"{}", None, 415),
        ("POST", live + "/resolve", b"{}", "text/plain", 415),
        ("POST", live + "/ack", b'{"pad":"' + b"x" * 70_000 + b'"}', "application/json", 413),
    ]
    bad = []
    for method, path, data, ctype, want in cases:
        s, ct, body = call.raw(method, path, data, ctype)
        if s != want or not problem_ok(ct, body, s):
            bad.append((method, path, s, ct, body if len(str(body)) < 200 else "..."))
    c.add("every bad input gets its 4xx as problem+json with the contract code", not bad, f"{len(cases) - len(bad)} of {len(cases)} ok")
    for b in bad[:10]:
        print(f"      {b}")
    c.add("nothing changed after the bad ack and resolve calls", (api("GET", f"/alerts/{aid}")[1], m3.outbox_for(aid)) == before, "alert and outbox identical")
    c.add("no 5xx answer in any call", not call.server_errors, f"{call.total} calls, {len(call.server_errors)} server errors")
    w.finish(c, None)
