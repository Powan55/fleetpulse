"""Helpers for the alert, offline and webhook scenarios: faults, disconnects, REST reads, waiting for a calm fleet."""
import random
import threading

from lib import Inconclusive, api, db_query, poll, sim, sink

METRIC_RULE = {"temperature": "temperature_high", "battery": "battery_low", "errors": "error_burst"}
METRICS = list(METRIC_RULE)


def device_ids():
    return sorted(d["deviceId"] for d in sim("GET", "/ledger")["devices"])


def pick(ids, n, seed, exclude=()):
    pool = [d for d in ids if d not in set(exclude)]
    if len(pool) < n:
        raise Inconclusive(f"need {n} devices, have {len(pool)}")
    return sorted(random.Random(seed).sample(pool, n))


def fault(ids, metric, level=None, steps=None, duration_ms=None):
    """Constant level or a repeating pattern of (level, samples) steps on explicit devices. Returns the fault id."""
    body = {"target": {"deviceIds": list(ids)}, "metric": metric}
    if level:
        body["level"] = level
    else:
        body["pattern"] = {"steps": [{"level": lv, "samples": n} for lv, n in steps]}
    if duration_ms:
        body["durationMs"] = duration_ms
    return sim("POST", "/faults", body)["faultId"]


def disconnect(ids, duration_ms):
    return sim("POST", "/disconnects", {"target": {"deviceIds": list(ids)}, "durationMs": duration_ms})


def calm_state():
    """(devices not HEALTHY, open alerts, pending webhooks)"""
    return db_query(
        "SELECT (SELECT count(*) FROM device WHERE state <> 'HEALTHY'), (SELECT count(*) FROM alert WHERE status IN ('OPEN', 'ACKED')), (SELECT count(*) FROM outbox WHERE status = 'PENDING')"
    )[0]


def reset_controls():
    sim("DELETE", "/faults")
    sim("DELETE", "/disconnects")
    sim("PUT", "/chaos", {})
    sink("PUT", "/mode", {})


def settle(timeout=180):
    """Clear every control, wait for an all-HEALTHY fleet with no open alert and no pending webhook, reset the sink."""
    reset_controls()
    last = [None]

    def calm():
        last[0] = calm_state()
        return not any(last[0])

    ok, _ = poll(calm, timeout)
    if not ok:
        raise Inconclusive(f"fleet not calm after {timeout} s (not healthy, open alerts, pending webhooks) = {last[0]}")
    sink("DELETE", "/deliveries")


def open_alerts(device=None, rule=None):
    q = "/alerts?status=OPEN,ACKED&size=200" + (f"&deviceId={device}" if device else "") + (f"&ruleId={rule}" if rule else "")
    status, body = api("GET", q)
    if status != 200:
        raise Inconclusive(f"GET {q} -> {status}")
    return body["items"]


def get_alert(alert_id):
    return api("GET", f"/alerts/{alert_id}")


def outbox_for(alert_id):
    return [r[0] for r in db_query("SELECT transition FROM outbox WHERE alert_id = %s ORDER BY alert_version", (alert_id,))]


def pctl(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None


class Acker(threading.Thread):
    """Acks every OPEN alert it sees, once, every 2 s."""

    def __init__(self):
        super().__init__(daemon=True)
        self.done, self.errors, self._stop_evt = set(), 0, threading.Event()

    def run(self):
        while not self._stop_evt.wait(2.0):
            try:
                for a in open_alerts():
                    if a["status"] == "OPEN" and a["alertId"] not in self.done:
                        s, _ = api("POST", f"/alerts/{a['alertId']}/ack", {})
                        if s == 200:
                            self.done.add(a["alertId"])
                        elif s != 409:
                            self.errors += 1
            except Inconclusive:
                self.errors += 1

    def finish(self):
        self._stop_evt.set()
        self.join(10)
        return self


FLAP = [("DEGRADED", 3), ("OK", 5)]


def flap_100(seed=100):
    """3 bad samples, 5 good, repeating, on 100 devices. Returns their ids."""
    ids = pick(device_ids(), 100, seed)
    fault(ids, "temperature", steps=FLAP)
    return ids


def devices_in(state):
    """{deviceId: item} for one state, from the api."""
    status, body = api("GET", f"/devices?state={state}&size=1000")
    if status != 200:
        raise Inconclusive(f"GET /devices?state={state} -> {status}")
    return {d["deviceId"]: d for d in body["items"]}


def open_pairs():
    return {(a["deviceId"], a["ruleId"]) for a in open_alerts()}
