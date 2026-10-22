"""A client that drops its event stream again and again converges to the REST state after every reconnect."""
import http.client
import json
import random
import threading
import time
from collections import Counter
from urllib.parse import urlparse

import m3
from chaos import Window
from lib import CLEANUP, URLS, Checks, Inconclusive, api, poll, sim

DROPS = 30
CONVERGE_BOUND_S = 10.0
CONVERGE_P95_S = 5.0
DEVICE_KEYS = {"deviceId", "state", "stateVersion", "stateChangedAt", "lastSeenAt"}
ALERT_KEYS = {"alertId", "deviceId", "ruleId", "status", "version", "backfilled", "openedAt", "updatedAt"}
STATES = {"HEALTHY", "DEGRADED", "FAILED", "OFFLINE", "RECOVERING"}


def apply_devices(known, items):
    for it in items:
        cur = known.get(it["deviceId"])
        if cur is None or it["stateVersion"] > cur[0]:
            known[it["deviceId"]] = (it["stateVersion"], it["state"])


def apply_alerts(known, items):
    for it in items:
        cur = known.get(it["alertId"])
        if cur is None or it["version"] > cur[0]:
            known[it["alertId"]] = (it["version"], it["status"])


def problems(name, data):
    """Shape violations of one event, checked per event name (required keys and enums)."""
    if name == "hello":
        return [] if {"serverInstance", "at"} <= data.keys() else ["hello keys"]
    if name == "devices":
        return [] if data.get("items") and all(DEVICE_KEYS <= i.keys() and i["state"] in STATES for i in data["items"]) else ["devices items"]
    if name == "alerts":
        return [] if data.get("items") and all(ALERT_KEYS <= i.keys() for i in data["items"]) else ["alerts items"]
    if name == "fleet.summary":
        return [] if {"total", "byState", "asOf"} <= data.keys() else ["summary keys"]
    if name == "resync":
        return [] if data.get("reason") in ("listener_reconnected", "overflow") else ["resync reason"]
    return [f"unknown event {name}"]


def differences(devices, alerts, rest_devices, rest_alerts):
    """Probe state against a REST snapshot: devices by (version, state), open alerts by (version, status)."""
    dev = [d for d, v in rest_devices.items() if devices.get(d) != v]
    open_probe = {a: v for a, v in alerts.items() if v[1] in ("OPEN", "ACKED")}
    al = [a for a in rest_alerts.keys() | open_probe.keys() if rest_alerts.get(a) != open_probe.get(a)]
    return dev, al


class Probe(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.devices, self.alerts, self.seen, self.errors = {}, {}, Counter(), []
        self.ready, self.conn = threading.Event(), None

    def run(self):
        u = urlparse(URLS["api"])
        try:
            self.conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=30)
            self.conn.request("GET", "/api/v1/stream", headers={"Accept": "text/event-stream"})
            r = self.conn.getresponse()
            if r.status != 200:
                self.errors.append(f"stream status {r.status}")
                return
            name, data = None, []
            while True:
                line = r.fp.readline()
                if not line:
                    return
                line = line.decode().rstrip("\r\n")
                if line.startswith("event:"):
                    name = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].strip())
                elif line == "" and name:
                    self.handle(name, json.loads("".join(data)))
                    name, data = None, []
        except (OSError, http.client.HTTPException, ValueError) as e:
            if not self.ready.is_set():
                self.errors.append(f"stream: {e!r}")

    def handle(self, name, data):
        self.seen[name] += 1
        self.errors += problems(name, data)
        if name == "hello":
            self.snapshot()
            self.ready.set()
        elif name == "devices":
            apply_devices(self.devices, data["items"])
        elif name == "alerts":
            apply_alerts(self.alerts, data["items"])
        elif name == "resync":
            self.snapshot()

    def snapshot(self):
        self.devices.clear()
        self.alerts.clear()
        apply_devices(self.devices, rest_devices_items())
        apply_alerts(self.alerts, rest_alerts_items())

    def drop(self):
        try:
            self.conn.sock.shutdown(2)
        except (OSError, AttributeError):
            pass
        try:
            self.conn.close()
        except OSError:
            pass


def rest_devices_items():
    s, b = api("GET", "/devices?size=1000")
    if s != 200:
        raise Inconclusive(f"GET /devices -> {s}")
    return b["items"]


def rest_alerts_items():
    s, b = api("GET", "/alerts?status=OPEN,ACKED&size=200")
    if s != 200:
        raise Inconclusive(f"GET /alerts -> {s}")
    return b["items"]


def rest_state():
    return {d["deviceId"]: (d["stateVersion"], d["state"]) for d in rest_devices_items()}, {a["alertId"]: (a["version"], a["status"]) for a in rest_alerts_items()}


def run(c: Checks):
    w = Window(c)
    rnd = random.Random(44)
    m3.flap_100(44)
    times, seen, errors, stuck = [], Counter(), [], []
    for i in range(DROPS):
        p = Probe()
        CLEANUP.append(p.drop)
        t0 = time.monotonic()
        p.start()
        if not p.ready.wait(CONVERGE_BOUND_S):
            stuck.append((i, "no hello and snapshot", p.errors[:2]))
            p.drop()
            continue

        def same():
            dev, al = differences(p.devices, p.alerts, *rest_state())
            return not dev and not al

        ok, _ = poll(same, CONVERGE_BOUND_S, 0.25)
        took = time.monotonic() - t0
        times.append(took)
        if not ok:
            stuck.append((i, f"not converged after {took:.1f} s", differences(p.devices, p.alerts, *rest_state())))
        time.sleep(rnd.uniform(1, 3))
        seen.update(p.seen)
        errors += p.errors
        p.drop()
        time.sleep(rnd.uniform(0.5, 2))
    if seen["devices"] == 0:
        raise Inconclusive("the probe never saw a devices event: no state changed while it was connected")
    c.add("seen events: devices, alerts and summaries arrived", seen["devices"] > 0 and seen["alerts"] > 0 and seen["fleet.summary"] > 0, f"{dict(seen)}")
    c.add("every event matched its schema shape", not errors, f"{len(errors)} violations" + (f"; first {errors[:3]}" if errors else ""))
    c.add(f"after each of {DROPS} reconnects the probe equals REST within {CONVERGE_BOUND_S:g} s", not stuck and len(times) == DROPS, f"{len(times) - len(stuck)} of {DROPS} converged" + (f"; first {stuck[:2]}" if stuck else ""))
    if times:
        t = sorted(times)
        c.note(f"time from connect to converged: p50 {m3.pctl(t, 0.5):.2f} p95 {m3.pctl(t, 0.95):.2f} max {t[-1]:.2f} s (the {CONVERGE_P95_S:g} s p95 bound is judged at the full-fleet run)")
    sim("DELETE", "/faults")
    final = Probe()
    CLEANUP.append(final.drop)
    final.start()
    final.ready.wait(CONVERGE_BOUND_S)

    def settled():
        dev, al = differences(final.devices, final.alerts, *rest_state())
        return not dev and not al and m3.calm_state()[0] == 0

    ok, _ = poll(settled, 60, 0.5)
    final.drop()
    c.add("after the faults clear a fresh probe ends on the REST state with every device HEALTHY", ok, "converged" if ok else f"{differences(final.devices, final.alerts, *rest_state())}")
    w.finish(c, None)
