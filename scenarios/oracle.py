"""Reference model of device states and alerts, written from the state machine table and rules.yaml only.

Pure functions over plain tuples; nothing here reads the processor or the database.
"""
import math
from bisect import bisect_right

ENTER, EXIT, RECOVER = 3, 5, 5
# metric: (op, degraded threshold, failed threshold)
SEVERITY = {"temperature_c": ("gte", 70, 85), "battery_pct": ("lte", 20, 5), "error_count": ("gte", 5, 20)}
# rule id: (metric, op, threshold)
METRIC_RULES = {"temperature_high": ("temperature_c", "gte", 70), "battery_low": ("battery_pct", "lte", 20), "error_burst": ("error_count", "gte", 5)}
METRIC_INDEX = {"temperature_c": 0, "battery_pct": 1, "error_count": 2}

OK, DEGRADED, FAILED = 0, 1, 2
ALLOWED = (
    {(s, "FAILED", "telemetry") for s in ("HEALTHY", "DEGRADED", "RECOVERING")}
    | {(s, "DEGRADED", "telemetry") for s in ("HEALTHY", "RECOVERING")}
    | {("DEGRADED", "HEALTHY", "telemetry"), ("FAILED", "RECOVERING", "telemetry"), ("RECOVERING", "HEALTHY", "telemetry"), ("OFFLINE", "RECOVERING", "event_after_offline")}
    | {(s, "OFFLINE", "offline_sweep") for s in ("HEALTHY", "DEGRADED", "FAILED", "RECOVERING")}
)


def _hit(v, op, limit):
    return v >= limit if op == "gte" else v <= limit


def severity(sample):
    """sample = (temperature_c, battery_pct, error_count). Worst over the three metrics."""
    worst = OK
    for metric, (op, deg, fail) in SEVERITY.items():
        v = sample[METRIC_INDEX[metric]]
        worst = max(worst, FAILED if _hit(v, op, fail) else DEGRADED if _hit(v, op, deg) else OK)
    return worst


def breaches(rule, sample):
    metric, op, limit = METRIC_RULES[rule]
    return _hit(sample[METRIC_INDEX[metric]], op, limit)


def model(events, sweeps=(), state="HEALTHY"):
    """events: [(seq, sample or None, replay)] in seq order, sample None for a heartbeat.

    sweeps: positions of offline sweeps, sorted; k means the sweep ran after the first k events.
    Returns (transitions, alerts). transitions: [(from, to, cause, seq or None)]. alerts: per rule id
    [(open_seq or None, resolved, backfilled)] in opening order.
    """
    trans, alerts = [], {}
    bad = failed = ok = 0
    open_ix = {}
    streak = {r: [0, 0] for r in METRIC_RULES}

    def open_alert(rule, seq, replay):
        if rule in open_ix:
            return
        alerts.setdefault(rule, []).append([seq, False, replay])
        open_ix[rule] = len(alerts[rule]) - 1

    def resolve(rule):
        if rule in open_ix:
            alerts[rule][open_ix.pop(rule)][1] = True

    def go(new, cause, seq, replay):
        nonlocal state, bad, failed, ok
        trans.append((state, new, cause, seq))
        if state == "FAILED":
            resolve("device_failed")
        if state == "OFFLINE":
            resolve("device_offline")
        state = new
        if new in ("RECOVERING", "OFFLINE"):
            bad = failed = ok = 0
        if new == "FAILED":
            open_alert("device_failed", seq, replay)
        if new == "OFFLINE":
            open_alert("device_offline", seq, False)

    def count(sev):
        nonlocal bad, failed, ok
        bad = bad + 1 if sev >= DEGRADED else 0
        failed = failed + 1 if sev == FAILED else 0
        ok = ok + 1 if sev == OK else 0

    sweeps = list(sweeps)
    si = 0
    for i in range(len(events) + 1):
        while si < len(sweeps) and sweeps[si] <= i:
            if state != "OFFLINE":
                go("OFFLINE", "offline_sweep", None, False)
            si += 1
        if i == len(events):
            break
        seq, sample, replay = events[i]
        if state == "OFFLINE":
            go("RECOVERING", "event_after_offline", seq, replay)
            if sample is not None:
                count(severity(sample))
        elif sample is not None:
            count(severity(sample))
            if state in ("HEALTHY", "DEGRADED", "RECOVERING") and failed >= ENTER:
                go("FAILED", "telemetry", seq, replay)
            elif state in ("HEALTHY", "RECOVERING") and bad >= ENTER:
                go("DEGRADED", "telemetry", seq, replay)
            elif state == "DEGRADED" and ok >= EXIT:
                go("HEALTHY", "telemetry", seq, replay)
            elif state == "FAILED" and ok >= EXIT:
                go("RECOVERING", "telemetry", seq, replay)
            elif state == "RECOVERING" and ok >= RECOVER:
                go("HEALTHY", "telemetry", seq, replay)
        if sample is not None:
            for rule in METRIC_RULES:
                s = streak[rule]
                if breaches(rule, sample):
                    s[0], s[1] = s[0] + 1, 0
                    if s[0] >= ENTER:
                        open_alert(rule, seq, replay)
                else:
                    s[0], s[1] = 0, s[1] + 1
                    if s[1] >= EXIT:
                        resolve(rule)
    return trans, {r: [tuple(a) for a in v] for r, v in alerts.items()}


def sweep_positions(received, sweep_times):
    """Number of events received at or before each sweep time. received sorted ascending."""
    return [bisect_right(received, t) for t in sweep_times]


def diff(device, exp_tr, exp_alerts, act_tr, act_alerts):
    """Human-readable differences between model output and what the database holds for one device."""
    out = []
    if exp_tr != act_tr:
        n = next((i for i, (a, b) in enumerate(zip(exp_tr, act_tr)) if a != b), min(len(exp_tr), len(act_tr)))
        out.append((device, "transitions", f"{len(exp_tr)} expected, {len(act_tr)} actual; first difference at #{n}: expected {exp_tr[n:n + 1]} actual {act_tr[n:n + 1]}"))
    for rule in sorted(exp_alerts.keys() | act_alerts.keys()):
        e, a = exp_alerts.get(rule, []), act_alerts.get(rule, [])
        if e != a:
            out.append((device, f"alerts {rule}", f"expected {e} actual {a}"))
    return out


def bounds(device, transitions, alerts, window_s):
    """Secondary guard: flap bounds for a window of window_s seconds."""
    out = []
    n22, n16 = math.ceil(window_s / 22), math.ceil(window_s / 16)
    if len(transitions) > 3 * n22:
        out.append((device, "transitions", f"{len(transitions)} > {3 * n22}"))
    for rule, lst in alerts.items():
        if rule == "device_offline":
            continue
        cap = n22 if rule == "device_failed" else n16
        if len(lst) > cap:
            out.append((device, f"alerts {rule}", f"{len(lst)} > {cap}"))
    return out


def not_allowed(transitions):
    return [t for t in transitions if (t[0], t[1], t[2]) not in ALLOWED]
