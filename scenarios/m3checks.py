"""Alert, outbox and webhook checks of the reconciliation (alert/outbox consistency, one open alert, sink keys, reference model).

Pure comparison functions take plain tuples; the loaders read the database as the read-only role and the sink over HTTP.
"""
from collections import Counter, defaultdict
from datetime import timedelta

import oracle
from lib import Checks, db_query, sink

MODEL_MAX_AGE_S = 14 * 60  # telemetry is pruned after 15 min; older windows cannot be replayed


def outbox_mismatch(rows):
    """rows [(alert_id, version, acked, resolved, [transition by alert_version])].

    opened always, acked iff acked_at, resolved iff resolved_at, row count == version.
    """
    bad = []
    for aid, version, acked, resolved, got in rows:
        want = ["opened"] + (["acked"] if acked else []) + (["resolved"] if resolved else [])
        if list(got) != want or len(got) != version:
            bad.append((aid, f"version {version}, expected outbox {want}, found {list(got)}"))
    return bad


def multi_open(rows):
    """rows [(device, rule, status)] -> (device, rule) with more than one OPEN/ACKED alert."""
    n = Counter((d, r) for d, r, s in rows if s in ("OPEN", "ACKED"))
    return sorted(k for k, v in n.items() if v > 1)


def webhook_mismatch(outbox, keys, summary, allow_dead=False):
    """outbox [(key, alert_id, version, status, in_window)] created since the sink reset (in_window: created before the cut).

    keys: sink /deliveries/keys. Returns {check name: [offenders]}; empty list = ok.
    """
    S = {k["key"]: k for k in keys}
    O = {k: (aid, ver, st, win) for k, aid, ver, st, win in outbox}
    win = {k: v for k, v in O.items() if v[3]}
    res = {
        "no dead outbox rows": [] if allow_dead else sorted(k for k, v in win.items() if v[2] == "DEAD"),
        "no pending outbox rows at the cut": sorted(k for k, v in win.items() if v[2] == "PENDING"),
        "every non-dead outbox key reached the sink": sorted(k for k, v in win.items() if v[2] != "DEAD" and k not in S),
        "no sink key without an outbox row": sorted(k for k in S if k not in O),
        "sink keys without a delivered outbox row are dead rows": sorted(k for k in S if k in win and win[k][2] not in ("DELIVERED", "DEAD")),
        "sink saw only valid payloads": [] if summary.get("rejectedInvalid", 0) == 0 else [f"rejectedInvalid={summary['rejectedInvalid']}"],
        "sink alert id and version match the outbox row": sorted(k for k in S if k in O and (S[k]["alertId"], S[k]["alertVersion"]) != O[k][:2]),
    }
    per_alert = defaultdict(list)
    for k in keys:
        per_alert[k["alertId"]].append((k["firstReceivedAt"], k["alertVersion"]))
    res["per alert, first receipt follows alert version"] = sorted(a for a, v in per_alert.items() if [x[1] for x in sorted(v)] != sorted(x[1] for x in v))
    return res


def check_alert_tables(c: Checks, lo, t_cut):
    """lo = None for no lower bound. Alerts opened in [lo, t_cut]."""
    lo = lo or "-infinity"
    rows = db_query(
        "SELECT a.alert_id, a.version, a.acked_at IS NOT NULL, a.resolved_at IS NOT NULL, "
        "coalesce(array_agg(o.transition ORDER BY o.alert_version) FILTER (WHERE o.transition IS NOT NULL), '{}') "
        "FROM alert a LEFT JOIN outbox o ON o.alert_id = a.alert_id WHERE a.opened_at >= %s AND a.opened_at <= %s GROUP BY a.alert_id",
        (lo, t_cut),
    )
    bad = outbox_mismatch(rows)
    c.add("outbox rows per alert match its transitions and version", not bad, f"{len(rows)} alerts, {len(bad)} differ" + (f"; first {bad[0]}" if bad else ""))
    idx = db_query("SELECT count(*) FROM pg_indexes WHERE indexname = 'alert_one_open'")[0][0]
    c.add("index alert_one_open exists", idx == 1, f"{idx}")
    multi = multi_open(db_query("SELECT device_id, rule_id, status FROM alert WHERE status IN ('OPEN', 'ACKED')"))
    c.add("no (device, rule) has more than one open alert", not multi, f"{len(multi)} offenders" + (f"; first {multi[:3]}" if multi else ""))
    dup = db_query("SELECT trigger_event_id, rule_id FROM alert WHERE trigger_event_id IS NOT NULL AND opened_at >= %s AND opened_at <= %s GROUP BY 1, 2 HAVING count(*) > 1", (lo, t_cut))
    c.add("no event opened the same rule twice", not dup, f"{len(dup)} offenders")
    return rows


def check_webhooks(c: Checks, since, t_cut, allow_dead=False):
    keys, summary = sink("GET", "/deliveries/keys"), sink("GET", "/deliveries/summary")
    outbox = db_query("SELECT idempotency_key, alert_id, alert_version, status, created_at <= %s FROM v_outbox_keys WHERE created_at >= %s", (t_cut, since))
    res = webhook_mismatch(outbox, keys, summary, allow_dead)
    for name, bad in res.items():
        c.add(name, not bad, f"{len(bad)} offenders" + (f"; first {bad[:3]}" if bad else ""))
    print(f"    outbox rows {len(outbox)}, sink keys {len(keys)}, sink summary {summary}")
    return res


def load_window(epoch, since, t_cut):
    """Everything the model needs, keyed by device. Events are in-order inbox rows joined to telemetry."""
    ev = defaultdict(list)
    for dev, seq, eid, t, b, e, replay, rec in db_query(
        "SELECT i.device_id, i.seq, i.event_id, t.temperature_c, t.battery_pct, t.error_count, t.replay, i.received_at "
        "FROM inbox i LEFT JOIN telemetry t ON t.device_id = i.device_id AND t.epoch = i.epoch AND t.seq = i.seq "
        "WHERE i.epoch = %s AND NOT i.late AND i.received_at >= %s AND i.received_at <= %s ORDER BY i.device_id, i.seq",
        (epoch, since, t_cut),
    ):
        ev[dev].append((seq, str(eid), None if t is None else (t, b, e), bool(replay), rec))
    tr = defaultdict(list)
    for dev, frm, to, cause, seq, at in db_query(
        "SELECT device_id, from_state, to_state, cause, seq, occurred_at FROM state_transition WHERE occurred_at >= %s AND occurred_at <= %s ORDER BY device_id, id",
        (since, t_cut),
    ):
        tr[dev].append((frm, to, cause, seq, at))
    start = {d: s for d, s in db_query("SELECT DISTINCT ON (device_id) device_id, to_state FROM state_transition WHERE occurred_at < %s ORDER BY device_id, id DESC", (since,))}
    al = defaultdict(list)
    for dev, rule, status, by, back, trig, aid in db_query(
        "SELECT device_id, rule_id, CASE WHEN resolved_at > %s THEN 'OPEN' ELSE status END, resolved_by, backfilled, trigger_event_id, alert_id FROM alert WHERE opened_at >= %s AND opened_at <= %s ORDER BY alert_id",
        (t_cut, since, t_cut),
    ):
        al[dev].append((rule, status, by, back, None if trig is None else str(trig), aid))
    return {"events": ev, "transitions": tr, "start": start, "alerts": al, "window_s": (t_cut - since).total_seconds()}


def model_view(dev, w):
    """(expected, actual) as (transitions, alerts) for one device."""
    ev = w["events"].get(dev, [])
    seq_of = {e[1]: e[0] for e in ev}
    sweeps = oracle.sweep_positions([e[4] for e in ev], [t[4] for t in w["transitions"].get(dev, []) if t[2] == "offline_sweep"])
    exp = oracle.model([(e[0], e[2], e[3]) for e in ev], sweeps, w["start"].get(dev, "HEALTHY"))
    act_tr = [t[:4] for t in w["transitions"].get(dev, [])]
    act_al = defaultdict(list)
    for rule, status, by, back, trig, _ in w["alerts"].get(dev, []):
        act_al[rule].append((None if trig is None else seq_of.get(trig, f"unknown event {trig}"), status == "RESOLVED", back))
    return exp, (act_tr, dict(act_al))


def check_model(c: Checks, w):
    devs = sorted(w["events"].keys() | w["transitions"].keys() | w["alerts"].keys())
    skipped = sorted(d for d in devs if any(a[2] == "operator" for a in w["alerts"].get(d, [])))
    diffs, bad_bounds, bad_edges = [], [], []
    for d in devs:
        if d in skipped:
            continue
        (exp_tr, exp_al), (act_tr, act_al) = model_view(d, w)
        diffs += oracle.diff(d, exp_tr, exp_al, act_tr, act_al)
        bad_bounds += oracle.bounds(d, act_tr, act_al, w["window_s"])
        bad_edges += [(d, t) for t in oracle.not_allowed(act_tr)]
    n_tr = sum(len(v) for v in w["transitions"].values())
    c.add("transitions and alerts equal the reference model for every device", not diffs, f"{len(devs) - len(skipped)} devices, {n_tr} transitions, {len(diffs)} differences")
    print_rows("device, what, detail", diffs)
    c.add("every transition is an allowed (from, to, cause)", not bad_edges, f"{len(bad_edges)} offenders")
    c.add(f"flap bounds hold over {w['window_s']:.0f} s", not bad_bounds, f"{len(bad_bounds)} offenders")
    print_rows("device, what, detail", bad_bounds)
    if skipped:
        c.note(f"reference model skipped for {len(skipped)} devices with operator-resolved alerts")


def print_rows(title, rows, n=50):
    if rows:
        print(f"    {title}: {len(rows)} total, first {min(len(rows), n)}")
        for r in rows[:n]:
            print(f"      {r}")


def run_all(c: Checks, since, t_cut, epoch, windowed, allow_dead=False):
    """windowed: a scenario window with a sink reset at its start; otherwise only table-level checks run."""
    check_alert_tables(c, since if windowed else None, t_cut)
    if not windowed:
        c.note("sink and reference-model checks skipped: no scenario window")
        return None
    check_webhooks(c, since, t_cut, allow_dead)
    if (t_cut - since) > timedelta(seconds=MODEL_MAX_AGE_S):
        c.note("reference model skipped: window longer than the telemetry retention")
        return None
    w = load_window(epoch, since, t_cut)
    check_model(c, w)
    return w
