"""Warm start: data flowing, all services ready, list, filter and paging consistent."""
from lib import FLEET, URLS, Checks, Inconclusive, db_query, get_json, http, wait_flowing

NFR_FLOW_S = 120
READY = {
    "simulator": "/actuator/health/readiness",
    "processor": "/actuator/health/readiness",
    "api": "/actuator/health/readiness",
    "web": "/healthz",
}


def ready_all(c):
    bad = []
    for name, path in READY.items():
        status, _, _ = http("GET", URLS[name] + path)
        if status != 200:
            bad.append(f"{name}={status}")
    return c.add("services ready (simulator, processor, api, web)", not bad, ", ".join(bad))


def paging(c):
    ids, prev = [], None
    for page in range(10):
        d = get_json(f"{URLS['web']}/api/v1/devices?size=100&page={page}")
        ids += [i["deviceId"] for i in d["items"]]
        prev = d["totalItems"]
    c.add("paging: 10 pages of 100 cover 1000 distinct ids, ascending", len(ids) == FLEET == len(set(ids)) and ids == sorted(ids), f"n={len(ids)} totalItems={prev}")
    tail = get_json(f"{URLS['web']}/api/v1/devices?size=100&page=10")
    c.add("paging: page 11 is empty", tail["items"] == [], f"n={len(tail['items'])}")
    again = [i["deviceId"] for i in get_json(f"{URLS['web']}/api/v1/devices?size=100&page=0")["items"]]
    c.add("paging: page 1 repeats identically", again == ids[:100])


def filtering(c, summary):
    for state, n in summary["byState"].items():
        if not n:
            continue
        d = get_json(f"{URLS['web']}/api/v1/devices?size=1000&state={state}")
        items = d["items"]
        ids = [i["deviceId"] for i in items]
        c.add(f"filter {state}: only that state, ascending, totalItems matches", all(i["state"] == state for i in items) and ids == sorted(ids) and d["totalItems"] == len(items), f"n={len(items)} summary={n}")
    status, headers, _ = http("GET", f"{URLS['web']}/api/v1/devices?state=BOGUS")
    c.add("filter: unknown state -> 400 problem+json", status == 400 and "problem+json" in (headers.get("Content-Type") or ""), f"status={status}")


def timing(c):
    start, lo, hi, n = db_query("SELECT pg_postmaster_start_time(), min(created_at), max(created_at), count(*) FROM device")[0]
    if n < FLEET:
        raise Inconclusive(f"only {n} device rows in db")
    if lo < start:
        c.note("device rows older than this postgres process: start timing not measurable (run after a fresh `up`)")
        return
    secs = (hi - start).total_seconds()
    c.add(f"data flowing within {NFR_FLOW_S}s of postgres start (proxy for `up`)", secs <= NFR_FLOW_S, f"{secs:.1f}s to the 1000th device row")


def run(c: Checks):
    ready_all(c)
    ok, detail, took = wait_flowing(240)
    c.add("data flowing: 1000 distinct ids, >= 99% seen within 10 s", ok, f"{detail} after {took:.0f}s polling")
    if not ok:
        return
    summary = get_json(f"{URLS['web']}/api/v1/fleet/summary")
    c.add("summary: byState sums to total == 1000", sum(summary["byState"].values()) == summary["total"] == FLEET, str(summary["byState"]))
    paging(c)
    filtering(c, summary)
    status, _, _ = http("GET", f"{URLS['processor']}/test/v1/partitions")
    c.add("test hooks absent in default stack (processor /test/v1/partitions -> 404)", status == 404, f"status={status}")
    timing(c)
