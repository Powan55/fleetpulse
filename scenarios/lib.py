"""Shared helpers for scenarios."""
import json
import os
import platform
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PASS, FAIL, INCONCLUSIVE = 0, 1, 2
SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{1,31}$")
OUT_DIR = Path(os.environ.get("SCENARIO_OUT", Path(__file__).parent / "out"))

URLS = {
    "web": os.environ.get("WEB_URL", "http://web:8080"),
    "api": os.environ.get("API_URL", "http://api:8080"),
    "simulator": os.environ.get("SIM_URL", "http://simulator:8080"),
    "processor": os.environ.get("PROCESSOR_URL", "http://processor:8080"),
}
# local-only default credentials of the read-only role
PG = dict(
    host=os.environ.get("PGHOST", "postgres"),
    port=int(os.environ.get("PGPORT", "5432")),
    dbname=os.environ.get("PGDATABASE", "fleetpulse"),
    user=os.environ.get("PGUSER", "fleetpulse_ro"),
    password=os.environ.get("PGPASSWORD", "fleetpulse-ro-local"),
)


class Inconclusive(Exception):
    pass


CLEANUP = []


class Checks:
    def __init__(self, label=""):
        self.rows = []
        self.label = label

    def add(self, name, ok, detail=""):
        self.rows.append({"check": name, "ok": bool(ok), "detail": str(detail)})
        print(f"  {self.label}{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
        return ok

    def note(self, text):
        print(f"  note  {text}")

    @property
    def failed(self):
        return [r for r in self.rows if not r["ok"]]


def http(method, url, body=None, timeout=10):
    """Returns (status, headers, bytes). Connection problems raise Inconclusive."""
    data = None
    headers = {}
    if method in ("POST", "PUT"):
        data = json.dumps(body if body is not None else {}).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        raise Inconclusive(f"{url} unreachable: {e}")


def sim(method, path, body=None):
    """Simulator control call. Non-2xx raises Inconclusive."""
    status, _, raw = http(method, f"{URLS['simulator']}/api/v1{path}", body)
    if not 200 <= status < 300:
        raise Inconclusive(f"simulator {method} {path} -> {status} {raw[:200]!r}")
    return json.loads(raw) if raw else None


def get_json(url):
    status, _, body = http("GET", url)
    if status != 200:
        raise Inconclusive(f"GET {url} -> {status}")
    return json.loads(body)


def parse_ts(s):
    return datetime.fromisoformat(s)


def db_query(sql, params=None):
    import psycopg  # lazy: unit tests run without it

    try:
        with psycopg.connect(**PG, connect_timeout=5) as conn:
            return conn.execute(sql, params).fetchall()
    except psycopg.OperationalError as e:
        raise Inconclusive(f"postgres unreachable as {PG['user']}: {e}")


def poll(fn, timeout, every=1.0):
    """Call fn until it returns truthy or timeout. Returns (last_value, elapsed_s)."""
    t0 = time.monotonic()
    while True:
        v = fn()
        if v or time.monotonic() - t0 >= timeout:
            return v, time.monotonic() - t0
        time.sleep(every)


def machine_label():
    cpu = "unknown cpu"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
        mem_kb = int(re.search(r"MemTotal:\s+(\d+)", Path("/proc/meminfo").read_text()).group(1))
        mem = f"{mem_kb / 1048576:.1f} GiB"
    except OSError:
        mem = "? GiB"
    label = f"{cpu}, {os.cpu_count()} logical CPUs, {mem} visible to the Docker VM, {platform.system()} {platform.release()}"
    host = os.environ.get("FP_HOST_LABEL")
    return f"{label}; host: {host}" if host else label


FLEET = 1000
FRESH_S = 10


def all_devices():
    return get_json(f"{URLS['web']}/api/v1/devices?size=1000")


def flowing(devs=None, now=None):
    """(ok, detail): 1000 distinct ids, >= 99% with lastSeenAt within 10 s."""
    devs = devs or all_devices()
    now = now or datetime.now(timezone.utc)
    items = devs["items"]
    ids = {d["deviceId"] for d in items}
    fresh = sum(1 for d in items if (now - parse_ts(d["lastSeenAt"])).total_seconds() <= FRESH_S)
    ok = len(ids) == FLEET and fresh >= FLEET * 0.99
    return ok, f"distinct={len(ids)} fresh<={FRESH_S}s={fresh}"


def wait_flowing(timeout):
    """Poll once a second. Returns (ok, detail, elapsed_s)."""
    last = [(False, "")]

    def step():
        try:
            last[0] = flowing()
        except Inconclusive as e:
            last[0] = (False, str(e))
        return last[0][0]

    _, t = poll(step, timeout)
    return last[0][0], last[0][1], t
