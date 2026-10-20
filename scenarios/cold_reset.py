"""Reset from empty. Run right after `docker compose down -v && docker compose up -d` on the host."""
from lib import FLEET, Checks, Inconclusive, db_query, get_json, URLS, wait_flowing

MAX_AGE_S = 600


def run(c: Checks):
    start, age, lo, n, tables = db_query(
        "SELECT pg_postmaster_start_time(), extract(epoch FROM now() - pg_postmaster_start_time()), "
        "(SELECT min(created_at) FROM device), (SELECT count(*) FROM device), "
        "(SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='device')"
    )[0]
    if age > MAX_AGE_S:
        raise Inconclusive(f"postgres up {age:.0f}s; run `docker compose down -v && docker compose up -d` first, then this within {MAX_AGE_S}s")
    c.add("schema rebuilt (device table present)", tables == 1)
    if n:
        c.add("no pre-reset rows: oldest device row is newer than the postgres process", lo >= start, f"oldest={lo} postgres_start={start}")
    else:
        c.note("device table still empty (checked before first event)")
    ok, detail, took = wait_flowing(180)
    c.add("fleet rebuilt: 1000 devices live", ok, f"{detail} after {took:.0f}s")
    if ok:
        s = get_json(f"{URLS['web']}/api/v1/fleet/summary")
        c.add("summary total == 1000", s["total"] == FLEET, str(s["byState"]))
