"""Warm start against a fake stack: proves the checks can pass and can fail."""
import json
import os
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import lib  # noqa: E402
import run  # noqa: E402

STATE = {"dup_page": False}


class Fake(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a):
        pass

    def reply(self, code, obj, ctype="application/json"):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        for i in range(0, len(body), 16384):  # one big send stalls on loopback here
            self.wfile.write(body[i : i + 16384])

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path.endswith(("readiness", "healthz")):
            return self.reply(200, {"status": "UP"})
        if u.path.startswith("/test/"):
            return self.reply(404, {})
        if u.path == "/api/v1/fleet/summary":
            return self.reply(200, {"total": 1000, "byState": {"HEALTHY": 1000, "DEGRADED": 0, "FAILED": 0, "OFFLINE": 0, "RECOVERING": 0}, "asOf": ""})
        if u.path == "/api/v1/devices":
            if q.get("state") == ["BOGUS"]:
                return self.reply(400, {}, "application/problem+json")
            size, page = int(q.get("size", [100])[0]), int(q.get("page", [0])[0])
            ts = datetime.now(timezone.utc).isoformat()
            rows = [{"deviceId": f"dev-{i:04d}", "state": "HEALTHY", "lastSeenAt": ts} for i in range(1000)]
            if STATE["dup_page"] and page == 1:
                rows[100:200] = rows[0:100]
            items = rows[page * size : (page + 1) * size]
            return self.reply(200, {"items": items, "page": page, "size": size, "totalItems": 1000})
        self.reply(404, {})


class WarmStartFake(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ThreadingHTTPServer.request_queue_size = 128
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Fake)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{cls.srv.server_port}"
        for k in lib.URLS:
            lib.URLS[k] = url
        now = datetime.now(timezone.utc)
        lib.db_query = lambda sql: [(now - timedelta(seconds=60), now - timedelta(seconds=30), now, 1000)]
        lib.OUT_DIR = __import__("pathlib").Path(os.environ.get("TMP", "/tmp")) / "fp-s01-fake"
        run.OUT_DIR = lib.OUT_DIR

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def test_pass(self):
        STATE["dup_page"] = False
        self.assertEqual(run.main(["run.py", "run", "warm-start"]), 0)

    def test_paging_overlap_fails(self):
        STATE["dup_page"] = True
        self.assertEqual(run.main(["run.py", "run", "warm-start"]), 1)


if __name__ == "__main__":
    unittest.main()
