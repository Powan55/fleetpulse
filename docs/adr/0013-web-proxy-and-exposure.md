# 0013 Web proxy and port exposure

## Context
The UI's chaos panel calls the simulator, and the UI calls the api. There is no auth, so exposure has to be minimal and cross-site requests from other web pages must not work. SSE through a proxy needs buffering off.

## Options
- The browser calls the api and simulator on their own ports. Needs CORS on both.
- The api proxies simulator calls. Proxy code in the api for a demo feature.
- nginx serves the UI and proxies `/api/` and `/sim/api/v1/` on the same origin. Chosen.

## Decision
- The browser only talks to `http://localhost:8080`. Same origin, so no CORS config anywhere.
- nginx:
  - `/api/v1/stream` with buffering off and a read timeout above the 15 s heartbeat.
  - `/api/` to the api, `/sim/api/v1/` to the simulator's API only (no actuator), SPA fallback to `index.html`. The processor is never proxied.
  - Accept only Host `localhost`, `127.0.0.1` or `web`; anything else gets 444. This blocks DNS rebinding.
  - Body size limit 16 KB.
  - Security headers (CSP with `frame-ancestors 'none'`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`) in one snippet with `always`, included in every location that sets its own headers. nginx drops inherited `add_header` lines in any block that defines its own, so the snippet has to be repeated.
- Every POST and PUT requires a JSON body with `Content-Type: application/json`, otherwise 415. That puts every write outside the browser's "simple request" class, so a cross-site page can't send one without a preflight, which fails. ack/resolve take `{}`.
- Published ports, all on 127.0.0.1: web 8080 and Grafana 3000. Everything else stays on the compose network.
- `compose.debug.yml` is an override file that also publishes the api (8081), simulator (8082), sink (8083) and Prometheus (9090) on 127.0.0.1. Compose profiles can't add ports to an existing service, so this has to be an override. These ports skip nginx's host check and Prometheus accepts metric writes, so only use them while debugging.
- CI checks `docker compose config` for both the base file and base plus debug: every published port is on 127.0.0.1, the base publishes only 8080 and 3000, and the processor never has ports.
- Actuator exposes only `health`. The webhook target URL is fixed configuration, not settable at runtime.

## Consequences
- Frontend dev outside Docker uses Vite's dev proxy for the same paths, still no CORS.
- Anyone with local access to the machine can drive chaos and acks. That's accepted for a local-only demo and stated in the docs.
