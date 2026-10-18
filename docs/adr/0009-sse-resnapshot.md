# 0009 SSE with re-snapshot

## Context
The UI needs live device and alert updates and must converge after any disconnect. Target: the UI matches the server within 5 s of reconnecting, with at most 4 update batches a second and 10 to 20 clients.

## Options
- Resume with `Last-Event-ID`. Needs a server-side event log that survives api restarts, and still needs a snapshot fallback.
- Re-fetch a snapshot on every connect and ignore stale updates by version. Chosen.

## Decision
- Event types: `hello`, `devices`, `alerts`, `fleet.summary`, `resync` (schemas in `contracts/sse-events.schema.json`). No `id:` lines, `retry: 2000`, a heartbeat comment every 15 s.
- Client: open the stream and buffer; on `hello`, fetch the snapshot (all devices, fleet summary, open alerts); apply it, then apply buffered and live messages, dropping any device or alert whose version is not newer. On `resync` or reconnect, do it again.
- Server: Spring MVC `SseEmitter`, no database connection per client, one flusher every 250 ms sending the latest state per changed entity.
- HTTP/1.1. Browsers only use HTTP/2 over TLS, and TLS is out of scope for a local demo. The 6-connections-per-origin limit is documented.
- nginx proxies the stream with buffering off.

## Consequences
- An api restart makes every client re-download the snapshot (about 150 KB each). Fine at 20 clients.
- A missed update can't survive a reconnect, because the snapshot is the source of truth.
