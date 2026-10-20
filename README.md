# FleetPulse

Real-time fleet health and alerting platform. Simulated devices stream telemetry through Kafka into Postgres, with a live dashboard for device health, failures and alerts. Duplicate events, disconnected devices and recovery are handled without losing data.

Status: planning complete, M1 slice built. The simulator, Kafka, processor, API and web dashboard run via docker compose and show 1000 live devices. Chaos controls, alert rules, webhooks and the outbox are not built yet.

## Quick start

Needs Docker with Compose v2.

```
docker compose up -d --build
```

Open http://127.0.0.1:8080 once all services report healthy (`docker compose ps`). Only the web port is published, bound to localhost.

```
docker compose down -v   # stop and wipe data
```

Demo GIF: coming.

## Docs

- [Overview and architecture](docs/architecture.md)
- [Decision records](docs/adr/)
- [Interface contracts](contracts/)
- [Docs index](docs/README.md)
