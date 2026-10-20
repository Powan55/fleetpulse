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

## Ports

The default stack publishes only the web UI on 127.0.0.1:8080. To reach the api and simulator directly, add the debug override:

```
docker compose -f docker-compose.yml -f compose.debug.yml up -d
```

That binds the api to 127.0.0.1:8084 and the simulator to 127.0.0.1:8082 (override with `FP_DEBUG_API_PORT` and `FP_DEBUG_SIM_PORT`). Those ports skip the Host check nginx does on 8080, so use the override only on a machine you trust.

## Scenarios

The scenario runner is not part of the default stack. List and run scenarios with:

```
docker compose --profile tools run --rm scenarios list
docker compose --profile tools run --rm scenarios run <name>
```

Results are also written to `scenarios/out/`. Simulator settings such as `SIM_SEED`, `SIM_EPOCH`, `SIM_BUFFER_CAPACITY`, `SIM_HEARTBEAT_INTERVAL`, `SIM_TELEMETRY_INTERVAL` and `SIM_MAX_SEND_RATE` pass through from your shell or `.env`.

## Security notes

Local use only. There is no authentication, and the data is synthetic.

Docker socket access. Two things in this repo get the host Docker socket (`/var/run/docker.sock`): the module test command (Testcontainers starts Kafka and Postgres) and the scenario runner (`docker compose --profile tools run --rm scenarios ...`), which kills and restarts stack containers. Anything with the Docker socket can control Docker on your machine, including starting containers that mount your files. On Docker Desktop that covers the drives shared with Docker. Build plugins, test dependencies and runner scripts therefore run with that power. Mounting the socket read-only (`:ro`) does not help: it only makes the socket file read-only, and the Docker API stays fully writable.

What we do about it: plain `docker compose up` never mounts the socket; only the `scenarios` service (profile `tools`) and the test command do. The runner only kills, stops, starts, restarts, inspects and scales the stack's own containers, picked by compose labels from a fixed list, and never passes free-form input to a shell. Images and dependencies are pinned.

If you don't want that, skip the scenario runner and module tests and use `docker compose up` only, or run them in a disposable VM.

## Docs

- [Overview and architecture](docs/architecture.md)
- [Decision records](docs/adr/)
- [Interface contracts](contracts/)
- [Docs index](docs/README.md)
