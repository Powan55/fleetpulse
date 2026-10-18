# 0001 Build layout and versions

## Context
Four JVM services (simulator, sink, processor, api) and a React app are built in parallel by different people. Anyone cloning the repo should only need Docker. Tests need real Kafka and Postgres.

## Options
- One Maven multi-module build with a parent pom. Every module edits the same file.
- Gradle composite build with a version catalog. Same shared-file problem, plus a second build tool.
- One independent Maven project per service. Chosen.

## Decision
- Each JVM service is its own Maven project using `spring-boot-starter-parent`. No aggregator pom, no shared library; DTOs are duplicated on purpose and each module's tests validate against the JSON files in `contracts/`.
- `mvn -B verify` runs unit tests (`*Test`) and integration tests (`*IT`, Testcontainers with real Kafka and Postgres). Builds and tests run inside the Maven image. Running Testcontainers from inside a container needs the Docker socket and, on Docker Desktop, `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal`.
- The web app is an npm project (React, TypeScript, Vite) built in a Node 24 LTS image.
- Dockerfiles live in `infra/docker/`, multi-stage, non-root at runtime.
- Pinned versions, checked against upstream release pages on 2026-10-16:

| Item | Version |
|---|---|
| Spring Boot | 4.1.1 (Spring for Apache Kafka 4.1.x, kafka-clients 4.2.x via the Boot BOM) |
| Java | 25 LTS (`maven:3.10.0-eclipse-temurin-25` to build, `eclipse-temurin:25-jre` to run) |
| Kafka | `apache/kafka:4.2.2` (KRaft, single node) |
| PostgreSQL | `postgres:18.6` |
| Flyway | `flyway/flyway:13.10.3` |
| OTel Collector | `otel/opentelemetry-collector-contrib:0.162.0` |
| Prometheus | `prom/prometheus:v3.13.4` |
| Tempo | `grafana/tempo:3.1.0` (tag still to confirm) |
| Grafana | `grafana/grafana:13.2.4` |
| Libraries (Testcontainers, pgjdbc, Micrometer, Flyway for tests) | from the Spring Boot BOM |

## Consequences
- The Boot version is repeated in four poms. A bump is four one-line edits.
- No shared code, so a contract change touches several modules; contract tests catch drift.
