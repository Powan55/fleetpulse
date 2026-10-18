# 0010 Observability stack

## Context
We want one end-to-end trace per event (simulator, Kafka, processor, webhook), metrics for ingest rate, lag, dedupes, late events, dead letters, latencies, alerts and webhooks, provisioned Grafana dashboards, and JSON logs with trace ids. At about 1000 events per second, tracing everything would swamp a laptop.

## Options
- Instrumentation: the OpenTelemetry Java agent, or Spring Boot 4's `spring-boot-starter-opentelemetry`. Chose the starter: versions come from the Boot BOM, no bytecode agent to keep in step with library versions, and Micrometer meters are easy to test. Neither gives per-record traces for a batch listener, so the agent wouldn't save that code.
- Backends: the `grafana/otel-lgtm` all-in-one image, or separate Collector, Prometheus, Tempo and Grafana. Chose separate containers: config per component in the repo, memory limits per container, and the Collector can also read Kafka consumer lag.
- Logs: no requirement needs log search in Grafana, so no Loki. JSON on stdout is enough.

## Decision
- Services export OTLP over HTTP to the Collector, every 5 s, time unit seconds. Each replica reports its container hostname as the instance id.
- Sampling: 1% of events at the simulator, parent-based everywhere else. Events from devices with an injected fault are always traced (up to 50 traces a second), so a fault can always be followed end to end. api requests are traced 100%.
- Kafka: the simulator's producer injects `traceparent` into record headers. The processor reads it per record and starts a child span for each sampled record. The outbox row stores that span's context, and the webhook delivery continues the same trace into the sink.
- Collector: OTLP receiver plus the Kafka metrics receiver for per-partition consumer lag; exports metrics to Prometheus's OTLP endpoint and traces to Tempo. Prometheus keeps 24 h, Tempo 1 h (single binary, local storage).
- Histogram buckets: 0.02, 0.05, 0.1, 0.15, 0.2, 0.25, 0.5, 1, 2, 3, 5, 10 s.
- Metric labels are small fixed sets only; device, event and alert ids never become labels.
- Logs: Spring Boot structured logging (logstash format), with `trace_id` and `span_id` from the tracing context.

## Consequences
- Four observability containers, about 1.5 GiB of memory caps in total.
- The Collector's Kafka metrics receiver is marked beta. If it misbehaves, the processor can export its own lag gauge.
- Prometheus derives its metric names from the OTLP names; dashboards should be built against the names Prometheus actually shows.
