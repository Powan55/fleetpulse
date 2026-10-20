# FleetPulse docs

- [Architecture](architecture.md): what the system does, how an event flows, device states, alerts, live UI, API errors, observability, scope.
- Decision records (`adr/`):
  - [0001 Build layout and versions](adr/0001-build-layout-and-versions.md)
  - [0002 Inbox dedupe and offset commit](adr/0002-inbox-dedupe-and-offset-commit.md)
  - [0003 Event ordering and ids](adr/0003-event-ordering-and-ids.md)
  - [0004 Offline detection](adr/0004-offline-detection.md)
  - [0005 State machine and hysteresis](adr/0005-state-machine-and-hysteresis.md)
  - [0006 Webhook delivery via outbox](adr/0006-webhook-outbox-delivery.md)
  - [0007 Who writes alert rows](adr/0007-alert-writes.md)
  - [0008 Change notifications with LISTEN/NOTIFY](adr/0008-change-notifications.md)
  - [0009 SSE with re-snapshot](adr/0009-sse-resnapshot.md)
  - [0010 Observability stack](adr/0010-observability-stack.md)
  - [0011 Migrations](adr/0011-migrations.md)
  - [0012 Mock webhook sink](adr/0012-mock-webhook-sink.md)
  - [0013 Web proxy and port exposure](adr/0013-web-proxy-and-exposure.md)
- Interface specs live in [`../contracts/`](../contracts/): Kafka envelope, REST and SSE, webhook payload, simulator and sink control APIs, database schema, alert rules.

Status: partly built (ingest, dedupe, ordering, read API, fleet and device views, scenario runner). Alerts, webhooks, faults and dashboards are still in progress; see the status line in [architecture.md](architecture.md). Performance numbers are targets until measured.
