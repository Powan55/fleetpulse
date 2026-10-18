-- FleetPulse reference schema. PostgreSQL 18.
-- Spec only, never executed from here. Flyway migrations in db/migrations/ (V1__..., V2__..., ...) must produce exactly
-- this end state. Applied migrations are never edited; changes = new migration.
-- Runner: one-shot `migrate` compose service (Flyway image). No application runs migrations.
-- migrate connects as the image superuser `fleetpulse` (POSTGRES_USER), which owns all tables. Nothing else uses it.
-- Flyway placeholders (set by compose / tests): ${app_password}, ${api_password}, ${ro_password}.
-- Placeholders are interpolated raw into SQL literals: passwords must not contain single quotes (local defaults only).
-- All timestamps timestamptz, UTC. "now()" = DB txn time.

-- Roles and device state

-- Login roles, none superuser, so a SQL injection in a service can't reach superuser-only features.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fleetpulse_app') THEN
    CREATE ROLE fleetpulse_app LOGIN NOSUPERUSER PASSWORD '${app_password}'; -- processor: read/write data, no DDL
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fleetpulse_api') THEN
    CREATE ROLE fleetpulse_api LOGIN NOSUPERUSER PASSWORD '${api_password}'; -- api: read all, alert status + outbox insert
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fleetpulse_ro') THEN
    CREATE ROLE fleetpulse_ro LOGIN NOSUPERUSER PASSWORD '${ro_password}';   -- reconciliation scripts, read only
  END IF;
END $$;
GRANT USAGE ON SCHEMA public TO fleetpulse_app, fleetpulse_api, fleetpulse_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fleetpulse_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO fleetpulse_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO fleetpulse_api, fleetpulse_ro;

-- One row per device, created on first valid event. Hot row: ~1 update per device per consumer batch, ~1000/s total.
-- fillfactor 50 + no secondary indexes keeps updates HOT. 1000 rows: filters use seq scan by design.
CREATE TABLE device (
  device_id        text        PRIMARY KEY CHECK (device_id ~ '^dev-[0-9]{4,6}$'),
  state            text        NOT NULL CHECK (state IN ('HEALTHY','DEGRADED','FAILED','OFFLINE','RECOVERING')),
  state_version    bigint      NOT NULL DEFAULT 1,           -- +1 per transition; SSE/REST ordering token
  state_changed_at timestamptz NOT NULL DEFAULT now(),
  last_epoch       bigint      NOT NULL,                     -- (last_epoch, last_seq) = last applied in-order event
  last_seq         bigint      NOT NULL,
  last_seen_at     timestamptz NOT NULL,                     -- Kafka LogAppendTime of last applied in-order event (liveness)
  last_event_time  timestamptz,                              -- device clock, display only
  bad_n            smallint    NOT NULL DEFAULT 0,           -- consecutive telemetry samples with severity >= DEGRADED
  fail_n           smallint    NOT NULL DEFAULT 0,           -- consecutive samples with severity = FAILED
  ok_n             smallint    NOT NULL DEFAULT 0,           -- consecutive samples with severity = OK
  rule_streaks     jsonb       NOT NULL DEFAULT '{}'::jsonb, -- {"<rule_id>": {"t": n, "f": n, "open": alert_id|null}} metric rules only
  created_at       timestamptz NOT NULL DEFAULT now()
) WITH (fillfactor = 50);

-- Dedupe, history, reconciliation

-- Dedupe store. Insert ON CONFLICT (event_id) DO NOTHING; zero rows = duplicate. Pruned after 30 min, which is longer
-- than the worst replay/redelivery delay (~9 min).
CREATE TABLE inbox (
  event_id    uuid        PRIMARY KEY,
  device_id   text        NOT NULL,
  epoch       bigint      NOT NULL,
  seq         bigint      NOT NULL,
  late        boolean     NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX inbox_received_at_brin ON inbox USING brin (received_at);

-- Persistent per (device, epoch) counters for reconciliation; never pruned, so counts survive inbox pruning.
-- Updated in the apply txn.
CREATE TABLE device_epoch_stats (
  device_id     text        NOT NULL,
  epoch         bigint      NOT NULL,
  applied_count bigint      NOT NULL DEFAULT 0, -- newly inserted inbox rows (in-order + late) for this epoch
  max_seq       bigint      NOT NULL DEFAULT 0, -- max seq over applied events (in-order + late)
  late_count    bigint      NOT NULL DEFAULT 0,
  gap_count     bigint      NOT NULL DEFAULT 0, -- missing seq numbers detected at apply time; processing never waits
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (device_id, epoch)
) WITH (fillfactor = 50);

-- Recent telemetry, in-order samples only. Pruned by received_at (ingest time, not the skewable device clock) after 15 min.
CREATE TABLE telemetry (
  device_id     text             NOT NULL,
  epoch         bigint           NOT NULL,
  seq           bigint           NOT NULL,
  event_id      uuid             NOT NULL,
  event_time    timestamptz      NOT NULL,
  received_at   timestamptz      NOT NULL DEFAULT now(),
  temperature_c double precision NOT NULL,
  battery_pct   double precision NOT NULL,
  error_count   integer          NOT NULL,
  replay        boolean          NOT NULL,
  PRIMARY KEY (device_id, epoch, seq)
);
CREATE INDEX telemetry_received_at_brin ON telemetry USING brin (received_at);

-- Late (out-of-order) events audit; they never change current state. Pruned after 1 h.
CREATE TABLE late_event (
  event_id          uuid        PRIMARY KEY,
  device_id         text        NOT NULL,
  epoch             bigint      NOT NULL,
  seq               bigint      NOT NULL,
  type              text        NOT NULL CHECK (type IN ('heartbeat','telemetry')),
  event_time        timestamptz NOT NULL,
  received_at       timestamptz NOT NULL DEFAULT now(),
  device_last_epoch bigint      NOT NULL,
  device_last_seq   bigint      NOT NULL
);
CREATE INDEX late_event_received_at_brin ON late_event USING brin (received_at);

-- Device state history, one row per transition. Pruned after 7 d.
CREATE TABLE state_transition (
  id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id   text        NOT NULL,
  from_state  text        NOT NULL,
  to_state    text        NOT NULL,
  cause       text        NOT NULL CHECK (cause IN ('telemetry','event_after_offline','offline_sweep')),
  event_id    uuid,                    -- null for offline_sweep
  epoch       bigint,
  seq         bigint,
  event_time  timestamptz,
  replay      boolean     NOT NULL DEFAULT false,
  occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX state_transition_device_idx ON state_transition (device_id, id DESC);
CREATE INDEX state_transition_occurred_brin ON state_transition USING brin (occurred_at);

-- Reconciliation source, read via fleetpulse_ro.
CREATE VIEW v_reconciliation AS
  SELECT device_id, epoch, applied_count, max_seq, late_count, gap_count, updated_at FROM device_epoch_stats;

-- Alerts and outbox

CREATE TABLE alert (
  alert_id                     bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_id                    text        NOT NULL,
  rule_id                      text        NOT NULL CHECK (rule_id IN ('temperature_high','battery_low','error_burst','device_offline','device_failed')),
  status                       text        NOT NULL CHECK (status IN ('OPEN','ACKED','RESOLVED')),
  version                      integer     NOT NULL DEFAULT 1,
  backfilled                   boolean     NOT NULL DEFAULT false,
  opened_at                    timestamptz NOT NULL DEFAULT now(),
  acked_at                     timestamptz,
  resolved_at                  timestamptz,
  resolved_by                  text        CHECK (resolved_by IN ('auto','operator')),
  trigger_event_id             uuid,
  condition_started_event_time timestamptz,
  updated_at                   timestamptz NOT NULL DEFAULT now(),
  CHECK ((status = 'RESOLVED') = (resolved_at IS NOT NULL)),
  CHECK ((status = 'RESOLVED') = (resolved_by IS NOT NULL))
);
-- At most one open alert per (device, rule), enforced here so it holds with concurrent consumers. Insert with
--   ON CONFLICT (device_id, rule_id) WHERE status IN ('OPEN','ACKED') DO NOTHING
CREATE UNIQUE INDEX alert_one_open ON alert (device_id, rule_id) WHERE status IN ('OPEN','ACKED');
CREATE INDEX alert_device_idx ON alert (device_id, alert_id DESC);
CREATE INDEX alert_status_idx ON alert (status, alert_id DESC);

-- Transactional outbox. Written in the same txn as the alert change, by processor or api.
-- Delivered by the processor worker only. DELIVERED purged after 24 h; PENDING and DEAD never purged.
CREATE TABLE outbox (
  outbox_id        bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  alert_id         bigint      NOT NULL REFERENCES alert (alert_id),
  alert_version    integer     NOT NULL,
  transition       text        NOT NULL CHECK (transition IN ('opened','acked','resolved')),
  idempotency_key  text        NOT NULL UNIQUE,              -- 'alert-<alert_id>-<transition>'
  payload          jsonb       NOT NULL,                     -- webhook.schema.json, built at write time
  traceparent      text,                                     -- W3C context of the writing span, if sampled
  status           text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','DELIVERED','DEAD')),
  attempts         integer     NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),       -- backoff and claim lease share this column
  last_status_code integer,
  last_error       text,                                     -- truncated to 512 chars by the writer
  created_at       timestamptz NOT NULL DEFAULT now(),
  delivered_at     timestamptz,
  dead_at          timestamptz,
  UNIQUE (alert_id, alert_version)
);
CREATE INDEX outbox_pending_idx ON outbox (next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX outbox_alert_pending_idx ON outbox (alert_id, outbox_id) WHERE status = 'PENDING';
CREATE INDEX outbox_delivered_idx ON outbox (delivered_at) WHERE status = 'DELIVERED';

CREATE VIEW v_outbox_keys AS
  SELECT idempotency_key, alert_id, alert_version, transition, status, attempts, created_at, delivered_at, dead_at FROM outbox;

-- api write grants: status columns of alert only, insert into outbox only. No sequence grants.
GRANT UPDATE (status, acked_at, resolved_at, resolved_by, version, updated_at) ON alert TO fleetpulse_api;
GRANT INSERT ON outbox TO fleetpulse_api;
-- Tested on PostgreSQL 18.6: fleetpulse_api INSERT into outbox (GENERATED ALWAYS identity) works with no USAGE
-- on outbox_outbox_id_seq. Do not add a sequence grant for fleetpulse_api.
-- SELECT/DML for roles comes from the default privileges above (they apply to objects created later by `fleetpulse`).

-- Change notifications to the api, with bind parameters, in every txn that
--   (a) inserts a new device row (first event of a device, state_version 1, so new tiles appear live),
--   (b) changes device state (state_version + 1), or
--   (c) inserts or changes an alert:
--   SELECT pg_notify(?, ?)  -- ('fleetpulse_changes', '{"t":"d","ids":["dev-0001",...]}'), devices, <= 200 ids per call
--   SELECT pg_notify(?, ?)  -- ('fleetpulse_changes', '{"t":"a","ids":[17,18]}'),         alerts,  <= 200 ids per call
-- Plain telemetry that only moves last_seen_at/last_seq does not notify.
-- Payload JSON built with Jackson, < 8000 bytes (PostgreSQL default limit). Any role can NOTIFY, so the api treats the
-- payload as a hint, drops unparseable payloads or ids not matching the formats, and counts them.
