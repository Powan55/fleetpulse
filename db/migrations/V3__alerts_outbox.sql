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
