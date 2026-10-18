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
