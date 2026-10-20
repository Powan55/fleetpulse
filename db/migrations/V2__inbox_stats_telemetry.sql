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
