package dev.fleetpulse.processor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

/** Alert and outbox writes plus NOTIFY. All methods join the caller's transaction. */
@Component
class AlertStore {

    private static final int NOTIFY_IDS = 200;
    private static final String TS = "to_char(%s AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')";

    private static final String OUTBOX = """
            INSERT INTO outbox (alert_id, alert_version, transition, idempotency_key, payload)
            SELECT a.alert_id, a.version, ?::text, 'alert-' || a.alert_id || '-' || ?::text, jsonb_build_object(
              'schemaVersion', 1,
              'idempotencyKey', 'alert-' || a.alert_id || '-' || ?::text,
              'alertId', a.alert_id, 'alertVersion', a.version, 'transition', ?::text, 'status', a.status,
              'deviceId', a.device_id, 'ruleId', a.rule_id, 'backfilled', a.backfilled,
              'occurredAt', %s, 'openedAt', %s, 'ackedAt', %s, 'resolvedAt', %s, 'resolvedBy', a.resolved_by,
              'triggerEventId', a.trigger_event_id::text, 'conditionStartedEventTime', %s)
            FROM alert a WHERE a.alert_id = ?
            ON CONFLICT DO NOTHING""".formatted(TS.formatted("a.updated_at"), TS.formatted("a.opened_at"), TS.formatted("a.acked_at"),
            TS.formatted("a.resolved_at"), TS.formatted("a.condition_started_event_time"));

    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final String channel;

    AlertStore(JdbcTemplate jdbc, MeterRegistry meters, @Value("${fp.notify-channel}") String channel) {
        this.jdbc = jdbc;
        this.meters = meters;
        this.channel = channel;
    }

    /** New alert unless one is already open for (device, rule). Returns the id, or null when one was open. */
    Long open(String device, String rule, boolean backfilled, String triggerEventId, Long conditionMicros, Set<Long> touched) {
        List<Long> ids = jdbc.query("""
                INSERT INTO alert (device_id, rule_id, status, backfilled, trigger_event_id, condition_started_event_time)
                VALUES (?, ?, 'OPEN', ?, ?::uuid, CASE WHEN ?::bigint IS NULL THEN NULL ELSE timestamptz 'epoch' + ?::bigint * interval '1 microsecond' END)
                ON CONFLICT (device_id, rule_id) WHERE status IN ('OPEN','ACKED') DO NOTHING
                RETURNING alert_id""", (rs, i) -> rs.getLong(1), device, rule, backfilled, triggerEventId, conditionMicros, conditionMicros);
        if (ids.isEmpty()) return null;
        long id = ids.get(0);
        outbox(id, "opened");
        touched.add(id);
        meters.counter("fleetpulse.processor.alerts", "rule_id", rule, "transition", "opened").increment();
        return id;
    }

    /** Auto-resolve the open alert of (device, rule). False when none was open (someone resolved first). */
    boolean resolve(String device, String rule, Set<Long> touched) {
        List<Long> ids = jdbc.query("""
                UPDATE alert SET status = 'RESOLVED', resolved_at = now(), resolved_by = 'auto', version = version + 1, updated_at = now()
                WHERE device_id = ? AND rule_id = ? AND status IN ('OPEN','ACKED') RETURNING alert_id""",
                (rs, i) -> rs.getLong(1), device, rule);
        if (ids.isEmpty()) return false;
        long id = ids.get(0);
        outbox(id, "resolved");
        touched.add(id);
        meters.counter("fleetpulse.processor.alerts", "rule_id", rule, "transition", "resolved").increment();
        return true;
    }

    /** State alerts whose latest alert an operator resolved while the device is still in that state. */
    void reopenStateRules(String[] ruleIds, String[] states, java.time.Duration after, Set<Long> touched) {
        List<Long> ids = jdbc.query("""
                INSERT INTO alert (device_id, rule_id, status, backfilled)
                SELECT d.device_id, r.rule_id, 'OPEN', false
                FROM device d JOIN unnest(?::text[], ?::text[]) AS r(rule_id, state) ON r.state = d.state
                WHERE EXISTS (SELECT 1 FROM alert a WHERE a.device_id = d.device_id AND a.rule_id = r.rule_id
                              AND a.status = 'RESOLVED' AND a.resolved_by = 'operator'
                              AND a.resolved_at < now() - make_interval(secs => ?)
                              AND a.alert_id = (SELECT max(x.alert_id) FROM alert x WHERE x.device_id = d.device_id AND x.rule_id = r.rule_id))
                ON CONFLICT (device_id, rule_id) WHERE status IN ('OPEN','ACKED') DO NOTHING
                RETURNING alert_id, rule_id""",
                ps -> {
                    ps.setArray(1, ps.getConnection().createArrayOf("text", ruleIds));
                    ps.setArray(2, ps.getConnection().createArrayOf("text", states));
                    ps.setDouble(3, after.toMillis() / 1000.0);
                },
                (rs, i) -> {
                    meters.counter("fleetpulse.processor.alerts", "rule_id", rs.getString(2), "transition", "opened").increment();
                    return rs.getLong(1);
                });
        for (long id : ids) {
            outbox(id, "opened");
            touched.add(id);
        }
    }

    private void outbox(long alertId, String transition) {
        jdbc.update(OUTBOX, transition, transition, transition, transition, alertId);
    }

    Map<String, Map<String, Long>> openAlerts(String[] devices) {
        Map<String, Map<String, Long>> out = new java.util.HashMap<>();
        jdbc.query("SELECT device_id, rule_id, alert_id FROM alert WHERE device_id = ANY(?) AND status IN ('OPEN','ACKED')",
                ps -> ps.setArray(1, ps.getConnection().createArrayOf("text", devices)),
                rs -> { out.computeIfAbsent(rs.getString(1), k -> new java.util.HashMap<>()).put(rs.getString(2), rs.getLong(3)); });
        return out;
    }

    void notifyDevices(Set<String> ids) {
        notifyChange("d", List.copyOf(ids));
    }

    void notifyAlerts(Set<Long> ids) {
        notifyChange("a", List.copyOf(ids));
    }

    private void notifyChange(String type, List<?> ids) {
        for (int i = 0; i < ids.size(); i += NOTIFY_IDS) {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("t", type);
            m.put("ids", ids.subList(i, Math.min(ids.size(), i + NOTIFY_IDS)));
            String payload = mapper.writeValueAsString(m);
            jdbc.query("SELECT pg_notify(?, ?)", rs -> {}, channel, payload);
        }
    }
}
