package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.Alert;
import dev.fleetpulse.api.Dto.AlertPage;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@Repository
public class AlertRepository {
    private static final String COLS = "alert_id, device_id, rule_id, status, version, backfilled, opened_at, acked_at,"
            + " resolved_at, resolved_by, updated_at, trigger_event_id, condition_started_event_time";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final JdbcClient db;
    private final String channel;

    public AlertRepository(DataSource ds, @Value("${fp.notify-channel}") String channel) {
        this.db = JdbcClient.create(ds);
        this.channel = channel;
    }

    public AlertPage list(List<String> statuses, String deviceId, String ruleId, int page, int size) {
        List<String> where = new ArrayList<>();
        if (!statuses.isEmpty()) where.add("status IN (:statuses)");
        if (deviceId != null) where.add("device_id = :deviceId");
        if (ruleId != null) where.add("rule_id = :ruleId");
        String w = where.isEmpty() ? "" : " WHERE " + String.join(" AND ", where);
        var count = db.sql("SELECT count(*) FROM alert" + w);
        var rows = db.sql("SELECT " + COLS + " FROM alert" + w + " ORDER BY alert_id DESC LIMIT :limit OFFSET :offset");
        if (!statuses.isEmpty()) {
            count = count.param("statuses", statuses);
            rows = rows.param("statuses", statuses);
        }
        if (deviceId != null) {
            count = count.param("deviceId", deviceId);
            rows = rows.param("deviceId", deviceId);
        }
        if (ruleId != null) {
            count = count.param("ruleId", ruleId);
            rows = rows.param("ruleId", ruleId);
        }
        long total = count.query(Long.class).single();
        List<Alert> items = rows.param("limit", size).param("offset", (long) page * size)
                .query(AlertRepository::map).list();
        return new AlertPage(items, page, size, total);
    }

    public Optional<Alert> get(long id) {
        return db.sql("SELECT " + COLS + " FROM alert WHERE alert_id = :id").param("id", id)
                .query(AlertRepository::map).optional();
    }

    public List<Alert> findByIds(Collection<Long> ids) {
        if (ids.isEmpty()) return List.of();
        return db.sql("SELECT " + COLS + " FROM alert WHERE alert_id IN (:ids) ORDER BY alert_id")
                .param("ids", ids).query(AlertRepository::map).list();
    }

    public List<Alert> forDevice(String deviceId, int limit) {
        return db.sql("SELECT " + COLS + " FROM alert WHERE device_id = :id ORDER BY alert_id DESC LIMIT :limit")
                .param("id", deviceId).param("limit", limit).query(AlertRepository::map).list();
    }

    @Transactional
    public Alert ack(long id) {
        Optional<Alert> done = db.sql("UPDATE alert SET status = 'ACKED', acked_at = now(), version = version + 1,"
                        + " updated_at = now() WHERE alert_id = :id AND status = 'OPEN' RETURNING " + COLS)
                .param("id", id).query(AlertRepository::map).optional();
        if (done.isPresent()) return committed(done.get(), "acked");
        Alert cur = get(id).orElseThrow(() -> ApiException.notFound("Alert not found"));
        if (cur.status().equals("RESOLVED")) throw ApiException.resolved(cur);
        return cur;
    }

    @Transactional
    public Alert resolve(long id) {
        Optional<Alert> done = db.sql("UPDATE alert SET status = 'RESOLVED', resolved_at = now(), resolved_by = 'operator',"
                        + " version = version + 1, updated_at = now() WHERE alert_id = :id AND status IN ('OPEN','ACKED')"
                        + " RETURNING " + COLS)
                .param("id", id).query(AlertRepository::map).optional();
        if (done.isPresent()) return committed(done.get(), "resolved");
        Alert cur = get(id).orElseThrow(() -> ApiException.notFound("Alert not found"));
        throw ApiException.resolved(cur);
    }

    private Alert committed(Alert a, String transition) {
        String key = "alert-" + a.alertId() + "-" + transition;
        db.sql("INSERT INTO outbox (alert_id, alert_version, transition, idempotency_key, payload)"
                        + " VALUES (:alert, :version, :transition, :key, CAST(:payload AS jsonb))")
                .param("alert", a.alertId()).param("version", a.version()).param("transition", transition)
                .param("key", key).param("payload", payload(a, transition, key)).update();
        ObjectNode n = JSON.createObjectNode().put("t", "a");
        n.putArray("ids").add(a.alertId());
        db.sql("SELECT pg_notify(:ch, :payload)").param("ch", channel).param("payload", n.toString()).query(rs -> {});
        return a;
    }

    private static String payload(Alert a, String transition, String key) {
        ObjectNode n = JSON.createObjectNode();
        n.put("schemaVersion", 1).put("idempotencyKey", key).put("alertId", a.alertId()).put("alertVersion", a.version())
                .put("transition", transition).put("status", a.status()).put("deviceId", a.deviceId())
                .put("ruleId", a.ruleId()).put("backfilled", a.backfilled()).put("occurredAt", a.updatedAt().toString())
                .put("openedAt", a.openedAt().toString());
        putOrNull(n, "ackedAt", a.ackedAt() == null ? null : a.ackedAt().toString());
        putOrNull(n, "resolvedAt", a.resolvedAt() == null ? null : a.resolvedAt().toString());
        putOrNull(n, "resolvedBy", a.resolvedBy());
        putOrNull(n, "triggerEventId", a.triggerEventId() == null ? null : a.triggerEventId().toString());
        putOrNull(n, "conditionStartedEventTime",
                a.conditionStartedEventTime() == null ? null : a.conditionStartedEventTime().toString());
        return n.toString();
    }

    private static void putOrNull(ObjectNode n, String k, String v) {
        if (v == null) n.putNull(k);
        else n.put(k, v);
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        OffsetDateTime t = rs.getObject(col, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    private static Alert map(ResultSet rs, int i) throws SQLException {
        return new Alert(rs.getLong("alert_id"), rs.getString("device_id"), rs.getString("rule_id"), rs.getString("status"),
                rs.getInt("version"), rs.getBoolean("backfilled"), instant(rs, "opened_at"), instant(rs, "acked_at"),
                instant(rs, "resolved_at"), rs.getString("resolved_by"), instant(rs, "updated_at"),
                rs.getObject("trigger_event_id", UUID.class), instant(rs, "condition_started_event_time"));
    }
}
