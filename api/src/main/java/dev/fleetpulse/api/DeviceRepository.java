package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.DevicePage;
import dev.fleetpulse.api.Dto.DeviceSummary;
import dev.fleetpulse.api.Dto.FleetSummary;
import dev.fleetpulse.api.Dto.DeviceDetail;
import dev.fleetpulse.api.Dto.StateTransition;
import dev.fleetpulse.api.Dto.TelemetryPoint;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class DeviceRepository {
    public static final List<String> STATES = List.of("HEALTHY", "DEGRADED", "FAILED", "OFFLINE", "RECOVERING");
    private static final String COLS = "device_id, state, state_version, state_changed_at, last_seen_at";

    private final JdbcClient db;
    private final Duration telemetryWindow;

    public DeviceRepository(DataSource ds, @Value("${fp.retention.telemetry}") Duration telemetryWindow) {
        this.db = JdbcClient.create(ds);
        this.telemetryWindow = telemetryWindow;
    }

    public FleetSummary summary() {
        Map<String, Long> by = new LinkedHashMap<>();
        STATES.forEach(s -> by.put(s, 0L));
        db.sql("SELECT state, count(*) AS n FROM device GROUP BY state")
                .query(rs -> {
                    by.put(rs.getString("state"), rs.getLong("n"));
                });
        return new FleetSummary(by.values().stream().mapToLong(Long::longValue).sum(), by, Instant.now());
    }

    public DevicePage list(String state, int page, int size) {
        String where = state == null ? "" : " WHERE state = :state";
        var count = db.sql("SELECT count(*) FROM device" + where);
        var rows = db.sql("SELECT " + COLS + " FROM device" + where + " ORDER BY device_id LIMIT :limit OFFSET :offset");
        if (state != null) {
            count = count.param("state", state);
            rows = rows.param("state", state);
        }
        long total = count.query(Long.class).single();
        List<DeviceSummary> items = rows.param("limit", size).param("offset", (long) page * size)
                .query(DeviceRepository::map).list();
        return new DevicePage(items, page, size, total);
    }

    public Optional<DeviceDetail> detail(String id, int telemetryLimit, int historyLimit) {
        return db.sql("SELECT device_id, state, state_version, state_changed_at, last_seen_at, last_event_time, last_epoch, last_seq"
                        + " FROM device WHERE device_id = :id").param("id", id)
                .query((rs, i) -> new DeviceDetail(rs.getString("device_id"), rs.getString("state"), rs.getLong("state_version"),
                        instant(rs, "state_changed_at"), instant(rs, "last_seen_at"), instant(rs, "last_event_time"),
                        rs.getLong("last_epoch"), rs.getLong("last_seq"), telemetry(id, telemetryLimit),
                        history(id, historyLimit), List.of()))
                .optional();
    }

    private List<TelemetryPoint> telemetry(String id, int limit) {
        return db.sql("SELECT epoch, seq, event_time, received_at, temperature_c, battery_pct, error_count, replay FROM telemetry"
                        + " WHERE device_id = :id AND received_at > now() - make_interval(secs => :secs)"
                        + " ORDER BY epoch DESC, seq DESC LIMIT :limit")
                .param("id", id).param("secs", (double) telemetryWindow.toSeconds()).param("limit", limit)
                .query((rs, i) -> new TelemetryPoint(rs.getLong("epoch"), rs.getLong("seq"), instant(rs, "event_time"),
                        instant(rs, "received_at"), rs.getDouble("temperature_c"), rs.getDouble("battery_pct"),
                        rs.getInt("error_count"), rs.getBoolean("replay")))
                .list();
    }

    private List<StateTransition> history(String id, int limit) {
        return db.sql("SELECT id, from_state, to_state, cause, event_id, event_time, occurred_at, replay FROM state_transition"
                        + " WHERE device_id = :id ORDER BY id DESC LIMIT :limit")
                .param("id", id).param("limit", limit)
                .query((rs, i) -> new StateTransition(rs.getLong("id"), rs.getString("from_state"), rs.getString("to_state"),
                        rs.getString("cause"), rs.getObject("event_id", UUID.class), instant(rs, "event_time"),
                        instant(rs, "occurred_at"), rs.getBoolean("replay")))
                .list();
    }

    private static Instant instant(ResultSet rs, String col) throws SQLException {
        OffsetDateTime t = rs.getObject(col, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }

    public List<DeviceSummary> findByIds(Collection<String> ids) {
        if (ids.isEmpty()) return List.of();
        return db.sql("SELECT " + COLS + " FROM device WHERE device_id IN (:ids) ORDER BY device_id")
                .param("ids", ids).query(DeviceRepository::map).list();
    }

    private static DeviceSummary map(ResultSet rs, int i) throws SQLException {
        return new DeviceSummary(rs.getString("device_id"), rs.getString("state"), rs.getLong("state_version"),
                rs.getObject("state_changed_at", OffsetDateTime.class).toInstant(),
                rs.getObject("last_seen_at", OffsetDateTime.class).toInstant());
    }
}
