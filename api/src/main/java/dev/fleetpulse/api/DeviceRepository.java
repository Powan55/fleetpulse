package dev.fleetpulse.api;

import dev.fleetpulse.api.Dto.DevicePage;
import dev.fleetpulse.api.Dto.DeviceSummary;
import dev.fleetpulse.api.Dto.FleetSummary;
import java.sql.ResultSet;
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

    public DeviceRepository(DataSource ds) {
        this.db = JdbcClient.create(ds);
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
