package dev.fleetpulse.processor;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.json.JsonMapper;

@Component
class DeviceStore {

    record Applied(Validator.Event event, long brokerTimeMs) {}

    private static final int NOTIFY_IDS = 200;
    private static final String UPSERT = """
            INSERT INTO device AS d (device_id, state, last_epoch, last_seq, last_seen_at, last_event_time)
            SELECT u.id, 'HEALTHY', u.ep, u.sq,
                   timestamptz 'epoch' + u.seen * interval '1 microsecond',
                   timestamptz 'epoch' + u.ev * interval '1 microsecond'
            FROM unnest(?::text[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[]) AS u(id, ep, sq, seen, ev)
            ORDER BY u.id
            ON CONFLICT (device_id) DO UPDATE SET
              last_epoch = EXCLUDED.last_epoch, last_seq = EXCLUDED.last_seq,
              last_seen_at = EXCLUDED.last_seen_at, last_event_time = EXCLUDED.last_event_time
            WHERE (d.last_epoch, d.last_seq) < (EXCLUDED.last_epoch, EXCLUDED.last_seq)
            RETURNING d.device_id, (xmax = 0) AS inserted
            """;

    private final JdbcTemplate jdbc;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final String channel;

    DeviceStore(JdbcTemplate jdbc, @Value("${fp.notify-channel}") String channel) {
        this.jdbc = jdbc;
        this.channel = channel;
    }

    /** One txn per batch. Keeps the newest (epoch, seq) per device; notifies for devices created in this batch. */
    @Transactional
    public int apply(List<Applied> batch) {
        Map<String, Applied> latest = new TreeMap<>();
        for (Applied a : batch) {
            latest.merge(a.event().deviceId(), a, (x, y) -> NEWER.compare(x, y) >= 0 ? x : y);
        }
        if (latest.isEmpty()) return 0;
        List<Applied> rows = new ArrayList<>(latest.values());
        List<String> created = jdbc.execute((Connection c) -> {
            try (var ps = c.prepareStatement(UPSERT)) {
                ps.setArray(1, c.createArrayOf("text", rows.stream().map(a -> a.event().deviceId()).toArray(String[]::new)));
                ps.setArray(2, longs(c, rows.stream().map(a -> a.event().epoch())));
                ps.setArray(3, longs(c, rows.stream().map(a -> a.event().seq())));
                ps.setArray(4, longs(c, rows.stream().map(a -> a.brokerTimeMs() * 1000)));
                ps.setArray(5, longs(c, rows.stream().map(a -> a.event().eventTimeMicros())));
                List<String> ids = new ArrayList<>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) if (rs.getBoolean(2)) ids.add(rs.getString(1));
                }
                return ids;
            }
        });
        notifyDevices(created);
        return rows.size();
    }

    private static final Comparator<Applied> NEWER = Comparator.<Applied>comparingLong(a -> a.event().epoch())
            .thenComparingLong(a -> a.event().seq());

    private void notifyDevices(List<String> ids) {
        for (int i = 0; i < ids.size(); i += NOTIFY_IDS) {
            String payload = mapper.writeValueAsString(
                    Map.of("t", "d", "ids", ids.subList(i, Math.min(ids.size(), i + NOTIFY_IDS))));
            jdbc.query("SELECT pg_notify(?, ?)", rs -> {}, channel, payload);
        }
    }

    private static Array longs(Connection c, java.util.stream.Stream<Long> v) throws SQLException {
        return c.createArrayOf("bigint", v.toArray(Long[]::new));
    }
}
