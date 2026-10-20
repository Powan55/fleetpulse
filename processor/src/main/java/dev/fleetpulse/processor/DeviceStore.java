package dev.fleetpulse.processor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.json.JsonMapper;

@Component
class DeviceStore {

    record Applied(Validator.Event event, ConsumerRecord<byte[], byte[]> record) {
        long brokerTimeMs() { return record.timestamp(); }
    }

    record Counts(int applied, int duplicate, int late, long gaps) {}

    private record Arr(String type, Object[] values) {}

    private static final Logger log = LoggerFactory.getLogger(DeviceStore.class);
    private static final int NOTIFY_IDS = 200;
    private static final long SEQ_JUMP_WARN = 100_000;
    private static final String TS = "timestamptz 'epoch' + %s * interval '1 microsecond'";

    private static final Comparator<Validator.Event> ORDER = Comparator.<Validator.Event>comparingLong(Validator.Event::epoch)
            .thenComparingLong(Validator.Event::seq);

    private final JdbcTemplate jdbc;
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final String channel;

    DeviceStore(JdbcTemplate jdbc, @Value("${fp.notify-channel}") String channel) {
        this.jdbc = jdbc;
        this.channel = channel;
    }

    private static final class Stat {
        long applied, maxSeq, late, gaps;
    }

    /** One txn per batch: inbox dedupe, late/gap classification per device, stats, telemetry, device cursor. */
    @Transactional
    public Counts apply(List<Applied> batch) {
        Map<String, Applied> unique = new LinkedHashMap<>();
        for (Applied a : batch) unique.putIfAbsent(a.event().eventId(), a);
        if (unique.isEmpty()) return new Counts(0, 0, 0, 0);
        List<Applied> uniq = new ArrayList<>(unique.values());

        Set<String> fresh = Set.copyOf(query("""
                INSERT INTO inbox (event_id, device_id, epoch, seq, late)
                SELECT u.id::uuid, u.dev, u.ep, u.sq, false
                FROM unnest(?::text[], ?::text[], ?::bigint[], ?::bigint[]) AS u(id, dev, ep, sq)
                ORDER BY u.id
                ON CONFLICT (event_id) DO NOTHING RETURNING event_id::text""",
                text(uniq, a -> a.event().eventId()), text(uniq, a -> a.event().deviceId()),
                longs(uniq, a -> a.event().epoch()), longs(uniq, a -> a.event().seq())));
        int duplicates = batch.size() - fresh.size();

        Map<String, List<Applied>> byDevice = new TreeMap<>();
        for (Applied a : uniq) {
            if (fresh.contains(a.event().eventId())) byDevice.computeIfAbsent(a.event().deviceId(), k -> new ArrayList<>()).add(a);
        }
        if (byDevice.isEmpty()) return new Counts(0, duplicates, 0, 0);
        byDevice.values().forEach(l -> l.sort(Comparator.comparing(Applied::event, ORDER)));

        String[] ids = byDevice.keySet().toArray(String[]::new);
        List<String> created = query("""
                INSERT INTO device (device_id, state, last_epoch, last_seq, last_seen_at)
                SELECT u.id, 'HEALTHY', 0, 0, now() FROM unnest(?::text[]) AS u(id) ORDER BY u.id
                ON CONFLICT (device_id) DO NOTHING RETURNING device_id""", new Arr("text", ids));
        Map<String, long[]> cursor = new HashMap<>();
        jdbc.execute((Connection c) -> {
            try (var ps = c.prepareStatement("SELECT device_id, last_epoch, last_seq FROM device WHERE device_id = ANY(?) ORDER BY device_id FOR UPDATE")) {
                bind(c, ps, new Arr[] {new Arr("text", ids)});
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) cursor.put(rs.getString(1), new long[] {rs.getLong(2), rs.getLong(3)});
                }
            }
            return null;
        });

        Map<String, Stat> stats = new TreeMap<>();
        List<Applied> late = new ArrayList<>(), tel = new ArrayList<>();
        List<long[]> lateCursor = new ArrayList<>();
        Map<String, Applied> lastInOrder = new TreeMap<>();
        int inOrder = 0;
        long gapTotal = 0;
        for (var e : byDevice.entrySet()) {
            long[] cur = cursor.get(e.getKey());
            for (Applied a : e.getValue()) {
                Validator.Event ev = a.event();
                Stat st = stats.computeIfAbsent(ev.deviceId() + "\n" + ev.epoch(), k -> new Stat());
                st.applied++;
                st.maxSeq = Math.max(st.maxSeq, ev.seq());
                if (ev.epoch() < cur[0] || (ev.epoch() == cur[0] && ev.seq() <= cur[1])) {
                    st.late++;
                    late.add(a);
                    lateCursor.add(cur.clone());
                    continue;
                }
                long gap = ev.epoch() == cur[0] ? ev.seq() - cur[1] - 1 : ev.seq() - 1;
                if (ev.epoch() == cur[0] && gap + 1 > SEQ_JUMP_WARN) {
                    log.warn("seq jump device={} epoch={} from={} to={}", ev.deviceId(), ev.epoch(), cur[1], ev.seq());
                }
                st.gaps += gap;
                gapTotal += gap;
                cur[0] = ev.epoch();
                cur[1] = ev.seq();
                lastInOrder.put(ev.deviceId(), a);
                inOrder++;
                if (ev.telemetry()) tel.add(a);
            }
        }

        if (!late.isEmpty()) {
            update("UPDATE inbox SET late = true WHERE event_id = ANY(?::text[]::uuid[])", text(late, a -> a.event().eventId()));
            update("""
                    INSERT INTO late_event (event_id, device_id, epoch, seq, type, event_time, device_last_epoch, device_last_seq)
                    SELECT u.id::uuid, u.dev, u.ep, u.sq, u.ty, %s, u.le, u.ls
                    FROM unnest(?::text[], ?::text[], ?::bigint[], ?::bigint[], ?::text[], ?::bigint[], ?::bigint[], ?::bigint[])
                         AS u(id, dev, ep, sq, ty, ev, le, ls)
                    ON CONFLICT (event_id) DO NOTHING""".formatted(TS.formatted("u.ev")),
                    text(late, a -> a.event().eventId()), text(late, a -> a.event().deviceId()),
                    longs(late, a -> a.event().epoch()), longs(late, a -> a.event().seq()),
                    text(late, a -> a.event().telemetry() ? "telemetry" : "heartbeat"),
                    longs(late, a -> a.event().eventTimeMicros()),
                    new Arr("bigint", lateCursor.stream().map(c -> c[0]).toArray(Long[]::new)),
                    new Arr("bigint", lateCursor.stream().map(c -> c[1]).toArray(Long[]::new)));
        }

        if (!tel.isEmpty()) {
            update("""
                    INSERT INTO telemetry (device_id, epoch, seq, event_id, event_time, temperature_c, battery_pct, error_count, replay)
                    SELECT u.dev, u.ep, u.sq, u.id::uuid, %s, u.t, u.b, u.err::int, u.rp
                    FROM unnest(?::text[], ?::bigint[], ?::bigint[], ?::text[], ?::bigint[], ?::float8[], ?::float8[], ?::bigint[], ?::boolean[])
                         AS u(dev, ep, sq, id, ev, t, b, err, rp)
                    ON CONFLICT DO NOTHING""".formatted(TS.formatted("u.ev")),
                    text(tel, a -> a.event().deviceId()), longs(tel, a -> a.event().epoch()), longs(tel, a -> a.event().seq()),
                    text(tel, a -> a.event().eventId()), longs(tel, a -> a.event().eventTimeMicros()),
                    new Arr("float8", tel.stream().map(a -> a.event().temperatureC()).toArray(Double[]::new)),
                    new Arr("float8", tel.stream().map(a -> a.event().batteryPct()).toArray(Double[]::new)),
                    longs(tel, a -> (long) a.event().errorCount()),
                    new Arr("boolean", tel.stream().map(a -> a.event().replay()).toArray(Boolean[]::new)));
        }

        if (!lastInOrder.isEmpty()) {
            List<Applied> last = new ArrayList<>(lastInOrder.values());
            update("""
                    UPDATE device d SET last_epoch = u.ep, last_seq = u.sq, last_seen_at = %s, last_event_time = %s
                    FROM unnest(?::text[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[]) AS u(id, ep, sq, seen, ev)
                    WHERE d.device_id = u.id""".formatted(TS.formatted("u.seen"), TS.formatted("u.ev")),
                    text(last, a -> a.event().deviceId()), longs(last, a -> a.event().epoch()), longs(last, a -> a.event().seq()),
                    longs(last, a -> a.brokerTimeMs() * 1000), longs(last, a -> a.event().eventTimeMicros()));
        }

        List<String> keys = new ArrayList<>(stats.keySet());
        update("""
                INSERT INTO device_epoch_stats AS s (device_id, epoch, applied_count, max_seq, late_count, gap_count)
                SELECT u.dev, u.ep, u.ap, u.mx, u.lt, u.gp
                FROM unnest(?::text[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[], ?::bigint[]) AS u(dev, ep, ap, mx, lt, gp)
                ORDER BY u.dev, u.ep
                ON CONFLICT (device_id, epoch) DO UPDATE SET
                  applied_count = s.applied_count + EXCLUDED.applied_count, max_seq = GREATEST(s.max_seq, EXCLUDED.max_seq),
                  late_count = s.late_count + EXCLUDED.late_count, gap_count = s.gap_count + EXCLUDED.gap_count, updated_at = now()""",
                new Arr("text", keys.stream().map(k -> k.substring(0, k.indexOf('\n'))).toArray(String[]::new)),
                new Arr("bigint", keys.stream().map(k -> Long.parseLong(k.substring(k.indexOf('\n') + 1))).toArray(Long[]::new)),
                new Arr("bigint", keys.stream().map(k -> stats.get(k).applied).toArray(Long[]::new)),
                new Arr("bigint", keys.stream().map(k -> stats.get(k).maxSeq).toArray(Long[]::new)),
                new Arr("bigint", keys.stream().map(k -> stats.get(k).late).toArray(Long[]::new)),
                new Arr("bigint", keys.stream().map(k -> stats.get(k).gaps).toArray(Long[]::new)));

        notifyDevices(created);
        return new Counts(inOrder, duplicates, late.size(), gapTotal);
    }

    private void notifyDevices(List<String> ids) {
        for (int i = 0; i < ids.size(); i += NOTIFY_IDS) {
            String payload = mapper.writeValueAsString(
                    Map.of("t", "d", "ids", ids.subList(i, Math.min(ids.size(), i + NOTIFY_IDS))));
            jdbc.query("SELECT pg_notify(?, ?)", rs -> {}, channel, payload);
        }
    }

    static SQLException sqlCause(Throwable t) {
        for (; t != null; t = t.getCause()) if (t instanceof SQLException s) return s;
        return null;
    }

    private static Arr text(List<Applied> l, java.util.function.Function<Applied, String> f) {
        return new Arr("text", l.stream().map(f).toArray(String[]::new));
    }

    private static Arr longs(List<Applied> l, java.util.function.ToLongFunction<Applied> f) {
        return new Arr("bigint", l.stream().map(a -> f.applyAsLong(a)).toArray(Long[]::new));
    }

    private void update(String sql, Arr... arrays) {
        jdbc.execute((Connection c) -> {
            try (var ps = c.prepareStatement(sql)) {
                bind(c, ps, arrays);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private List<String> query(String sql, Arr... arrays) {
        return jdbc.execute((Connection c) -> {
            try (var ps = c.prepareStatement(sql)) {
                bind(c, ps, arrays);
                List<String> out = new ArrayList<>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) out.add(rs.getString(1));
                }
                return out;
            }
        });
    }

    private static void bind(Connection c, java.sql.PreparedStatement ps, Arr[] arrays) throws SQLException {
        for (int i = 0; i < arrays.length; i++) ps.setArray(i + 1, c.createArrayOf(arrays[i].type(), arrays[i].values()));
    }
}
