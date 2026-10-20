package dev.fleetpulse.simulator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;

/** One simulated device. Not thread safe; the scheduler is the only caller. */
final class Device {

    record Event(String eventId, String deviceId, long epoch, long seq, String type, String json) {}

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    final String id;
    private final long seed;
    private final long epoch;
    private final Random rnd;
    /** Independent stream for chaos draws so chaos settings never change payloads. */
    final Random chaos;
    private long seq;

    /** Start offsets in [0, interval) drawn first so they depend only on seed and index. */
    final long heartbeatPhaseNanos;
    final long telemetryPhaseNanos;

    Device(int index, long seed, long epoch, long heartbeatNanos, long telemetryNanos) {
        this.id = String.format("dev-%04d", index);
        this.seed = seed;
        this.epoch = epoch;
        this.rnd = new Random(seed * 1_000_003L + index);
        this.chaos = new Random((seed * 1_000_003L + index) ^ 0x5DEECE66DL);
        this.heartbeatPhaseNanos = (long) (rnd.nextDouble() * heartbeatNanos);
        this.telemetryPhaseNanos = (long) (rnd.nextDouble() * telemetryNanos);
    }

    Event next(String type, Instant now) {
        return next(type, now, 0);
    }

    Event next(String type, Instant now, long skewMs) {
        long s = ++seq;
        String eventId = UUID.nameUUIDFromBytes(
                (seed + ":" + id + ":" + epoch + ":" + s).getBytes(StandardCharsets.UTF_8)).toString();
        String payload = "heartbeat".equals(type) ? "{}" : String.format(Locale.ROOT,
                "{\"temperature_c\":%.1f,\"battery_pct\":%.1f,\"error_count\":%d}",
                20 + rnd.nextDouble() * 30, 40 + rnd.nextDouble() * 60, rnd.nextInt(2));
        String json = String.format(Locale.ROOT,
                "{\"schema_version\":1,\"event_id\":\"%s\",\"device_id\":\"%s\",\"epoch\":%d,\"seq\":%d,"
                        + "\"event_time\":\"%s\",\"type\":\"%s\",\"replay\":false,\"payload\":%s}",
                eventId, id, epoch, s, TIME.format(now.plusMillis(skewMs)), type, payload);
        return new Event(eventId, id, epoch, s, type, json);
    }
}
