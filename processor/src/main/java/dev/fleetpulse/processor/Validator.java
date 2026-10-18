package dev.fleetpulse.processor;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Total validation of the telemetry envelope; never throws, returns a DLQ reason instead. */
final class Validator {

    record Event(String deviceId, long epoch, long seq, long eventTimeMicros) {}

    record Result(Event event, String reason) {
        static Result ok(Event e) { return new Result(e, null); }
        static Result bad(String reason) { return new Result(null, reason); }
    }

    static final int MAX_BYTES = 1024;
    private static final long MAX_SAFE = 9007199254740991L;
    private static final Instant MIN_TIME = Instant.parse("2000-01-01T00:00:00Z");
    private static final Instant MAX_TIME = Instant.parse("2100-01-01T00:00:00Z");
    private static final Pattern EVENT_ID = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-3[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");
    private static final Pattern DEVICE_ID = Pattern.compile("dev-[0-9]{4,6}");
    private static final Set<String> FIELDS = Set.of("schema_version", "event_id", "device_id", "epoch", "seq",
            "event_time", "type", "replay", "payload");
    private static final Set<String> TELEMETRY = Set.of("temperature_c", "battery_pct", "error_count");

    private static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    private Validator() {}

    static Result validate(byte[] key, byte[] value, long brokerTimeMs) {
        if (value == null || value.length > MAX_BYTES) return Result.bad("too_large");
        JsonNode n;
        try {
            n = MAPPER.readTree(new String(value, StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            return Result.bad("invalid_json");
        }
        if (n == null || !n.isObject()) return Result.bad("schema");
        for (String f : n.propertyNames()) if (!FIELDS.contains(f)) return Result.bad("schema");
        for (String f : FIELDS) if (!n.has(f)) return Result.bad("schema");

        if (!isInt(n.get("schema_version")) || n.get("schema_version").asLong() != 1) return Result.bad("schema");
        String eventId = text(n.get("event_id"));
        if (eventId == null || !EVENT_ID.matcher(eventId).matches() || !isUuid(eventId)) return Result.bad("schema");
        String deviceId = text(n.get("device_id"));
        if (deviceId == null || !DEVICE_ID.matcher(deviceId).matches()) return Result.bad("schema");
        if (!inRange(n.get("epoch")) || !inRange(n.get("seq"))) return Result.bad("schema");
        long micros = micros(text(n.get("event_time")));
        if (micros == Long.MIN_VALUE) return Result.bad("schema");
        String type = text(n.get("type"));
        if (!"heartbeat".equals(type) && !"telemetry".equals(type)) return Result.bad("schema");
        if (!n.get("replay").isBoolean()) return Result.bad("schema");
        if (!payloadOk(n.get("payload"), "telemetry".equals(type))) return Result.bad("schema");

        if (key == null || !Arrays.equals(key, deviceId.getBytes(StandardCharsets.UTF_8))) return Result.bad("key_mismatch");
        long epoch = n.get("epoch").asLong();
        if (epoch > brokerTimeMs + 24L * 3600_000L) return Result.bad("epoch_future");
        return Result.ok(new Event(deviceId, epoch, n.get("seq").asLong(), micros));
    }

    private static boolean payloadOk(JsonNode p, boolean telemetry) {
        if (!p.isObject()) return false;
        if (!telemetry) return p.isEmpty();
        if (p.size() != 3) return false;
        for (String f : p.propertyNames()) if (!TELEMETRY.contains(f)) return false;
        JsonNode t = p.get("temperature_c"), b = p.get("battery_pct"), e = p.get("error_count");
        return t != null && t.isNumber() && t.asDouble() >= -100 && t.asDouble() <= 200
                && b != null && b.isNumber() && b.asDouble() >= 0 && b.asDouble() <= 100
                && e != null && isInt(e) && e.asLong() >= 0 && e.asLong() <= 1_000_000;
    }

    private static boolean isInt(JsonNode v) { return v != null && v.isIntegralNumber() && v.canConvertToLong(); }

    private static boolean inRange(JsonNode v) { return isInt(v) && v.asLong() >= 1 && v.asLong() <= MAX_SAFE; }

    private static String text(JsonNode v) { return v != null && v.isString() ? v.asString() : null; }

    private static boolean isUuid(String s) {
        try { UUID.fromString(s); return true; } catch (IllegalArgumentException e) { return false; }
    }

    private static long micros(String s) {
        if (s == null) return Long.MIN_VALUE;
        try {
            Instant i = OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
            if (i.isBefore(MIN_TIME) || !i.isBefore(MAX_TIME)) return Long.MIN_VALUE;
            return i.getEpochSecond() * 1_000_000L + i.getNano() / 1000;
        } catch (RuntimeException e) {
            return Long.MIN_VALUE;
        }
    }
}
