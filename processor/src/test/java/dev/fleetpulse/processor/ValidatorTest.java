package dev.fleetpulse.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ValidatorTest {

    static final long NOW = 1_792_130_400_000L;

    static String event(String deviceId, String eventTime, String epoch, String type, String payload) {
        String id = UUID.nameUUIDFromBytes((deviceId + epoch).getBytes(StandardCharsets.UTF_8)).toString();
        return """
                {"schema_version":1,"event_id":"%s","device_id":%s,"epoch":%s,"seq":7,"event_time":%s,"type":"%s","replay":false,"payload":%s}"""
                .formatted(id, deviceId, epoch, eventTime, type, payload);
    }

    static final String T = "{\"temperature_c\":41.5,\"battery_pct\":87.0,\"error_count\":0}";

    static String reason(String json, String key) {
        return Validator.validate(key.getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8), NOW).reason();
    }

    static String good() { return event("\"dev-0042\"", "\"2026-10-16T12:34:56.789Z\"", "1792130400000", "telemetry", T); }

    @Test
    void validRecordPasses() {
        var r = Validator.validate("dev-0042".getBytes(StandardCharsets.UTF_8), good().getBytes(StandardCharsets.UTF_8), NOW);
        assertNull(r.reason());
        assertEquals(7, r.event().seq());
    }

    @Test
    void heartbeatPasses() {
        assertNull(reason(event("\"dev-0042\"", "\"2026-10-16T12:34:56Z\"", "1792130400000", "heartbeat", "{}"), "dev-0042"));
    }

    @Test
    void trailingNewlineInDeviceIdIsRejected() {
        assertEquals("schema", reason(event("\"dev-0001\\n\"", "\"2026-10-16T12:34:56Z\"", "1792130400000", "telemetry", T), "dev-0001\n"));
    }

    @Test
    void badEventTimeIsRejected() {
        assertEquals("schema", reason(event("\"dev-0042\"", "\"x\"", "1792130400000", "telemetry", T), "dev-0042"));
        assertEquals("schema", reason(event("\"dev-0042\"", "\"+99999-01-01T00:00:00Z\"", "1792130400000", "telemetry", T), "dev-0042"));
        assertEquals("schema", reason(event("\"dev-0042\"", "\"1999-12-31T23:59:59Z\"", "1792130400000", "telemetry", T), "dev-0042"));
    }

    @Test
    void keyMismatch() {
        assertEquals("key_mismatch", reason(good(), "dev-0043"));
    }

    @Test
    void futureEpoch() {
        String far = Long.toString(NOW + 25L * 3600_000L);
        assertEquals("epoch_future", reason(event("\"dev-0042\"", "\"2026-10-16T12:34:56Z\"", far, "telemetry", T), "dev-0042"));
    }

    @Test
    void duplicateKeysAndGarbage() {
        assertEquals("invalid_json", reason(good().replace("\"seq\":7", "\"seq\":7,\"seq\":8"), "dev-0042"));
        assertEquals("invalid_json", reason("{not json", "dev-0042"));
        assertEquals("invalid_json", reason(good() + " {}", "dev-0042"));
    }

    @Test
    void oversize() {
        assertEquals("too_large", reason(good() + " ".repeat(1100), "dev-0042"));
    }

    @Test
    void payloadRules() {
        assertEquals("schema", reason(event("\"dev-0042\"", "\"2026-10-16T12:34:56Z\"", "1792130400000", "telemetry",
                "{\"temperature_c\":500,\"battery_pct\":87,\"error_count\":0}"), "dev-0042"));
        assertEquals("schema", reason(event("\"dev-0042\"", "\"2026-10-16T12:34:56Z\"", "1792130400000", "heartbeat", T), "dev-0042"));
        assertEquals("schema", reason(good().replace("}}", "},\"extra\":1}"), "dev-0042"));
    }
}
