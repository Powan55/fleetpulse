package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DeviceTest {

    private static final long SEC = 2_000_000_000L;
    private static final Instant NOW = Instant.parse("2026-10-16T12:34:56.789Z");

    @Test
    void eventIdIsUuidV3OfSeedDeviceEpochSeq() {
        Device d = new Device(42, 7, 1000, SEC, SEC);
        Device.Event e = d.next("telemetry", NOW);
        String expected = UUID.nameUUIDFromBytes("7:dev-0042:1000:1".getBytes(StandardCharsets.UTF_8)).toString();
        assertEquals(expected, e.eventId());
        assertEquals("dev-0042", e.deviceId());
        assertTrue(e.json().contains("\"event_time\":\"2026-10-16T12:34:56.789Z\""));
    }

    @Test
    void seqIncreasesAcrossTypes() {
        Device d = new Device(1, 42, 1000, SEC, SEC);
        assertEquals(1, d.next("heartbeat", NOW).seq());
        assertEquals(2, d.next("telemetry", NOW).seq());
        assertEquals(3, d.next("heartbeat", NOW).seq());
    }

    @Test
    void sameSeedAndEpochGiveSameStream() {
        Device a = new Device(5, 42, 1000, SEC, SEC);
        Device b = new Device(5, 42, 1000, SEC, SEC);
        assertEquals(a.heartbeatPhaseNanos, b.heartbeatPhaseNanos);
        for (int i = 0; i < 5; i++) {
            assertEquals(a.next("telemetry", NOW).json(), b.next("telemetry", NOW).json());
        }
        assertNotEquals(new Device(5, 42, 1001, SEC, SEC).next("telemetry", NOW).eventId(),
                new Device(5, 42, 1000, SEC, SEC).next("telemetry", NOW).eventId());
    }

    @Test
    void phasesStayInsideInterval() {
        for (int i = 1; i <= 1000; i++) {
            Device d = new Device(i, 42, 1000, SEC, SEC);
            assertTrue(d.heartbeatPhaseNanos >= 0 && d.heartbeatPhaseNanos < SEC);
            assertTrue(d.telemetryPhaseNanos >= 0 && d.telemetryPhaseNanos < SEC);
        }
    }
}
