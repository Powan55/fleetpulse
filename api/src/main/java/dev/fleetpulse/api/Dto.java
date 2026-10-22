package dev.fleetpulse.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class Dto {
    private Dto() {}

    public record DeviceSummary(String deviceId, String state, long stateVersion, Instant stateChangedAt, Instant lastSeenAt) {}

    public record DevicePage(List<DeviceSummary> items, int page, int size, long totalItems) {}

    public record TelemetryPoint(long epoch, long seq, Instant eventTime, Instant receivedAt, double temperatureC,
                                 double batteryPct, int errorCount, boolean replay) {}

    public record StateTransition(long id, String fromState, String toState, String cause, UUID eventId,
                                  Instant eventTime, Instant occurredAt, boolean replay) {}

    public record DeviceDetail(String deviceId, String state, long stateVersion, Instant stateChangedAt,
                               Instant lastSeenAt, Instant lastEventTime, long lastEpoch, long lastSeq,
                               List<TelemetryPoint> recentTelemetry, List<StateTransition> stateHistory,
                               List<Alert> alerts) {}

    public record Alert(long alertId, String deviceId, String ruleId, String status, int version, boolean backfilled,
                        Instant openedAt, Instant ackedAt, Instant resolvedAt, String resolvedBy, Instant updatedAt,
                        UUID triggerEventId, Instant conditionStartedEventTime) {}

    public record AlertPage(List<Alert> items, int page, int size, long totalItems) {}

    public record FleetSummary(long total, Map<String, Long> byState, Instant asOf) {}

    public record Items<T>(List<T> items) {}

    public record Hello(String serverInstance, Instant at) {}

    public record Resync(String reason) {}
}
