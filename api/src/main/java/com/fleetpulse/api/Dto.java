package com.fleetpulse.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class Dto {
    private Dto() {}

    public record DeviceSummary(String deviceId, String state, long stateVersion, Instant stateChangedAt, Instant lastSeenAt) {}

    public record DevicePage(List<DeviceSummary> items, int page, int size, long totalItems) {}

    public record FleetSummary(long total, Map<String, Long> byState, Instant asOf) {}

    public record Items<T>(List<T> items) {}

    public record Hello(String serverInstance, Instant at) {}

    public record Resync(String reason) {}
}
