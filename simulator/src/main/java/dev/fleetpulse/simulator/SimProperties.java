package dev.fleetpulse.simulator;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("sim")
public record SimProperties(
        String topic,
        int deviceCount,
        long seed,
        Long epoch,
        Duration heartbeatInterval,
        Duration telemetryInterval,
        Duration reorderHold) {

    public SimProperties {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("sim.topic is required");
        }
        if (deviceCount < 1 || deviceCount > 99999) {
            throw new IllegalArgumentException("sim.device-count must be 1..99999");
        }
        if (heartbeatInterval == null || heartbeatInterval.toMillis() < 1) {
            throw new IllegalArgumentException("sim.heartbeat-interval must be >= 1ms");
        }
        if (telemetryInterval == null || telemetryInterval.toMillis() < 1) {
            throw new IllegalArgumentException("sim.telemetry-interval must be >= 1ms");
        }
        if (reorderHold == null) {
            reorderHold = Duration.ofSeconds(1);
        }
        if (reorderHold.toMillis() < 1) {
            throw new IllegalArgumentException("sim.reorder-hold must be >= 1ms");
        }
        if (epoch != null && epoch < 1) {
            throw new IllegalArgumentException("sim.epoch must be positive");
        }
    }
}
