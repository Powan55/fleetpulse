package dev.fleetpulse.simulator;

import java.time.Instant;
import java.util.List;

/** A constant level or a repeating step pattern on one metric for a set of devices. */
final class Fault {

    enum Level { OK, DEGRADED, FAILED }

    record Step(Level level, int samples) {}

    final String id;
    final String metric;
    final Level level;
    final List<Step> steps;
    final int[] devices;
    final Long untilNanos;
    final Instant until;
    private final int[] pos;
    private final int cycle;

    Fault(String id, String metric, Level level, List<Step> steps, int[] devices, int deviceCount, Long untilNanos, Instant until) {
        this.id = id;
        this.metric = metric;
        this.level = level;
        this.steps = steps;
        this.devices = devices;
        this.untilNanos = untilNanos;
        this.until = until;
        this.pos = new int[deviceCount];
        this.cycle = steps == null ? 0 : steps.stream().mapToInt(Step::samples).sum();
    }

    /** Level for the next telemetry sample of this device; patterns advance one sample per call. */
    Level next(int device) {
        if (level != null) {
            return level;
        }
        int p = pos[device];
        pos[device] = (p + 1) % cycle;
        for (Step s : steps) {
            if (p < s.samples()) {
                return s.level();
            }
            p -= s.samples();
        }
        throw new IllegalStateException();
    }
}
