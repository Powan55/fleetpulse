package dev.fleetpulse.simulator;

import java.util.List;

record Chaos(
        double duplicateRate,
        double dropRate,
        double reorderRate,
        List<Skew> skewSets,
        boolean replayOnReconnect,
        Long replayMaxRate) {

    record Skew(double fraction, long offsetMs) {}

    static final Chaos OFF = new Chaos(0, 0, 0, List.of(), true, null);
}
