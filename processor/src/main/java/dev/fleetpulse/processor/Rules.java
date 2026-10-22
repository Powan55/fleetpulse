package dev.fleetpulse.processor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.dataformat.yaml.YAMLMapper;

/** Typed rules.yaml. Anything unexpected fails startup. */
record Rules(int version, Hysteresis hysteresis, Map<String, Sev> severity, List<Rule> rules) {

    record Hysteresis(int enterSamples, int exitSamples, int recoverSamples, String stateRuleReopenAfter) {}

    record Bound(Double gte, Double lte) {
        boolean hit(double v) { return (gte != null && v >= gte) || (lte != null && v <= lte); }
    }

    record Sev(Bound degraded, Bound failed) {}

    record Rule(String id, String kind, String metric, String op, Double threshold, String state) {
        boolean metricRule() { return "metric".equals(kind); }

        boolean breached(double v) { return "gte".equals(op) ? v >= threshold : v <= threshold; }
    }

    static final int OK = 0, DEGRADED = 1, FAILED = 2;

    private static final Set<String> METRICS = Set.of("temperature_c", "battery_pct", "error_count");
    private static final Set<String> STATES = Set.of("HEALTHY", "DEGRADED", "FAILED", "OFFLINE", "RECOVERING");
    private static final Pattern DURATION = Pattern.compile("(\\d+)(ms|s|m)");

    int severity(double temperatureC, double batteryPct, int errorCount) {
        return Math.max(sev("temperature_c", temperatureC), Math.max(sev("battery_pct", batteryPct), sev("error_count", errorCount)));
    }

    private int sev(String metric, double v) {
        Sev s = severity.get(metric);
        return s.failed().hit(v) ? FAILED : s.degraded().hit(v) ? DEGRADED : OK;
    }

    Duration reopenAfter() {
        Matcher m = DURATION.matcher(hysteresis.stateRuleReopenAfter());
        if (!m.matches()) throw new IllegalArgumentException("bad duration " + hysteresis.stateRuleReopenAfter());
        long n = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> Duration.ofMillis(n);
            case "s" -> Duration.ofSeconds(n);
            default -> Duration.ofMinutes(n);
        };
    }

    static Rules load(String file) throws IOException {
        if (file != null && !file.isBlank()) return parse(Files.readString(Path.of(file), StandardCharsets.UTF_8));
        try (InputStream in = Rules.class.getResourceAsStream("/rules.yaml")) {
            return parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    static Rules parse(String yaml) {
        YAMLMapper mapper = YAMLMapper.builder()
                .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();
        Rules r = mapper.readValue(yaml, Rules.class);
        r.validate();
        return r;
    }

    private void validate() {
        if (version != 1) throw new IllegalArgumentException("rules version " + version);
        if (hysteresis == null || hysteresis.enterSamples() < 1 || hysteresis.exitSamples() < 1 || hysteresis.recoverSamples() < 1) {
            throw new IllegalArgumentException("hysteresis");
        }
        reopenAfter();
        if (severity == null || !severity.keySet().equals(METRICS)) throw new IllegalArgumentException("severity metrics");
        for (Sev s : severity.values()) {
            if (s == null || s.degraded() == null || s.failed() == null) throw new IllegalArgumentException("severity bounds");
        }
        if (rules == null || rules.isEmpty()) throw new IllegalArgumentException("no rules");
        Set<String> ids = new HashSet<>();
        for (Rule x : rules) {
            if (x.id() == null || !ids.add(x.id())) throw new IllegalArgumentException("rule id " + x.id());
            if (x.metricRule()) {
                if (!METRICS.contains(x.metric()) || x.threshold() == null || !("gte".equals(x.op()) || "lte".equals(x.op()))) {
                    throw new IllegalArgumentException("metric rule " + x.id());
                }
            } else if (!"state".equals(x.kind()) || !STATES.contains(x.state())) {
                throw new IllegalArgumentException("state rule " + x.id());
            }
        }
    }
}
