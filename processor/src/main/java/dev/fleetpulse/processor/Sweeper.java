package dev.fleetpulse.processor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Marks silent devices OFFLINE unless the consumer is behind, and re-opens state alerts an operator resolved too early. */
@Component
class Sweeper {

    interface LagProbe {
        /** Highest per-partition lag in records. */
        long maxLag() throws Exception;
    }

    private record Swept(String device, String from) {}

    private static final Logger log = LoggerFactory.getLogger(Sweeper.class);
    private static final int BATCH = 500;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final LagProbe probe;
    private final AlertStore alerts;
    private final Rules rules;
    private final MeterRegistry meters;
    private final Duration timeout, grace;
    private final long maxLag;
    private volatile Instant lastBehind = Instant.now();

    Sweeper(JdbcTemplate jdbc, TransactionTemplate tx, LagProbe probe, AlertStore alerts, Rules rules, MeterRegistry meters,
            @Value("${fp.sweep.offline-timeout}") Duration timeout, @Value("${fp.sweep.grace}") Duration grace,
            @Value("${fp.sweep.max-lag}") long maxLag) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.probe = probe;
        this.alerts = alerts;
        this.rules = rules;
        this.meters = meters;
        this.timeout = timeout;
        this.grace = grace;
        this.maxLag = maxLag;
    }

    @Scheduled(fixedDelayString = "${fp.sweep.interval}")
    void tick() {
        try {
            sweep();
        } catch (RuntimeException e) {
            meters.counter("fleetpulse.processor.sweep.runs", "result", "skipped_error").increment();
            log.warn("sweep failed", e);
        }
        try {
            reopen();
        } catch (RuntimeException e) {
            log.warn("state alert reopen failed", e);
        }
    }

    /** Returns the sweep.runs result tag. */
    String sweep() {
        String result;
        try {
            if (probe.maxLag() > maxLag) {
                lastBehind = Instant.now();
                result = "skipped_lag";
            } else if (Instant.now().isBefore(lastBehind.plus(grace))) {
                result = "skipped_grace";
            } else {
                result = "ran";
            }
        } catch (Exception e) {
            log.warn("lag check failed: {}", e.toString());
            result = "skipped_error";
        }
        meters.counter("fleetpulse.processor.sweep.runs", "result", result).increment();
        if (result.equals("ran")) tx.executeWithoutResult(s -> markOffline());
        return result;
    }

    private void markOffline() {
        List<Swept> rows = jdbc.query("""
                WITH c AS (SELECT device_id, state FROM device
                           WHERE state <> 'OFFLINE' AND last_seen_at < now() - make_interval(secs => ?)
                           ORDER BY device_id LIMIT ? FOR UPDATE SKIP LOCKED)
                UPDATE device d SET state = 'OFFLINE', state_version = d.state_version + 1, state_changed_at = now(),
                                    bad_n = 0, fail_n = 0, ok_n = 0
                FROM c WHERE d.device_id = c.device_id
                RETURNING d.device_id, c.state""", (rs, i) -> new Swept(rs.getString(1), rs.getString(2)),
                timeout.toMillis() / 1000.0, BATCH);
        if (rows.isEmpty()) return;
        Set<Long> touched = new LinkedHashSet<>();
        Set<String> devices = new LinkedHashSet<>();
        for (Swept w : rows) {
            jdbc.update("INSERT INTO state_transition (device_id, from_state, to_state, cause) VALUES (?, ?, 'OFFLINE', 'offline_sweep')", w.device(), w.from());
            meters.counter("fleetpulse.processor.transitions", "from", w.from(), "to", "OFFLINE").increment();
            for (Rules.Rule r : rules.rules()) {
                if (r.metricRule()) continue;
                if (r.state().equals(w.from())) alerts.resolve(w.device(), r.id(), touched);
                if (r.state().equals("OFFLINE")) alerts.open(w.device(), r.id(), false, null, null, touched);
            }
            devices.add(w.device());
        }
        alerts.notifyDevices(devices);
        if (!touched.isEmpty()) alerts.notifyAlerts(touched);
        log.info("offline sweep marked {} devices", rows.size());
    }

    void reopen() {
        List<String> ids = new ArrayList<>(), states = new ArrayList<>();
        for (Rules.Rule r : rules.rules()) {
            if (!r.metricRule()) {
                ids.add(r.id());
                states.add(r.state());
            }
        }
        tx.executeWithoutResult(s -> {
            Set<Long> touched = new LinkedHashSet<>();
            alerts.reopenStateRules(ids.toArray(String[]::new), states.toArray(String[]::new), rules.reopenAfter(), touched);
            if (!touched.isEmpty()) alerts.notifyAlerts(touched);
        });
    }
}
