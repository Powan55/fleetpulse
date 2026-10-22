package dev.fleetpulse.processor;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class Retention {

    private record Rule(String table, String column, String extra, Duration keep) {}

    private static final Logger log = LoggerFactory.getLogger(Retention.class);
    private static final int CHUNK = 5000;

    private final JdbcTemplate jdbc;
    private final List<Rule> rules;

    Retention(JdbcTemplate jdbc, @Value("${fp.retention.inbox}") Duration inbox, @Value("${fp.retention.telemetry}") Duration telemetry,
              @Value("${fp.retention.late}") Duration late, @Value("${fp.retention.history}") Duration history,
              @Value("${fp.retention.alerts}") Duration alerts, @Value("${fp.retention.outbox-delivered}") Duration delivered) {
        this.jdbc = jdbc;
        // Outbox goes before alerts: an alert is kept while any of its outbox rows exists (PENDING and DEAD rows are never deleted).
        this.rules = List.of(
                new Rule("inbox", "received_at", "", inbox),
                new Rule("telemetry", "received_at", "", telemetry),
                new Rule("late_event", "received_at", "", late),
                new Rule("state_transition", "occurred_at", "", history),
                new Rule("outbox", "delivered_at", "status = 'DELIVERED' AND", delivered),
                new Rule("alert", "resolved_at", "status = 'RESOLVED' AND NOT EXISTS (SELECT 1 FROM outbox WHERE outbox.alert_id = alert.alert_id) AND", alerts));
    }

    @Scheduled(fixedDelayString = "${fp.retention.interval}")
    void prune() {
        for (Rule r : rules) {
            try {
                prune(r);
            } catch (RuntimeException e) {
                log.warn("retention failed table={}", r.table(), e);
            }
        }
    }

    private void prune(Rule r) {
        int n;
        do {
            n = jdbc.update("DELETE FROM " + r.table() + " WHERE ctid IN (SELECT ctid FROM " + r.table() + " WHERE " + r.extra() + " "
                    + r.column() + " < now() - make_interval(secs => ?) LIMIT " + CHUNK + ")", r.keep().toMillis() / 1000.0);
        } while (n == CHUNK);
    }
}
