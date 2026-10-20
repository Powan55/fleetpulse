package dev.fleetpulse.processor;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class Retention {

    private static final Logger log = LoggerFactory.getLogger(Retention.class);
    private static final int CHUNK = 5000;

    private final JdbcTemplate jdbc;
    private final Duration inbox, telemetry, late;

    Retention(JdbcTemplate jdbc, @Value("${fp.retention.inbox}") Duration inbox,
              @Value("${fp.retention.telemetry}") Duration telemetry, @Value("${fp.retention.late}") Duration late) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.telemetry = telemetry;
        this.late = late;
    }

    @Scheduled(fixedDelayString = "${fp.retention.interval}")
    void prune() {
        for (var t : new Object[][] {{"inbox", inbox}, {"telemetry", telemetry}, {"late_event", late}}) {
            try {
                prune((String) t[0], (Duration) t[1]);
            } catch (RuntimeException e) {
                log.warn("retention failed table={}", t[0], e);
            }
        }
    }

    private void prune(String table, Duration keep) {
        int n;
        do {
            n = jdbc.update("DELETE FROM " + table + " WHERE ctid IN (SELECT ctid FROM " + table
                    + " WHERE received_at < now() - make_interval(secs => ?) LIMIT " + CHUNK + ")", keep.toMillis() / 1000.0);
        } while (n == CHUNK);
    }
}
