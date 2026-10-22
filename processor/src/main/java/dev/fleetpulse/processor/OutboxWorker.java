package dev.fleetpulse.processor;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Delivers outbox rows over HTTP outside any transaction; rows are claimed with a lease so replicas never share one. */
@Component
class OutboxWorker {

    private record Row(long id, String key, String payload, String traceparent, int attempts, Instant createdAt) {}

    private enum Kind { SUCCESS, RETRY, PERMANENT }

    private record Outcome(Kind kind, Integer status, String error) {}

    private static final Logger log = LoggerFactory.getLogger(OutboxWorker.class);
    private static final int CLAIM = 50;
    private static final int BODY_CAP = 4096;
    private static final int ERROR_CAP = 512;

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final HttpClient client;
    private final URI sink;
    private final Duration timeout, backoffBase, backoffMax;
    private final int maxAttempts;
    private final double jitter;
    private final ExecutorService pool;

    OutboxWorker(JdbcTemplate jdbc, TransactionTemplate tx, MeterRegistry meters,
                 @Value("${fp.webhook.sink-url}") String sinkUrl, @Value("${fp.webhook.timeout}") Duration timeout,
                 @Value("${fp.webhook.max-attempts}") int maxAttempts, @Value("${fp.webhook.backoff-base}") Duration backoffBase,
                 @Value("${fp.webhook.backoff-max}") Duration backoffMax, @Value("${fp.webhook.jitter}") double jitter,
                 @Value("${fp.webhook.concurrency}") int concurrency) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.meters = meters;
        this.sink = validate(sinkUrl);
        this.timeout = timeout;
        this.maxAttempts = maxAttempts;
        this.backoffBase = backoffBase;
        this.backoffMax = backoffMax;
        this.jitter = jitter;
        this.client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout).build();
        this.pool = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r, "webhook");
            t.setDaemon(true);
            return t;
        });
    }

    static URI validate(String url) {
        try {
            URI u = URI.create(url);
            if (!("http".equals(u.getScheme()) || "https".equals(u.getScheme())) || u.getHost() == null) {
                throw new IllegalArgumentException("sink url must be http(s) with a host");
            }
            return u;
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("invalid FP_SINK_URL", e);
        }
    }

    @Scheduled(fixedDelayString = "${fp.webhook.poll-interval}")
    void poll() {
        try {
            List<Row> rows = claim();
            if (rows.isEmpty()) return;
            List<Callable<Void>> jobs = new ArrayList<>();
            for (Row r : rows) {
                jobs.add(() -> {
                    try {
                        deliver(r);
                    } catch (RuntimeException e) {
                        log.warn("outbox delivery bookkeeping failed id={}", r.id(), e);
                    }
                    return null;
                });
            }
            pool.invokeAll(jobs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("outbox poll failed", e);
        }
    }

    private List<Row> claim() {
        double lease = timeout.toMillis() / 1000.0 + 5;
        return tx.execute(s -> jdbc.query("""
                WITH c AS (SELECT o.outbox_id FROM outbox o
                           WHERE o.status = 'PENDING' AND o.next_attempt_at <= now()
                             AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.alert_id = o.alert_id AND p.status = 'PENDING' AND p.outbox_id < o.outbox_id)
                           ORDER BY o.next_attempt_at, o.outbox_id LIMIT ? FOR UPDATE OF o SKIP LOCKED)
                UPDATE outbox o SET next_attempt_at = now() + make_interval(secs => ?)
                FROM c WHERE o.outbox_id = c.outbox_id
                RETURNING o.outbox_id, o.idempotency_key, o.payload::text, o.traceparent, o.attempts, o.created_at""",
                (rs, i) -> new Row(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getInt(5),
                        rs.getTimestamp(6).toInstant()), CLAIM, lease));
    }

    private void deliver(Row r) {
        long t0 = System.nanoTime();
        Outcome o = post(r);
        meters.timer("fleetpulse.webhook.delivery.duration").record(Duration.ofNanos(System.nanoTime() - t0));
        int attempts = r.attempts() + 1;
        switch (o.kind()) {
            case SUCCESS -> {
                jdbc.update("UPDATE outbox SET status = 'DELIVERED', attempts = ?, delivered_at = now(), last_status_code = ?, last_error = NULL "
                        + "WHERE outbox_id = ? AND status = 'PENDING'", attempts, o.status(), r.id());
                meters.counter("fleetpulse.webhook.deliveries", "result", "success").increment();
                meters.timer("fleetpulse.webhook.delivery.latency").record(Duration.between(r.createdAt(), Instant.now()));
            }
            case PERMANENT -> {
                dead(r, attempts, o);
                meters.counter("fleetpulse.webhook.deliveries", "result", "permanent_failure").increment();
            }
            case RETRY -> {
                if (attempts >= maxAttempts) {
                    dead(r, attempts, o);
                    meters.counter("fleetpulse.webhook.deliveries", "result", "retryable_failure").increment();
                } else {
                    jdbc.update("UPDATE outbox SET attempts = ?, next_attempt_at = now() + make_interval(secs => ?), last_status_code = ?, last_error = ? "
                            + "WHERE outbox_id = ? AND status = 'PENDING'", attempts, backoff(attempts), o.status(), o.error(), r.id());
                    meters.counter("fleetpulse.webhook.deliveries", "result", "retryable_failure").increment();
                }
            }
        }
    }

    private void dead(Row r, int attempts, Outcome o) {
        jdbc.update("UPDATE outbox SET status = 'DEAD', attempts = ?, dead_at = now(), last_status_code = ?, last_error = ? "
                + "WHERE outbox_id = ? AND status = 'PENDING'", attempts, o.status(), o.error(), r.id());
        meters.counter("fleetpulse.webhook.deliveries", "result", "dead").increment();
        log.warn("outbox row dead id={} key={} status={}", r.id(), r.key(), o.status());
    }

    double backoff(int attempts) {
        double base = backoffBase.toMillis() / 1000.0, max = backoffMax.toMillis() / 1000.0;
        double d = Math.min(max, base * Math.pow(2, Math.min(attempts - 1, 30)));
        return d * (1 + jitter * (2 * ThreadLocalRandom.current().nextDouble() - 1));
    }

    private Outcome post(Row r) {
        HttpRequest.Builder b = HttpRequest.newBuilder(sink).timeout(timeout).header("Content-Type", "application/json")
                .header("Idempotency-Key", r.key()).POST(HttpRequest.BodyPublishers.ofString(r.payload()));
        if (r.traceparent() != null) b.header("traceparent", r.traceparent());
        try {
            HttpResponse<InputStream> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofInputStream());
            String body;
            try (InputStream in = resp.body()) {
                body = new String(in.readNBytes(BODY_CAP), StandardCharsets.UTF_8);
            }
            int code = resp.statusCode();
            String err = cap("HTTP " + code + " " + body);
            if (code >= 200 && code < 300) return new Outcome(Kind.SUCCESS, code, null);
            if (code == 408 || code == 429 || code >= 500) return new Outcome(Kind.RETRY, code, err);
            return new Outcome(Kind.PERMANENT, code, err);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Outcome(Kind.RETRY, null, "interrupted");
        } catch (Exception e) {
            return new Outcome(Kind.RETRY, null, cap(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    private static String cap(String s) {
        return s.length() <= ERROR_CAP ? s : s.substring(0, ERROR_CAP);
    }

    @PreDestroy
    void stop() {
        pool.shutdownNow();
    }
}
