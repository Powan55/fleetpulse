package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {"fp.webhook.poll-interval=50ms", "fp.webhook.backoff-base=50ms", "fp.webhook.backoff-max=200ms",
        "fp.webhook.max-attempts=4", "fp.webhook.timeout=2s"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxIT {

    record Hit(String key, String path, String body, String contentType) {}

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));
    static final PostgreSQLContainer PG = new PostgreSQLContainer(DockerImageName.parse("postgres:18.6"));
    static final List<Hit> HITS = new CopyOnWriteArrayList<>();
    static final Map<String, Queue<Integer>> SCRIPT = new ConcurrentHashMap<>();
    static HttpServer SINK;

    static {
        KAFKA.start();
        PG.start();
        Flyway.configure().dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("filesystem:../db/migrations")
                .placeholders(Map.of("app_password", "app-pw", "api_password", "api-pw", "ro_password", "ro-pw"))
                .load().migrate();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("fleetpulse.telemetry.v1", 6, (short) 1),
                    new NewTopic("fleetpulse.telemetry.v1.dlq", 1, (short) 1))).all().get();
            SINK = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        SINK.createContext("/", x -> {
            String key = x.getRequestHeaders().getFirst("Idempotency-Key");
            String body = new String(x.getRequestBody().readAllBytes());
            HITS.add(new Hit(key, x.getRequestURI().getPath(), body, x.getRequestHeaders().getFirst("Content-Type")));
            Queue<Integer> q = SCRIPT.get(key);
            Integer code = q == null ? null : q.poll();
            int status = code == null ? 200 : code;
            if (status == 302) x.getResponseHeaders().add("Location", "/redirected");
            if (status == 599) {
                byte[] big = new byte[1 << 20];
                java.util.Arrays.fill(big, (byte) 'x');
                x.sendResponseHeaders(500, big.length);
                try {
                    x.getResponseBody().write(big);
                } catch (java.io.IOException ignored) {
                    // client stops reading at its cap
                }
            } else {
                x.sendResponseHeaders(status, -1);
            }
            x.close();
        });
        SINK.start();
    }

    @AfterAll
    static void stop() {
        SINK.stop(0);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", () -> "fleetpulse_app");
        r.add("spring.datasource.password", () -> "app-pw");
        r.add("fp.webhook.sink-url", () -> "http://127.0.0.1:" + SINK.getAddress().getPort() + "/webhooks/alerts");
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    @Autowired MeterRegistry meters;

    long alert(String dev) {
        return jdbc.queryForObject("INSERT INTO alert (device_id, rule_id, status) VALUES (?, 'battery_low', 'OPEN') RETURNING alert_id", Long.class, dev);
    }

    String row(long alert, int version, String transition, Integer... script) {
        String key = "alert-" + alert + "-" + transition;
        if (script.length > 0) SCRIPT.put(key, new ConcurrentLinkedQueue<>(List.of(script)));
        jdbc.update("INSERT INTO outbox (alert_id, alert_version, transition, idempotency_key, payload) VALUES (?, ?, ?, ?, ?::jsonb)",
                alert, version, transition, key, "{\"k\":\"" + key + "\"}");
        return key;
    }

    String state(String key) {
        return jdbc.queryForObject("SELECT status||','||attempts||','||coalesce(last_status_code::text,'-') FROM outbox WHERE idempotency_key=?", String.class, key);
    }

    void awaitNot(String key, String notStatus) {
        await().atMost(Duration.ofSeconds(20)).until(() -> !state(key).startsWith(notStatus));
    }

    List<Hit> hits(String key) {
        return HITS.stream().filter(h -> key.equals(h.key())).toList();
    }

    @Test
    void serverErrorThenOkIsDeliveredOnceWithTheSameKey() {
        String k = row(alert("dev-0401"), 1, "opened", 500);
        awaitNot(k, "PENDING");
        assertThat(state(k)).isEqualTo("DELIVERED,2,200");
        assertThat(hits(k)).hasSize(2);
        assertThat(hits(k)).allSatisfy(h -> {
            assertThat(h.contentType()).isEqualTo("application/json");
            assertThat(h.body()).isEqualTo("{\"k\": \"" + k + "\"}");
            assertThat(h.path()).isEqualTo("/webhooks/alerts");
        });
        assertThat(jdbc.queryForObject("SELECT delivered_at IS NOT NULL AND last_error IS NULL FROM outbox WHERE idempotency_key=?", Boolean.class, k)).isTrue();
    }

    @Test
    void clientErrorsAndRedirectsAreDeadAtOnce() {
        String a = row(alert("dev-0402"), 1, "opened", 400);
        String b = row(alert("dev-0403"), 1, "opened", 404);
        String c = row(alert("dev-0404"), 1, "opened", 302);
        for (String k : List.of(a, b, c)) awaitNot(k, "PENDING");
        assertThat(state(a)).isEqualTo("DEAD,1,400");
        assertThat(state(b)).isEqualTo("DEAD,1,404");
        assertThat(state(c)).isEqualTo("DEAD,1,302");
        assertThat(HITS.stream().filter(h -> h.path().equals("/redirected"))).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE dead_at IS NOT NULL AND idempotency_key IN (?,?,?)", Long.class, a, b, c)).isEqualTo(3);
    }

    @Test
    void requestTimeoutAndTooManyRequestsAreRetried() {
        String k = row(alert("dev-0405"), 1, "opened", 408, 429);
        awaitNot(k, "PENDING");
        assertThat(state(k)).isEqualTo("DELIVERED,3,200");
    }

    @Test
    void attemptsRunOutToDeadAndBigBodiesAreCapped() {
        String k = row(alert("dev-0406"), 1, "opened", 599, 599, 599, 599);
        awaitNot(k, "PENDING");
        assertThat(state(k)).isEqualTo("DEAD,4,500");
        assertThat(hits(k)).hasSize(4);
        assertThat(jdbc.queryForObject("SELECT length(last_error) FROM outbox WHERE idempotency_key=?", Integer.class, k)).isBetween(10, 512);
    }

    @Test
    void transitionsOfOneAlertGoOutInOrderAndDeadRowsDoNotBlock() {
        long a = alert("dev-0407");
        String a1 = row(a, 1, "opened", 500, 500);
        String a2 = row(a, 2, "acked");
        long b = alert("dev-0408");
        String b1 = row(b, 1, "opened", 400);
        String b2 = row(b, 2, "resolved");
        awaitNot(a2, "PENDING");
        awaitNot(b2, "PENDING");
        assertThat(state(a1)).startsWith("DELIVERED");
        assertThat(state(b1)).startsWith("DEAD");
        assertThat(state(b2)).startsWith("DELIVERED");
        List<String> order = HITS.stream().map(Hit::key).filter(k -> k.equals(a1) || k.equals(a2)).toList();
        assertThat(order).isEqualTo(List.of(a1, a1, a1, a2));
    }

    @Test
    void twoWorkersNeverDeliverTheSameRowTwice() throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 40; i++) keys.add(row(alert("dev-05%02d".formatted(i)), 1, "opened"));
        OutboxWorker other = new OutboxWorker(jdbc, tx, meters, "http://127.0.0.1:" + SINK.getAddress().getPort() + "/webhooks/alerts",
                Duration.ofSeconds(2), 4, Duration.ofMillis(50), Duration.ofMillis(200), 0.2, 5);
        Thread t = new Thread(() -> {
            for (int i = 0; i < 40; i++) {
                other.poll();
                try {
                    Thread.sleep(25);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.start();
        for (String k : keys) awaitNot(k, "PENDING");
        t.join();
        for (String k : keys) assertThat(hits(k)).as(k).hasSize(1);
    }

    @Test
    void sinkUrlIsValidatedAtStartup() {
        assertThat(OutboxWorker.validate("http://sink:8080/webhooks/alerts").getHost()).isEqualTo("sink");
        for (String bad : List.of("ftp://sink/x", "not a url", "/relative", "http:///nohost", "")) {
            assertThatThrownBy(() -> OutboxWorker.validate(bad)).as(bad).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void backoffDoublesToTheCapWithJitter() {
        OutboxWorker w = new OutboxWorker(jdbc, tx, meters, "http://x:1/y", Duration.ofSeconds(5), 20, Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2, 1);
        for (int i = 0; i < 50; i++) {
            assertThat(w.backoff(1)).isBetween(0.8, 1.2);
            assertThat(w.backoff(3)).isBetween(3.2, 4.8);
            assertThat(w.backoff(19)).isBetween(24.0, 36.0);
        }
    }
}
