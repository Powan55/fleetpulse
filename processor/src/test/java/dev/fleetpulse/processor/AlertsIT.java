package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = "fp.sweep.grace=0s")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class AlertsIT {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));
    static final PostgreSQLContainer PG = new PostgreSQLContainer(DockerImageName.parse("postgres:18.6"));
    static final int PORT = freePort();
    static final long T0 = System.currentTimeMillis() * 1000;
    static final JsonMapper JSON = JsonMapper.builder().build();

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
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", () -> "fleetpulse_app");
        r.add("spring.datasource.password", () -> "app-pw");
        r.add("server.port", () -> PORT);
    }

    static class FakeProbe implements Sweeper.LagProbe {
        volatile long lag;

        @Override
        public long maxLag() { return lag; }
    }

    @TestConfiguration
    static class Cfg {
        @Bean @Primary FakeProbe fakeProbe() { return new FakeProbe(); }
    }

    @Autowired DeviceStore store;
    @Autowired AlertStore alerts;
    @Autowired Sweeper sweeper;
    @Autowired Retention retention;
    @Autowired FakeProbe probe;
    @Autowired JdbcTemplate jdbc;
    @Autowired TransactionTemplate tx;
    @Autowired Rules rules;
    @Autowired MeterRegistry meters;

    final Map<String, Long> seqs = new HashMap<>();

    DeviceStore.Applied ev(String dev, double temp, double batt, int err, boolean replay, boolean telemetry) {
        long seq = seqs.merge(dev, 1L, Long::sum);
        var e = new Validator.Event(UUID.randomUUID().toString(), dev, 1, seq, T0 + seq * 2_000_000L, telemetry, replay, temp, batt, err);
        var rec = new ConsumerRecord<byte[], byte[]>("t", 0, seq, System.currentTimeMillis(), TimestampType.LOG_APPEND_TIME, 0, 0,
                null, null, new RecordHeaders(), Optional.empty());
        return new DeviceStore.Applied(e, rec);
    }

    void send(String dev, int n, double temp) {
        for (int i = 0; i < n; i++) store.apply(List.of(ev(dev, temp, 80, 0, false, true)));
    }

    static Connection su() throws SQLException {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    static String str(String sql) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement(); var rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static long num(String sql) throws Exception {
        return Long.parseLong(str(sql));
    }

    static void exec(String sql) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static final Schema WEBHOOK = load();

    static Schema load() {
        try {
            return SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                    .getSchema(Files.readString(Path.of("../contracts/webhook.schema.json")), InputFormat.JSON);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void assertPayloadsValid(String dev) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement(); var rs = s.executeQuery(
                "SELECT o.payload::text FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='%s'".formatted(dev))) {
            int n = 0;
            while (rs.next()) {
                assertThat(WEBHOOK.validate(rs.getString(1), InputFormat.JSON)).as(rs.getString(1)).isEmpty();
                n++;
            }
            assertThat(n).isPositive();
        }
    }

    @Test
    void newDeviceStartsHealthyWithNoTransition() throws Exception {
        send("dev-0201", 1, 41);
        assertThat(str("SELECT state||','||state_version FROM device WHERE device_id='dev-0201'")).isEqualTo("HEALTHY,1");
        assertThat(num("SELECT count(*) FROM state_transition WHERE device_id='dev-0201'")).isZero();
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='dev-0201'")).isZero();
    }

    @Test
    void failureOpensAlertsWithOutboxAndAutoResolves() throws Exception {
        String d = "dev-0202";
        send(d, 3, 90);
        assertThat(str("SELECT state||','||state_version||','||ok_n FROM device WHERE device_id='" + d + "'")).isEqualTo("FAILED,2,0");
        assertThat(str("SELECT from_state||'>'||to_state||','||cause||','||(event_id IS NOT NULL) FROM state_transition WHERE device_id='" + d + "'"))
                .isEqualTo("HEALTHY>FAILED,telemetry,true");
        assertThat(str("SELECT string_agg(rule_id||':'||status||':'||backfilled||':'||version, ',' ORDER BY rule_id) FROM alert WHERE device_id='" + d + "'"))
                .isEqualTo("device_failed:OPEN:false:1,temperature_high:OPEN:false:1");
        assertThat(str("SELECT (trigger_event_id::text = (SELECT event_id::text FROM state_transition WHERE device_id='" + d
                + "'))::text FROM alert WHERE device_id='" + d + "' AND rule_id='temperature_high'")).isEqualTo("true");
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='" + d + "' AND condition_started_event_time = timestamptz 'epoch' + "
                + (T0 + 2_000_000L) + " * interval '1 microsecond' AND rule_id='temperature_high'")).isEqualTo(1);
        assertThat(num("SELECT count(*) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='" + d
                + "' AND o.transition='opened' AND o.status='PENDING' AND o.idempotency_key = 'alert-'||a.alert_id||'-opened'")).isEqualTo(2);

        send(d, 5, 40);
        assertThat(str("SELECT state||','||state_version FROM device WHERE device_id='" + d + "'")).isEqualTo("RECOVERING,3");
        assertThat(str("SELECT string_agg(rule_id||':'||status||':'||resolved_by||':'||version, ',' ORDER BY rule_id) FROM alert WHERE device_id='" + d + "'"))
                .isEqualTo("device_failed:RESOLVED:auto:2,temperature_high:RESOLVED:auto:2");
        assertThat(num("SELECT count(*) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='" + d + "' AND o.transition='resolved'")).isEqualTo(2);
        assertPayloadsValid(d);
        assertThat(rules.rules()).hasSize(5);
    }

    @Test
    void replayedEndedFaultGivesOneBackfilledAlert() throws Exception {
        String d = "dev-0203";
        List<DeviceStore.Applied> batch = new ArrayList<>();
        for (int i = 0; i < 3; i++) batch.add(ev(d, 75, 80, 0, true, true));
        for (int i = 0; i < 5; i++) batch.add(ev(d, 40, 80, 0, true, true));
        store.apply(batch);
        assertThat(str("SELECT count(*)||','||min(status)||','||bool_and(backfilled) FROM alert WHERE device_id='" + d + "'")).isEqualTo("1,RESOLVED,true");
        assertThat(str("SELECT string_agg(o.transition||o.alert_version||(o.payload->>'backfilled'), ',' ORDER BY o.outbox_id) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='" + d + "'"))
                .isEqualTo("opened1true,resolved2true");
        assertThat(str("SELECT string_agg(from_state||'>'||to_state, ',' ORDER BY id) FROM state_transition WHERE device_id='" + d + "'"))
                .isEqualTo("HEALTHY>DEGRADED,DEGRADED>HEALTHY");
        assertPayloadsValid(d);
    }

    @Test
    void manualResolveRestartsTheBreachStreak() throws Exception {
        String d = "dev-0204";
        send(d, 3, 75);
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='" + d + "'")).isEqualTo(1);
        exec("UPDATE alert SET status='RESOLVED', resolved_at=now(), resolved_by='operator', version=version+1, updated_at=now() WHERE device_id='" + d + "'");
        send(d, 2, 75);
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='" + d + "'")).isEqualTo(1);
        send(d, 1, 75);
        assertThat(str("SELECT count(*)||','||count(*) FILTER (WHERE status='OPEN') FROM alert WHERE device_id='" + d + "'")).isEqualTo("2,1");
    }

    @Test
    void stateAlertReopensSixSecondsAfterManualResolve() throws Exception {
        String d = "dev-0205";
        send(d, 3, 90);
        exec("UPDATE alert SET status='RESOLVED', resolved_at=now(), resolved_by='operator', version=version+1, updated_at=now() WHERE device_id='" + d + "' AND rule_id='device_failed'");
        sweeper.reopen();
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='" + d + "' AND rule_id='device_failed' AND status='OPEN'")).isZero();
        Thread.sleep(6500);
        sweeper.reopen();
        sweeper.reopen();
        assertThat(str("SELECT count(*)||','||min(coalesce(trigger_event_id::text,'null'))||','||min(backfilled::text) FROM alert WHERE device_id='" + d
                + "' AND rule_id='device_failed' AND status='OPEN'")).isEqualTo("1,null,false");
        assertThat(num("SELECT count(*) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='" + d + "' AND a.rule_id='device_failed' AND o.transition='opened'")).isEqualTo(2);
    }

    @Test
    void concurrentSweepsMarkOfflineOnceAndAnyEventWakesTheDevice() throws Exception {
        String d = "dev-0206";
        send(d, 1, 40);
        exec("UPDATE device SET last_seen_at = now() - interval '30 seconds' WHERE device_id='" + d + "'");
        probe.lag = 0;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<String>> fs = new ArrayList<>();
        for (int i = 0; i < 4; i++) fs.add(pool.submit((Callable<String>) sweeper::sweep));
        for (Future<String> f : fs) assertThat(f.get()).isEqualTo("ran");
        pool.shutdown();
        assertThat(str("SELECT state||','||state_version FROM device WHERE device_id='" + d + "'")).isEqualTo("OFFLINE,2");
        assertThat(str("SELECT count(*)||','||min(cause)||','||min(from_state) FROM state_transition WHERE device_id='" + d + "' AND to_state='OFFLINE'"))
                .isEqualTo("1,offline_sweep,HEALTHY");
        assertThat(str("SELECT status||','||coalesce(trigger_event_id::text,'null')||','||backfilled FROM alert WHERE device_id='" + d + "' AND rule_id='device_offline'"))
                .isEqualTo("OPEN,null,false");
        assertThat(num("SELECT count(*) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='" + d + "'")).isEqualTo(1);

        store.apply(List.of(ev(d, 0, 0, 0, false, false)));
        assertThat(str("SELECT state||','||state_version FROM device WHERE device_id='" + d + "'")).isEqualTo("RECOVERING,3");
        assertThat(str("SELECT cause FROM state_transition WHERE device_id='" + d + "' AND from_state='OFFLINE'")).isEqualTo("event_after_offline");
        assertThat(str("SELECT status||','||resolved_by FROM alert WHERE device_id='" + d + "' AND rule_id='device_offline'")).isEqualTo("RESOLVED,auto");
        send(d, 4, 40);
        assertThat(str("SELECT state FROM device WHERE device_id='" + d + "'")).isEqualTo("RECOVERING");
        send(d, 1, 40);
        assertThat(str("SELECT state FROM device WHERE device_id='" + d + "'")).isEqualTo("HEALTHY");
    }

    @Test
    void sweepIsSkippedWhileAnyPartitionIsBehindAndForTheGraceAfter() throws Exception {
        String d = "dev-0207";
        send(d, 1, 40);
        exec("UPDATE device SET last_seen_at = now() - interval '30 seconds' WHERE device_id='" + d + "'");
        FakeProbe p = new FakeProbe();
        Sweeper s = new Sweeper(jdbc, tx, p, alerts, rules, meters, Duration.ofSeconds(10), Duration.ofSeconds(2), 1000);
        p.lag = 1001;
        assertThat(s.sweep()).isEqualTo("skipped_lag");
        p.lag = 0;
        assertThat(s.sweep()).isEqualTo("skipped_grace");
        assertThat(str("SELECT state FROM device WHERE device_id='" + d + "'")).isEqualTo("HEALTHY");
        Thread.sleep(2200);
        assertThat(s.sweep()).isEqualTo("ran");
        assertThat(str("SELECT state FROM device WHERE device_id='" + d + "'")).isEqualTo("OFFLINE");
        Sweeper broken = new Sweeper(jdbc, tx, () -> { throw new IllegalStateException("down"); }, alerts, rules, meters,
                Duration.ofSeconds(10), Duration.ZERO, 1000);
        assertThat(broken.sweep()).isEqualTo("skipped_error");
    }

    @Test
    void notifiesOnlyOnStateAndAlertChanges() throws Exception {
        try (Connection c = su(); Statement s = c.createStatement()) {
            s.execute("LISTEN fleetpulse_changes");
            PGConnection pg = c.unwrap(PGConnection.class);
            pg.getNotifications(1);
            send("dev-0208", 1, 40);
            var first = pg.getNotifications(1500);
            assertThat(first).hasSize(1);
            assertThat(first[0].getParameter()).isEqualTo("{\"t\":\"d\",\"ids\":[\"dev-0208\"]}");
            send("dev-0208", 1, 40);
            assertThat(pg.getNotifications(500)).isNullOrEmpty();
            send("dev-0208", 3, 90);
            List<String> got = new ArrayList<>();
            for (int i = 0; i < 10 && got.size() < 2; i++) {
                var n = pg.getNotifications(500);
                if (n != null) for (var x : n) got.add(x.getParameter());
            }
            assertThat(got).hasSize(2);
            assertThat(got.stream().filter(x -> x.startsWith("{\"t\":\"d\"")).count()).isEqualTo(1);
            JsonNode a = JSON.readTree(got.stream().filter(x -> x.startsWith("{\"t\":\"a\"")).findFirst().orElseThrow());
            assertThat(a.get("ids").size()).isEqualTo(2);
            assertThat(a.get("ids").get(0).isIntegralNumber()).isTrue();

            Set<String> many = new TreeSet<>();
            for (int i = 0; i < 450; i++) many.add("dev-%04d".formatted(i));
            alerts.notifyDevices(many);
            int total = 0, calls = 0;
            for (int i = 0; i < 10 && total < 450; i++) {
                var n = pg.getNotifications(500);
                if (n == null) continue;
                for (var x : n) {
                    int k = JSON.readTree(x.getParameter()).get("ids").size();
                    assertThat(k).isLessThanOrEqualTo(200);
                    total += k;
                    calls++;
                }
            }
            assertThat(total).isEqualTo(450);
            assertThat(calls).isEqualTo(3);
        }
    }

    @Test
    void apiRoleCanOnlyTouchStatusColumnsAndInsertOutbox() throws Exception {
        Long id = alerts.open("dev-0209", "battery_low", false, null, null, new java.util.HashSet<>());
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "fleetpulse_api", "api-pw"); Statement s = c.createStatement()) {
            assertThat(s.executeUpdate("UPDATE alert SET status='ACKED', acked_at=now(), version=version+1, updated_at=now() WHERE alert_id=" + id)).isEqualTo(1);
            s.execute("INSERT INTO outbox (alert_id, alert_version, transition, idempotency_key, payload) VALUES (%d, 2, 'acked', 'alert-%d-acked', '{}')".formatted(id, id));
            for (String bad : List.of("UPDATE alert SET device_id='dev-9999' WHERE alert_id=" + id,
                    "UPDATE alert SET backfilled=true WHERE alert_id=" + id,
                    "DELETE FROM alert WHERE alert_id=" + id,
                    "UPDATE outbox SET status='DEAD'",
                    "UPDATE device SET state='FAILED'",
                    "INSERT INTO state_transition (device_id, from_state, to_state, cause) VALUES ('dev-0001','HEALTHY','FAILED','telemetry')")) {
                assertThatThrownBy(() -> s.execute(bad)).as(bad).isInstanceOf(SQLException.class).hasMessageContaining("permission denied");
            }
        }
    }

    @Test
    void concurrentOpenOfTheSameRuleCreatesOneAlert() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<Long>> fs = new ArrayList<>();
        for (int i = 0; i < 8; i++) fs.add(pool.submit(() -> alerts.open("dev-0210", "error_burst", false, null, null, new java.util.HashSet<>())));
        int created = 0;
        for (Future<Long> f : fs) if (f.get() != null) created++;
        pool.shutdown();
        assertThat(created).isEqualTo(1);
        assertThat(num("SELECT count(*) FROM alert WHERE device_id='dev-0210'")).isEqualTo(1);
        assertThat(num("SELECT count(*) FROM outbox o JOIN alert a USING (alert_id) WHERE a.device_id='dev-0210'")).isEqualTo(1);
        assertThat(num("SELECT count(*) FROM pg_indexes WHERE indexname='alert_one_open'")).isEqualTo(1);
    }

    @Test
    void retentionKeepsPendingAndDeadOutboxAndTheirAlerts() throws Exception {
        exec("""
                INSERT INTO state_transition (device_id, from_state, to_state, cause, occurred_at) VALUES
                  ('dev-0301','HEALTHY','FAILED','telemetry', now() - interval '8 days'),
                  ('dev-0301','FAILED','RECOVERING','telemetry', now() - interval '1 day');
                INSERT INTO alert (device_id, rule_id, status, resolved_at, resolved_by, opened_at) VALUES
                  ('dev-0302','battery_low','RESOLVED', now() - interval '8 days', 'auto', now() - interval '9 days'),
                  ('dev-0303','battery_low','RESOLVED', now() - interval '8 days', 'auto', now() - interval '9 days'),
                  ('dev-0304','battery_low','RESOLVED', now() - interval '8 days', 'auto', now() - interval '9 days'),
                  ('dev-0305','battery_low','RESOLVED', now() - interval '8 days', 'auto', now() - interval '9 days'),
                  ('dev-0306','battery_low','RESOLVED', now() - interval '1 day', 'auto', now() - interval '2 days');
                INSERT INTO outbox (alert_id, alert_version, transition, idempotency_key, payload, status, delivered_at, dead_at, created_at)
                  SELECT alert_id, 1, 'opened', 'ret-' || device_id, '{}',
                         CASE device_id WHEN 'dev-0303' THEN 'DELIVERED' WHEN 'dev-0304' THEN 'PENDING' ELSE 'DEAD' END,
                         CASE device_id WHEN 'dev-0303' THEN now() - interval '25 hours' END,
                         CASE WHEN device_id = 'dev-0305' THEN now() - interval '9 days' END, now() - interval '9 days'
                  FROM alert WHERE device_id IN ('dev-0303','dev-0304','dev-0305');
                """);
        retention.prune();
        assertThat(num("SELECT count(*) FROM state_transition WHERE device_id='dev-0301'")).isEqualTo(1);
        assertThat(str("SELECT string_agg(device_id, ',' ORDER BY device_id) FROM alert WHERE device_id BETWEEN 'dev-0302' AND 'dev-0306'"))
                .isEqualTo("dev-0304,dev-0305,dev-0306");
        assertThat(str("SELECT string_agg(idempotency_key||':'||status, ',' ORDER BY idempotency_key) FROM outbox WHERE idempotency_key LIKE 'ret-%'"))
                .isEqualTo("ret-dev-0304:PENDING,ret-dev-0305:DEAD");
    }

    @Test
    void testHooksDoNotExistByDefault() throws Exception {
        HttpClient c = HttpClient.newHttpClient();
        assertThat(c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + "/test/v1/partitions")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
        assertThat(c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + "/test/v1/partitions/1/pause"))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(404);
        assertThat(c.send(HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + "/actuator/health/liveness")).build(),
                HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
    }
}
