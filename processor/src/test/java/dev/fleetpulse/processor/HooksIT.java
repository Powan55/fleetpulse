package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {"fp.test-hooks.enabled=true",
        "fp.test-hooks.auto-resume=6s", "fp.sweep.interval=500ms", "fp.sweep.max-lag=20", "fp.sweep.grace=1s"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class HooksIT {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));
    static final PostgreSQLContainer PG = new PostgreSQLContainer(DockerImageName.parse("postgres:18.6"));
    static final String TOPIC = "fleetpulse.telemetry.v1";
    static final int PORT = port();
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final JsonMapper JSON = JsonMapper.builder().build();

    static {
        KAFKA.start();
        PG.start();
        Flyway.configure().dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("filesystem:../db/migrations")
                .placeholders(Map.of("app_password", "app-pw", "api_password", "api-pw", "ro_password", "ro-pw"))
                .load().migrate();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 6, (short) 1), new NewTopic(TOPIC + ".dlq", 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static int port() {
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

    @Autowired MeterRegistry meters;

    static HttpResponse<String> call(String method, String path, String contentType, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + path));
        if (contentType != null) b.header("Content-Type", contentType);
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    static JsonNode partition(int p) throws Exception {
        for (JsonNode n : JSON.readTree(call("GET", "/test/v1/partitions", null, null).body())) {
            if (n.get("partition").asInt() == p) return n;
        }
        return null;
    }

    static String sql(String q) throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword()); Statement s = c.createStatement()) {
            s.execute(q);
            var rs = s.getResultSet();
            return rs != null && rs.next() ? rs.getString(1) : null;
        }
    }

    double skipped() {
        var c = meters.find("fleetpulse.processor.sweep.runs").tag("result", "skipped_lag").counter();
        return c == null ? 0 : c.count();
    }

    @Test
    void pausedPartitionClosesTheSweepGateUntilResumed(CapturedOutput out) throws Exception {
        await().atMost(Duration.ofSeconds(60)).until(() -> JSON.readTree(call("GET", "/test/v1/partitions", null, null).body()).size() == 6);
        assertThat(call("POST", "/test/v1/partitions/2/pause", "application/json", "{}").statusCode()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(10)).until(() -> partition(2).get("paused").asBoolean());
        assertThat(partition(2).get("autoResumeAt").isNull()).isFalse();
        assertThat(meters.find("fleetpulse.processor.test_hooks.paused_partitions").gauge().value()).isEqualTo(1.0);
        assertThat(out.getAll()).contains("test hooks enabled");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < 100; i++) producer.send(new ProducerRecord<>(TOPIC, 2, null, "k".getBytes(), "garbage".getBytes()));
            producer.flush();
        }
        await().atMost(Duration.ofSeconds(15)).until(() -> skipped() > 0);
        sql("INSERT INTO device (device_id, state, last_epoch, last_seq, last_seen_at) VALUES ('dev-0601', 'HEALTHY', 1, 1, now() - interval '1 minute')");
        Thread.sleep(2500);
        assertThat(sql("SELECT state FROM device WHERE device_id='dev-0601'")).isEqualTo("HEALTHY");

        assertThat(call("POST", "/test/v1/partitions/2/resume", "application/json", "{}").statusCode()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(30)).until(() -> "OFFLINE".equals(sql("SELECT state FROM device WHERE device_id='dev-0601'")));
        assertThat(meters.find("fleetpulse.processor.test_hooks.paused_partitions").gauge().value()).isEqualTo(0.0);
    }

    @Test
    void badCallsAndAutoResume(CapturedOutput out) throws Exception {
        await().atMost(Duration.ofSeconds(60)).until(() -> JSON.readTree(call("GET", "/test/v1/partitions", null, null).body()).size() == 6);
        assertThat(call("POST", "/test/v1/partitions/1/pause", null, null).statusCode()).isEqualTo(415);
        assertThat(call("POST", "/test/v1/partitions/1/pause", "text/plain", "{}").statusCode()).isEqualTo(415);
        assertThat(call("POST", "/test/v1/partitions/1/pause", "application/json", "{\"a\":1}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/test/v1/partitions/9/pause", "application/json", "{}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/test/v1/partitions/x/pause", "application/json", "{}").statusCode()).isEqualTo(400);
        assertThat(call("POST", "/test/v1/partitions/4/pause", "application/json", "{}").statusCode()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(10)).until(() -> out.getAll().contains("test hook paused partition 4"));
        await().atMost(Duration.ofSeconds(20)).until(() -> out.getAll().contains("test hook auto-resumed partition 4"));
        await().atMost(Duration.ofSeconds(10)).until(() -> !partition(4).get("paused").asBoolean());
    }
}
