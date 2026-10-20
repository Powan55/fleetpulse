package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class InboxIT {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));
    static final PostgreSQLContainer PG = new PostgreSQLContainer(DockerImageName.parse("postgres:18.6"));
    static final String TOPIC = "fleetpulse.telemetry.v1";
    static final String DLQ = "fleetpulse.telemetry.v1.dlq";
    static final long EPOCH = System.currentTimeMillis();

    static {
        KAFKA.start();
        PG.start();
        Flyway.configure()
                .dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("filesystem:../db/migrations")
                .placeholders(Map.of("app_password", "app-pw", "api_password", "api-pw", "ro_password", "ro-pw"))
                .load().migrate();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 6, (short) 1), new NewTopic(DLQ, 1, (short) 1))).all().get();
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
    }

    @Autowired DeviceStore store;
    @Autowired Retention retention;

    static String event(String device, long epoch, long seq, int errors) {
        String id = UUID.nameUUIDFromBytes(("42:" + device + ":" + epoch + ":" + seq).getBytes(StandardCharsets.UTF_8)).toString();
        return """
                {"schema_version":1,"event_id":"%s","device_id":"%s","epoch":%d,"seq":%d,"event_time":"2026-10-16T12:34:56.789Z","type":"telemetry","replay":false,"payload":{"temperature_c":41.5,"battery_pct":87.0,"error_count":%d}}"""
                .formatted(id, device, epoch, seq, errors);
    }

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static void send(String key, String... values) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
            for (String v : values) producer.send(new ProducerRecord<>(TOPIC, b(key), b(v)));
            producer.flush();
        }
    }

    static Connection su() throws Exception {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    static long count(String sql) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement(); var rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    static void exec(String sql) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static void waitLastSeq(String dev, long seq) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM device WHERE device_id='%s' AND last_seq=%d".formatted(dev, seq))).isEqualTo(1));
    }

    static String stats(String dev, long epoch) throws Exception {
        try (Connection c = su(); Statement s = c.createStatement(); var rs = s.executeQuery(
                "SELECT applied_count||','||max_seq||','||late_count||','||gap_count FROM device_epoch_stats WHERE device_id='%s' AND epoch=%d".formatted(dev, epoch))) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    @Test
    void duplicateIsOneInboxRowAndOneApply() throws Exception {
        String dev = "dev-0101";
        send(dev, event(dev, EPOCH, 1, 0), event(dev, EPOCH, 1, 0), event(dev, EPOCH, 2, 0));
        waitLastSeq(dev, 2);
        assertThat(count("SELECT count(*) FROM inbox WHERE device_id='%s'".formatted(dev))).isEqualTo(2);
        assertThat(stats(dev, EPOCH)).isEqualTo("2,2,0,0");
        assertThat(count("SELECT count(*) FROM telemetry WHERE device_id='%s'".formatted(dev))).isEqualTo(2);
    }

    @Test
    void gapIsCountedWithoutWaiting() throws Exception {
        String dev = "dev-0102";
        send(dev, event(dev, EPOCH, 1, 0), event(dev, EPOCH, 2, 0), event(dev, EPOCH, 5, 0));
        waitLastSeq(dev, 5);
        assertThat(stats(dev, EPOCH)).isEqualTo("3,5,0,2");
    }

    @Test
    void lateEventLeavesDeviceAtNewest() throws Exception {
        String dev = "dev-0103";
        send(dev, event(dev, EPOCH, 10, 0));
        waitLastSeq(dev, 10);
        send(dev, event(dev, EPOCH, 5, 0));
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(count("SELECT count(*) FROM late_event WHERE device_id='%s' AND seq=5 AND device_last_seq=10".formatted(dev))).isEqualTo(1));
        assertThat(count("SELECT count(*) FROM device WHERE device_id='%s' AND last_seq=10".formatted(dev))).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM inbox WHERE device_id='%s' AND seq=5 AND late".formatted(dev))).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM telemetry WHERE device_id='%s' AND seq=5".formatted(dev))).isZero();
        assertThat(stats(dev, EPOCH)).isEqualTo("2,10,1,9");
    }

    @Test
    void redeliveryAfterCommitIsDeduped() throws Exception {
        String dev = "dev-0104";
        var rec = new ConsumerRecord<>(TOPIC, 0, 0L, b(dev), b(event(dev, EPOCH, 1, 0)));
        var a = new DeviceStore.Applied(Validator.validate(rec.key(), rec.value(), System.currentTimeMillis()).event(), rec);
        assertThat(store.apply(List.of(a)).applied()).isEqualTo(1);
        var again = store.apply(List.of(a));
        assertThat(again.applied()).isZero();
        assertThat(again.duplicate()).isEqualTo(1);
        assertThat(stats(dev, EPOCH)).isEqualTo("1,1,0,0");
    }

    @Test
    void invalidRecordsGoToDlqWithSameBytes() throws Exception {
        String good = event("dev-0105", EPOCH, 1, 0);
        Map<String, String> bad = new HashMap<>();
        bad.put("invalid_json", "{broken");
        bad.put("schema", good.replace("\"replay\":false,", ""));
        bad.put("too_large", good.replace("41.5", "41.5" + " ".repeat(1100)));
        bad.put("epoch_future", event("dev-0105", System.currentTimeMillis() + 3 * 24 * 3600_000L, 1, 0));
        for (String v : bad.values()) send("dev-0105", v);
        send("dev-0999", good);
        send("dev-0105", good);

        Map<String, ConsumerRecord<byte[], byte[]>> got = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> c = dlqConsumer()) {
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                c.poll(Duration.ofMillis(500)).forEach(r -> got.put(hdr(r, "fp-dlq-reason"), r));
                return got.keySet().containsAll(List.of("invalid_json", "schema", "too_large", "epoch_future", "key_mismatch"));
            });
        }
        bad.forEach((reason, v) -> assertThat(got.get(reason).value()).isEqualTo(b(v)));
        assertThat(got.get("key_mismatch").key()).isEqualTo(b("dev-0999"));
        waitLastSeq("dev-0105", 1);
    }

    @Test
    void constraintViolationGoesToDlqAndRestOfBatchApplies() throws Exception {
        exec("ALTER TABLE telemetry ADD CONSTRAINT no_777 CHECK (error_count <> 777)");
        try {
            String poison = event("dev-0106", EPOCH, 2, 777);
            send("dev-0106", event("dev-0106", EPOCH, 1, 0), poison, event("dev-0106", EPOCH, 3, 0));
            send("dev-0107", event("dev-0107", EPOCH, 1, 0));
            ConsumerRecord<byte[], byte[]> dead = null;
            try (KafkaConsumer<byte[], byte[]> c = dlqConsumer()) {
                var recs = new ArrayList<ConsumerRecord<byte[], byte[]>>();
                await().atMost(Duration.ofSeconds(60)).until(() -> {
                    c.poll(Duration.ofMillis(500)).forEach(r -> {
                        if ("db_constraint".equals(hdr(r, "fp-dlq-reason"))) recs.add(r);
                    });
                    return !recs.isEmpty();
                });
                dead = recs.get(0);
            }
            assertThat(dead.value()).isEqualTo(b(poison));
            assertThat(dead.key()).isEqualTo(b("dev-0106"));
            assertThat(hdr(dead, "fp-dlq-sqlstate")).isEqualTo("23514");
            waitLastSeq("dev-0106", 3);
            waitLastSeq("dev-0107", 1);
            assertThat(count("SELECT count(*) FROM inbox WHERE device_id='dev-0106'")).isEqualTo(2);
        } finally {
            exec("ALTER TABLE telemetry DROP CONSTRAINT no_777");
        }
    }

    @Test
    void largeSeqJumpWarns(CapturedOutput out) throws Exception {
        String dev = "dev-0108";
        send(dev, event(dev, EPOCH, 1, 0));
        waitLastSeq(dev, 1);
        send(dev, event(dev, EPOCH, 200_001, 0));
        waitLastSeq(dev, 200_001);
        assertThat(out.getAll()).contains("seq jump device=dev-0108");
    }

    @Test
    void retentionDeletesOnlyOldRows() throws Exception {
        exec("""
                INSERT INTO inbox (event_id, device_id, epoch, seq, late, received_at) VALUES
                  ('00000000-0000-3000-8000-000000000001','dev-0200',1,1,false, now() - interval '2 hours'),
                  ('00000000-0000-3000-8000-000000000002','dev-0200',1,2,false, now() - interval '1 minute');
                INSERT INTO telemetry (device_id, epoch, seq, event_id, event_time, received_at, temperature_c, battery_pct, error_count, replay) VALUES
                  ('dev-0200',1,1,'00000000-0000-3000-8000-000000000001', now(), now() - interval '20 minutes', 1, 1, 0, false),
                  ('dev-0200',1,2,'00000000-0000-3000-8000-000000000002', now(), now() - interval '1 minute', 1, 1, 0, false);
                INSERT INTO late_event (event_id, device_id, epoch, seq, type, event_time, received_at, device_last_epoch, device_last_seq) VALUES
                  ('00000000-0000-3000-8000-000000000001','dev-0200',1,1,'telemetry', now(), now() - interval '2 hours', 1, 9),
                  ('00000000-0000-3000-8000-000000000002','dev-0200',1,2,'telemetry', now(), now() - interval '1 minute', 1, 9);
                """);
        retention.prune();
        for (String t : List.of("inbox", "telemetry", "late_event")) {
            assertThat(count("SELECT count(*) FROM %s WHERE device_id='dev-0200'".formatted(t))).as(t).isEqualTo(1);
            assertThat(count("SELECT count(*) FROM %s WHERE device_id='dev-0200' AND seq=2".formatted(t))).as(t).isEqualTo(1);
        }
    }

    @Test
    void readOnlyRoleSeesNewTablesAndView() throws Exception {
        try (Connection c = DriverManager.getConnection(PG.getJdbcUrl(), "fleetpulse_ro", "ro-pw"); Statement s = c.createStatement()) {
            for (String t : List.of("inbox", "device_epoch_stats", "telemetry", "late_event", "state_transition", "v_reconciliation")) {
                try (var rs = s.executeQuery("SELECT count(*) FROM " + t)) {
                    assertThat(rs.next()).as(t).isTrue();
                }
            }
        }
    }

    static String hdr(ConsumerRecord<byte[], byte[]> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    static KafkaConsumer<byte[], byte[]> dlqConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-reader-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
        c.subscribe(List.of(DLQ));
        return c;
    }
}
