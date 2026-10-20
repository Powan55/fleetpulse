package dev.fleetpulse.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
class ProcessorIT {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));
    static final PostgreSQLContainer PG = new PostgreSQLContainer(DockerImageName.parse("postgres:18.6"));
    static final String TOPIC = "fleetpulse.telemetry.v1";
    static final String DLQ = "fleetpulse.telemetry.v1.dlq";
    static final String GROUP = "fleetpulse-processor";

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

    static String event(String device, long epoch, long seq) {
        String id = UUID.nameUUIDFromBytes(("42:" + device + ":" + epoch + ":" + seq).getBytes(StandardCharsets.UTF_8)).toString();
        return """
                {"schema_version":1,"event_id":"%s","device_id":"%s","epoch":%d,"seq":%d,"event_time":"2026-10-16T12:34:56.789Z","type":"telemetry","replay":false,"payload":{"temperature_c":41.5,"battery_pct":87.0,"error_count":0}}"""
                .formatted(id, device, epoch, seq);
    }

    static Connection admin() throws Exception {
        return DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
    }

    static long committed() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            Set<TopicPartition> parts = new HashSet<>();
            for (int i = 0; i < 6; i++) parts.add(new TopicPartition(TOPIC, i));
            return c.committed(parts).values().stream().filter(o -> o != null).mapToLong(o -> o.offset()).sum();
        }
    }

    @Test
    void upsertsNotifiesAndCommitsOffsetsOnlyAfterDbCommit() throws Exception {
        try (Connection listen = admin(); Connection su = admin(); Statement s = su.createStatement()) {
            listen.createStatement().execute("LISTEN fleetpulse_changes");
            s.execute("REVOKE INSERT ON device FROM fleetpulse_app");

            Properties p = new Properties();
            p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            long epoch = System.currentTimeMillis();
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(p)) {
                for (int seq = 1; seq <= 5; seq++) {
                    for (int d = 1; d <= 10; d++) {
                        String dev = "dev-%04d".formatted(d);
                        producer.send(new ProducerRecord<>(TOPIC, dev.getBytes(StandardCharsets.UTF_8),
                                event(dev, epoch, seq).getBytes(StandardCharsets.UTF_8)));
                    }
                }
                producer.send(new ProducerRecord<>(TOPIC, "dev-0001".getBytes(StandardCharsets.UTF_8),
                        "{broken".getBytes(StandardCharsets.UTF_8)));
                producer.flush();
            }

            // DB write is failing: nothing stored and no offset may be committed.
            Thread.sleep(4000);
            assertThat(count(s, "SELECT count(*) FROM device")).isZero();
            assertThat(committed()).isZero();

            s.execute("GRANT INSERT ON device TO fleetpulse_app");
            await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(count(s, "SELECT count(*) FROM device WHERE last_seq = 5")).isEqualTo(10));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(committed()).isEqualTo(51));

            assertThat(count(s, "SELECT count(*) FROM device WHERE state = 'HEALTHY' AND state_version = 1")).isEqualTo(10);

            Set<String> ids = new HashSet<>();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var n = listen.unwrap(PGConnection.class).getNotifications(500);
                if (n != null) for (var x : n) ids.addAll(extractIds(x.getParameter()));
                assertThat(ids).hasSize(10);
            });

            try (KafkaConsumer<byte[], byte[]> c = dlqConsumer()) {
                var recs = new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>>();
                await().atMost(Duration.ofSeconds(15)).until(() -> {
                    c.poll(Duration.ofMillis(500)).forEach(recs::add);
                    return !recs.isEmpty();
                });
                assertThat(new String(recs.get(0).headers().lastHeader("fp-dlq-reason").value(), StandardCharsets.UTF_8))
                        .isEqualTo("invalid_json");
                assertThat(recs.get(0).value()).isEqualTo("{broken".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    static KafkaConsumer<byte[], byte[]> dlqConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-reader");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p);
        c.subscribe(List.of(DLQ));
        return c;
    }

    static List<String> extractIds(String payload) {
        var m = java.util.regex.Pattern.compile("dev-[0-9]{4}").matcher(payload);
        List<String> out = new ArrayList<>();
        while (m.find()) out.add(m.group());
        return out;
    }

    static long count(Statement s, String sql) throws Exception {
        try (var rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
