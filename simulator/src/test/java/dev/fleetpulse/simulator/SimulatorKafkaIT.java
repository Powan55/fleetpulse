package dev.fleetpulse.simulator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "sim.device-count=10",
            "sim.seed=42",
            "sim.epoch=1792130400000",
            "sim.heartbeat-interval=200ms",
            "sim.telemetry-interval=200ms",
            "sim.topic=sim-it"
        })
class SimulatorKafkaIT {

    private static final int DEVICES = 10;
    private static final int MIN_PER_DEVICE = 8;

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry r) throws Exception {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("sim-it", 6, (short) 1))).all().get();
        }
    }

    @Value("${local.server.port}")
    int port;

    @Test
    void recordsMatchContract() throws Exception {
        Schema schema = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12)
                .getSchema(Files.readString(Path.of("../contracts/envelope.schema.json")), InputFormat.JSON);

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Map<String, Long> lastSeq = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();
        Pattern seqRe = Pattern.compile("\"seq\":(\\d+)");
        Pattern idRe = Pattern.compile("\"event_id\":\"([^\"]+)\"");
        Pattern epochRe = Pattern.compile("\"epoch\":(\\d+)");
        Instant deadline = Instant.now().plusSeconds(60);

        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cfg)) {
            c.subscribe(List.of("sim-it"));
            while (Instant.now().isBefore(deadline)
                    && (counts.size() < DEVICES || counts.values().stream().anyMatch(n -> n < MIN_PER_DEVICE))) {
                for (ConsumerRecord<String, String> rec : c.poll(Duration.ofMillis(500))) {
                    String json = rec.value();
                    List<?> errors = schema.validate(json, InputFormat.JSON);
                    assertTrue(errors.isEmpty(), () -> errors + " in " + json);

                    assertTrue(json.getBytes(StandardCharsets.UTF_8).length <= 1024);
                    assertTrue(json.contains("\"device_id\":\"" + rec.key() + "\""), json);
                    assertTrue(json.contains("\"replay\":false"), json);

                    Matcher m = epochRe.matcher(json);
                    assertTrue(m.find());
                    assertEquals(1792130400000L, Long.parseLong(m.group(1)));

                    m = idRe.matcher(json);
                    assertTrue(m.find());
                    UUID.fromString(m.group(1));
                    m = seqRe.matcher(json);
                    assertTrue(m.find());
                    long seq = Long.parseLong(m.group(1));
                    String expectedId = UUID.nameUUIDFromBytes(
                            ("42:" + rec.key() + ":1792130400000:" + seq).getBytes(StandardCharsets.UTF_8))
                            .toString();
                    assertTrue(json.contains("\"event_id\":\"" + expectedId + "\""), json);

                    Long prev = lastSeq.put(rec.key(), seq);
                    assertEquals(prev == null ? 1L : prev + 1, seq, "seq gap for " + rec.key());
                    counts.merge(rec.key(), 1, Integer::sum);
                }
            }
        }

        assertEquals(DEVICES, counts.size(), counts.toString());
        counts.forEach((k, n) -> assertTrue(n >= MIN_PER_DEVICE, k + " only " + n));
    }

    @Test
    void healthProbesAreUp() throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        for (String p : List.of("liveness", "readiness")) {
            HttpResponse<String> res = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health/" + p)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, res.statusCode(), p);
            assertTrue(res.body().contains("UP"), res.body());
        }
        HttpResponse<String> info = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/env")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(404, info.statusCode());
    }
}
